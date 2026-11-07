package myce

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

// Use the same read-only Xe PMU events exposed to perf stat. Sysman/xpu-smi
// utilization and i915's intel_gpu_top can be unavailable on Xe while these
// counters work. Enumerate the kernel's actual engines; class/instance zero
// alone is not a card-wide activity measurement.
func nativeXeActivity(ctx context.Context, pci string) (float64, error) {
	probeguard.Check()
	pci = normalizePCIAddress(pci)
	if pci == "" || strings.ContainsAny(pci, "/\\") {
		return 0, errors.New("Xe PCI identity unavailable")
	}
	pmu := filepath.Join("/sys/bus/event_source/devices", "xe_"+strings.ReplaceAll(pci, ":", "_"))
	b, err := os.ReadFile(filepath.Join(pmu, "type"))
	if err != nil {
		return 0, err
	}
	typ, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 32)
	if err != nil {
		return 0, err
	}
	engines, err := xeEngineInventory(pci)
	if err != nil {
		return 0, err
	}
	var groups []*os.File
	var members []int
	defer func() {
		for _, fd := range members {
			_ = unix.Close(fd)
		}
		for _, f := range groups {
			_ = f.Close()
		}
	}()
	for _, engine := range engines {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		config := uint64(engine.class)<<20 | uint64(engine.instance)<<12 | uint64(engine.gt)<<60
		attr := unix.PerfEventAttr{Type: uint32(typ), Size: unix.PERF_ATTR_SIZE_VER0,
			Config: config | 2, Bits: unix.PerfBitDisabled,
			Read_format: unix.PERF_FORMAT_GROUP | unix.PERF_FORMAT_TOTAL_TIME_ENABLED | unix.PERF_FORMAT_TOTAL_TIME_RUNNING}
		fd, err := unix.PerfEventOpen(&attr, -1, 0, -1, unix.PERF_FLAG_FD_CLOEXEC)
		if err != nil {
			return 0, err // Never elevate or change perf/driver settings.
		}
		groups = append(groups, os.NewFile(uintptr(fd), "xe-activity"))
		attr.Config, attr.Bits = config|3, 0
		member, err := unix.PerfEventOpen(&attr, -1, 0, fd, unix.PERF_FLAG_FD_CLOEXEC)
		if err != nil {
			return 0, err
		}
		members = append(members, member)
		if err := unix.IoctlSetInt(fd, unix.PERF_EVENT_IOC_ENABLE, unix.PERF_IOC_FLAG_GROUP); err != nil {
			return 0, err
		}
	}
	// Reuse the memory sensor's observation interval: no extra rate, process,
	// daemon or privilege is required. This runs in the asynchronous inventory
	// probe, outside the actor and queue lock.
	timer := time.NewTimer(attemptMemorySamplePeriod)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
	}
	busy := 0.0
	for _, f := range groups {
		if err := unix.IoctlSetInt(int(f.Fd()), unix.PERF_EVENT_IOC_DISABLE, unix.PERF_IOC_FLAG_GROUP); err != nil {
			return 0, err
		}
		var raw [40]byte // nr, enabled, running, active, total; grouped snapshot.
		if _, err := io.ReadFull(f, raw[:]); err != nil {
			return 0, err
		}
		pct, err := xeEngineActivity(raw[:])
		if err != nil {
			return 0, err
		}
		busy = max(busy, pct)
	}
	return busy, nil
}

type xeEngine struct{ class, instance, gt uint16 }

func xeEngineInventory(pci string) ([]xeEngine, error) {
	// Open only the render node linked to this physical PCI device. The query
	// ioctl allocates no GPU objects and submits no work.
	nodes, err := filepath.Glob(filepath.Join("/sys/bus/pci/devices", pci, "drm/renderD*"))
	if err != nil || len(nodes) != 1 {
		return nil, errors.New("Xe render identity unavailable")
	}
	f, err := os.Open(filepath.Join("/dev/dri", filepath.Base(nodes[0])))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var query struct {
		Extensions  uint64
		Query, Size uint32
		Data        uint64
		Reserved    [2]uint64
	}
	call := func() error {
		// DRM_IOCTL_XE_DEVICE_QUERY = DRM_IOWR(0x40, 40-byte query), query 0
		// is DRM_XE_DEVICE_QUERY_ENGINES (include/uapi/drm/xe_drm.h).
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), 0xc0286440, uintptr(unsafe.Pointer(&query)))
		if errno != 0 {
			return errno
		}
		return nil
	}
	if err := call(); err != nil {
		return nil, err
	}
	// PMU config has 4 GT bits, 8 instance bits, and five hardware classes.
	// Bound the kernel reply by that addressable inventory before allocating.
	if query.Size < 8 || query.Size > 8+32*16*256*5 {
		return nil, errors.New("Xe engine inventory size invalid")
	}
	b := make([]byte, query.Size)
	query.Data = uint64(uintptr(unsafe.Pointer(&b[0])))
	err = call()
	runtime.KeepAlive(b)
	if err != nil {
		return nil, err
	}
	return parseXeEngines(b)
}

func parseXeEngines(b []byte) ([]xeEngine, error) {
	if len(b) < 8 {
		return nil, errors.New("Xe engine inventory incomplete")
	}
	n := int(binary.NativeEndian.Uint32(b))
	if n == 0 || n != (len(b)-8)/32 || (len(b)-8)%32 != 0 {
		return nil, errors.New("Xe engine inventory incomplete")
	}
	engines, seen := []xeEngine{}, map[xeEngine]bool{}
	for i := range n {
		p := b[8+32*i:]
		e := xeEngine{binary.NativeEndian.Uint16(p), binary.NativeEndian.Uint16(p[2:]), binary.NativeEndian.Uint16(p[4:])}
		if e.class > 4 || e.instance > 255 || e.gt > 15 || seen[e] {
			return nil, errors.New("Xe engine inventory invalid")
		}
		seen[e] = true
		engines = append(engines, e)
	}
	return engines, nil
}

func xeEngineActivity(b []byte) (float64, error) {
	if len(b) != 40 {
		return 0, errors.New("Xe activity snapshot incomplete")
	}
	n, enabled, running := binary.NativeEndian.Uint64(b), binary.NativeEndian.Uint64(b[8:]), binary.NativeEndian.Uint64(b[16:])
	active, total := binary.NativeEndian.Uint64(b[24:]), binary.NativeEndian.Uint64(b[32:])
	if n != 2 || enabled == 0 || running != enabled || total == 0 || active > total {
		return 0, errors.New("Xe activity counter unavailable or multiplexed")
	}
	return 100 * float64(active) / float64(total), nil
}
