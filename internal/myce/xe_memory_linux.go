package myce

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

// struct drm_xe_device_query; DRM_IOCTL_XE_DEVICE_QUERY is DRM_IOWR(0x40, 40).
type xeDeviceQuery struct {
	Extensions uint64
	Query      uint32
	Size       uint32
	Data       uint64
	Reserved   [2]uint64
}

const (
	xeDeviceQueryIoctl    = 0xc0286440
	xeQueryMemRegions     = 1
	xeMemRegionsReplySize = 8 + xeMaxMemRegions*xeMemRegionSize
)

func xeQuery(fd int, q *xeDeviceQuery) error {
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), xeDeviceQueryIoctl, uintptr(unsafe.Pointer(q))); errno != 0 {
		return errno
	}
	return nil
}

// nativeXeFreeMemory reads one card's free VRAM through the broker's kernel
// interface: a read-only MEM_REGIONS query on the card's render node. It starts
// no vendor runtime and creates no GPU object or execution context, unlike
// xpu-smi (docs/host-probes.md).
func nativeXeFreeMemory(ctx context.Context, pci string) (float64, error) {
	probeguard.Check()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	pci = normalizePCIAddress(pci)
	if pci == "" || strings.ContainsAny(pci, "/\\") {
		return 0, errors.New("Xe PCI identity unavailable")
	}
	nodes, err := filepath.Glob(filepath.Join("/sys/bus/pci/devices", pci, "drm", "renderD*"))
	if err != nil || len(nodes) != 1 {
		return 0, errors.New("Xe render node unavailable")
	}
	fd, err := unix.Open(filepath.Join("/dev/dri", filepath.Base(nodes[0])), unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return 0, err
	}
	defer unix.Close(fd)
	q := xeDeviceQuery{Query: xeQueryMemRegions}
	if err := xeQuery(fd, &q); err != nil {
		return 0, err
	}
	if q.Size <= 8 || q.Size > xeMemRegionsReplySize {
		return 0, errors.New("invalid Xe memory query size")
	}
	reply := make([]byte, q.Size)
	size := q.Size
	q.Data = uint64(uintptr(unsafe.Pointer(&reply[0])))
	err = xeQuery(fd, &q)
	runtime.KeepAlive(reply)
	if err != nil {
		return 0, err
	}
	if q.Size != size {
		return 0, errors.New("Xe memory query changed size")
	}
	return xeFreeVRAMGB(reply)
}
