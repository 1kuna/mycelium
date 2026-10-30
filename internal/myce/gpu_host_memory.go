package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// GPUHostMemory is complete xe host evidence, separate from device VRAM.
// DriverGB counts per-client system/GTT objects plus the independently read
// xe GTT managers, even where they overlap each other or cgroup charges.
// HostDropGB is the whole-host MemAvailable drop since the empty cgroup was
// created; it supplies an independent floor for allocations absent from fdinfo.
// OutsideGB retains max(driver objects, host drop minus synchronized current
// components, zero) across samples. It is added to the component peak; never
// subtract independent lifetime peaks. These are observations, not instantaneous
// allocation bounds. Unrelated host activity can conservatively increase them.
type GPUHostMemory struct {
	DriverGB   *float64 `json:"driver_gb"`
	HostDropGB *float64 `json:"host_drop_gb"`
	OutsideGB  *float64 `json:"outside_gb"`
}

func (p *AttemptMemoryPeak) gpuHostPeakGB() (float64, bool) {
	if p == nil || p.NonreclaimableGB == nil || !validMemoryValue(*p.NonreclaimableGB) || p.GPUHost == nil || p.GPUHost.DriverGB == nil || p.GPUHost.HostDropGB == nil || p.GPUHost.OutsideGB == nil || !validMemoryValue(*p.GPUHost.DriverGB) || !validMemoryValue(*p.GPUHost.HostDropGB) || !validMemoryValue(*p.GPUHost.OutsideGB) || *p.GPUHost.OutsideGB < *p.GPUHost.DriverGB {
		return 0, false
	}
	return max(*p.NonreclaimableGB+*p.GPUHost.OutsideGB, *p.GPUHost.HostDropGB), true
}

type gpuHostState struct {
	available          float64
	swap, oom, swapOut uint64
}

func readGPUHostState(fs memoryFS) (*gpuHostState, error) {
	b, err := fs.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, err
	}
	available, err := parseMemAvailable(b)
	if err != nil {
		return nil, err
	}
	values := map[string]uint64{}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 3 && f[2] == "kB" {
			if n, e := strconv.ParseUint(f[1], 10, 64); e == nil {
				values[f[0]] = n
			}
		}
	}
	total, tok := values["SwapTotal:"]
	free, fok := values["SwapFree:"]
	if !tok || !fok || free > total {
		return nil, errors.New("host swap counters unavailable")
	}
	b, err = fs.ReadFile("/proc/vmstat")
	if err != nil {
		return nil, err
	}
	stats := memoryCounters(b)
	oom, ok := stats["oom_kill"]
	out, outOK := stats["pswpout"]
	if !ok || !outOK {
		return nil, errors.New("host OOM/swap-out counters unavailable")
	}
	return &gpuHostState{available: available, swap: total - free, oom: oom, swapOut: out}, nil
}

// Only xe discrete GPUs have been investigated. Missing fields, other drivers,
// CUDA and unified memory keep the total-cgroup estimator.
func drmHostMemory(b []byte) (string, float64, error) {
	fields := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, ":")
		if ok {
			fields[k] = strings.TrimSpace(v)
		}
	}
	if fields["drm-client-id"] == "" {
		// Ordinary descriptors have no DRM fields. A damaged DRM identity
		// must invalidate coverage even if another client is readable.
		for name := range fields {
			if strings.HasPrefix(name, "drm-") {
				return "", 0, errAttemptMemoryUnsupported
			}
		}
		return "", 0, errAttemptMemoryPending
	}
	if fields["drm-driver"] != "xe" || fields["drm-pdev"] == "" {
		return "", 0, errAttemptMemoryUnsupported
	}
	total := 0.0
	for _, region := range []string{"system", "gtt"} {
		var allocated, resident float64
		for i, counter := range []string{"total", "resident"} {
			f := strings.Fields(fields["drm-"+counter+"-"+region])
			if len(f) < 1 || len(f) > 2 {
				return "", 0, errAttemptMemoryUnsupported
			}
			n, err := strconv.ParseUint(f[0], 10, 64)
			if err != nil {
				return "", 0, errAttemptMemoryUnsupported
			}
			scale := 1.0
			if len(f) == 2 {
				switch f[1] {
				case "KiB":
					scale = 1 << 10
				case "MiB":
					scale = 1 << 20
				default:
					return "", 0, errAttemptMemoryUnsupported
				}
			}
			if i == 0 {
				allocated = float64(n) * scale / (1 << 30)
			} else {
				resident = float64(n) * scale / (1 << 30)
			}
		}
		total += max(allocated, resident)
	}
	return fields["drm-pdev"] + "/" + fields["drm-client-id"], total, nil
}

func validateGPUTelemetryHelper(path string) error {
	if path != "" && (!filepath.IsAbs(path) || strings.ContainsAny(path, "\x00\n\r")) {
		return errors.New("gpu-telemetry-helper must be an absolute, single-line executable path")
	}
	return nil
}

// The operator supplies an executable, not a command line. A local wrapper may
// exec a separately scoped sudo reader; neither job input nor worker credentials
// are sent to it. The timestamp must belong to this invocation, not a cache.
func readGPUTelemetryHelper(ctx context.Context, clock Clock, path string) (map[string]string, error) {
	if err := validateGPUTelemetryHelper(path); err != nil || path == "" {
		return nil, errAttemptMemoryUnsupported
	}
	started := clock.Now()
	ctx, cancel := context.WithTimeout(ctx, attemptMemorySamplePeriod)
	defer cancel()
	cmd := exec.CommandContext(ctx, path)
	cmd.Dir = "/"
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C"}
	cmd.WaitDelay = attemptMemorySamplePeriod
	const maxOutput = 64 << 10
	out := &boundedProbeOutput{limit: maxOutput + 1}
	cmd.Stdout, cmd.Stderr = out, io.Discard
	if err := cmd.Run(); err != nil || ctx.Err() != nil || len(out.Bytes()) > maxOutput {
		return nil, errAttemptMemoryUnsupported
	}
	finished := clock.Now()
	var sample struct {
		SampledAt time.Time         `json:"sampled_at"`
		GTTMM     map[string]string `json:"gtt_mm"`
	}
	if err := json.Unmarshal(out.Bytes(), &sample); err != nil || sample.SampledAt.IsZero() || sample.SampledAt.Before(started) || sample.SampledAt.After(finished) || finished.Sub(started) > attemptMemorySamplePeriod || len(sample.GTTMM) == 0 {
		return nil, errAttemptMemoryUnsupported
	}
	return sample.GTTMM, nil
}

func (a *memoryAttempt) gpuHostMemory(ctx context.Context, clock Clock, helper, pci string) (float64, bool, error) {
	b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.events"))
	if err != nil {
		return 0, false, err
	}
	events := memoryCounters(b)
	for _, name := range []string{"high", "max", "oom", "oom_kill", "oom_group_kill"} {
		if _, ok := events[name]; !ok {
			return 0, false, errors.New("incomplete cgroup pressure counters")
		}
	}
	ids := map[int]bool{}
	if err := a.processIDs(ids); err != nil {
		return 0, false, err
	}
	clients := map[string]float64{}
	cards := map[string]bool{normalizePCIAddress(pci): true}
	for pid := range ids {
		dir := filepath.Join("/proc", strconv.Itoa(pid), "fdinfo")
		entries, err := a.fs.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} // A process exited since cgroup.procs.
		if err != nil {
			return 0, false, err
		}
		for _, entry := range entries {
			b, err := a.fs.ReadFile(filepath.Join(dir, entry.Name()))
			if errors.Is(err, os.ErrNotExist) {
				continue
			} // A descriptor closed since readdir.
			if err != nil {
				return 0, false, err
			}
			key, gb, err := drmHostMemory(b)
			if errors.Is(err, errAttemptMemoryPending) {
				continue
			}
			if err != nil {
				return 0, false, err
			}
			clients[key] = max(clients[key], gb) // inherited/duplicated fds count once
		}
	}
	total := 0.0
	for _, gb := range clients {
		total += gb
	}
	seen := false
	for key := range clients {
		device, _, _ := strings.Cut(key, "/")
		seen = seen || normalizePCIAddress(device) == normalizePCIAddress(pci)
		cards[normalizePCIAddress(device)] = true
	}
	var managers map[string]string
	if helper != "" {
		managers, err = readGPUTelemetryHelper(ctx, clock, helper)
		if err != nil {
			return 0, false, err
		}
	}
	for card := range cards {
		if card == "" || card == "." || filepath.Base(card) != card {
			return 0, false, errAttemptMemoryUnsupported
		}
		// Runtime corroboration is mandatory, through direct debugfs reads
		// or the explicitly configured helper. Missing evidence rejects it. Count
		// the whole manager, not its net change, so unrelated allocations
		// being freed cannot mask this card's driver-host storage.
		var b []byte
		if helper != "" {
			text, ok := managers[card]
			if !ok {
				return 0, false, errAttemptMemoryUnsupported
			}
			b = []byte(text)
		} else {
			b, err = a.fs.ReadFile(filepath.Join("/sys/kernel/debug/dri", card, "gtt_mm"))
			if err != nil {
				return 0, false, err
			}
		}
		stats := memoryCounters([]byte(strings.ReplaceAll(string(b), ":", "")))
		usage, ok := stats["usage"]
		if !ok || stats["use_type"] != 1 || stats["use_tt"] != 1 {
			return 0, false, errAttemptMemoryUnsupported
		}
		total += float64(usage) / (1 << 30)
	}
	return total, seen, nil
}

// Driver I/O may block in kernel locks. It must neither occupy the resident
// sampler nor hold its peak mutex. One collection period bounds freshness,
// not job runtime; slow/incomplete evidence costs conservative sizing only.
const attemptMemorySamplePeriod = 200 * time.Millisecond

type gpuHostCollection struct {
	driverGB float64
	seen     bool
	complete bool
	last     time.Time
}

func (a *memoryAttempt) collectGPUHost(ctx context.Context, clock Clock, helper, pci string) <-chan gpuHostCollection {
	result := make(chan gpuHostCollection, 1)
	go func() {
		out := gpuHostCollection{complete: true}
		defer func() { result <- out }()
		tick := time.NewTicker(attemptMemorySamplePeriod)
		defer tick.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			start := clock.Now()
			// Finish the in-flight collection at teardown, as for kernel reads.
			// The helper keeps its own deadline; cleanup still waits at most
			// one period and rejects a missing or stale result. Cancellation
			// stops subsequent collections, not a healthy final invocation.
			gb, seen, err := a.gpuHostMemory(context.WithoutCancel(ctx), clock, helper, pci)
			end := clock.Now()
			if err != nil || end.Before(start) || end.Sub(start) > attemptMemorySamplePeriod || !out.last.IsZero() && start.Sub(out.last) > 2*attemptMemorySamplePeriod {
				out.complete = false
				return
			}
			out.driverGB, out.seen, out.last = max(out.driverGB, gb), out.seen || seen, end
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	return result
}
