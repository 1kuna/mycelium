package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// GPUHostMemory is complete host evidence, separate from device allocations.
// DriverGB counts per-client system/GTT objects plus the independently read
// xe GTT managers, even where they overlap each other or cgroup charges.
// HostDropGB is the MemAvailable drop after subtracting sibling Mycelium
// cgroup growth. OutsideGB retains max(driver objects, adjusted drop minus
// synchronized current nonreclaimable components, zero) across the attempt.
// Unexplained activity outside Mycelium remains a conservative residual.
type GPUHostMemory struct {
	// CUDA has no exhaustive per-client host counter. Its proc upper bound
	// retains absolute outside-root physical occupancy and all exposed pins.
	Source     string   `json:"source,omitempty"`
	PinnedGB   *float64 `json:"pinned_gb,omitempty"`
	PhysicalGB *float64 `json:"physical_gb,omitempty"`
	// Identifies samples collected with sibling subtraction. Legacy evidence
	// is replaced when a corrected accepted sample arrives, including on reopen.
	SiblingAdjusted bool     `json:"sibling_adjusted,omitempty"`
	DriverGB        *float64 `json:"driver_gb"`
	HostDropGB      *float64 `json:"host_drop_gb"`
	OutsideGB       *float64 `json:"outside_gb"`
}

func (p *AttemptMemoryPeak) gpuHostPeakGB() (float64, bool) {
	if p == nil || p.NonreclaimableGB == nil || !validMemoryValue(*p.NonreclaimableGB) || p.GPUHost == nil || p.GPUHost.DriverGB == nil || p.GPUHost.HostDropGB == nil || p.GPUHost.OutsideGB == nil || !validMemoryValue(*p.GPUHost.DriverGB) || !validMemoryValue(*p.GPUHost.HostDropGB) || !validMemoryValue(*p.GPUHost.OutsideGB) || *p.GPUHost.OutsideGB < *p.GPUHost.DriverGB {
		return 0, false
	}
	if p.CombinedHostGB != nil && validMemoryValue(*p.CombinedHostGB) {
		return *p.CombinedHostGB, true
	}
	resident := *p.NonreclaimableGB
	if p.HostGB != nil && validMemoryValue(*p.HostGB) {
		resident = max(resident, *p.HostGB)
	}
	return max(resident+*p.GPUHost.OutsideGB, *p.GPUHost.HostDropGB), true
}

type gpuHostState struct {
	available          float64
	swap, oom, swapOut uint64
	psi                float64
	psiTotalUS         uint64
	level              int // Darwin: normal=1, warning=2, critical=4.
}

// some includes full stalls, so use its cumulative total once, without adding
// full again. avg10 can stay zero before the kernel's next averaging update.
func memoryPSITotal(b []byte) (uint64, error) {
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "some" {
			continue
		}
		for _, field := range fields[1:] {
			if value, ok := strings.CutPrefix(field, "total="); ok {
				return strconv.ParseUint(value, 10, 64)
			}
		}
	}
	return 0, errors.New("memory PSI total unavailable")
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
	b, err = fs.ReadFile("/proc/pressure/memory")
	if err != nil {
		return nil, err
	}
	psi, err := sharedMemoryPSI(b)
	if err != nil {
		return nil, err
	}
	psiTotal, err := memoryPSITotal(b)
	if err != nil {
		return nil, err
	}
	return &gpuHostState{available: available, swap: total - free, oom: oom, swapOut: out, psi: psi, psiTotalUS: psiTotal}, nil
}

func validateGPUTelemetryHelper(path string) error {
	if path != "" && (!filepath.IsAbs(path) || strings.ContainsAny(path, "\x00\n\r")) {
		return errors.New("gpu-telemetry-helper must be an absolute, single-line executable path")
	}
	return nil
}

// The operator supplies an executable, not a command line. A local wrapper can
// exec the unprivileged broker client; neither job input nor worker credentials
// are sent to it. The timestamp must belong to this invocation, not a cache.
type gpuDeviceMemory struct {
	TotalBytes *uint64 `json:"total_bytes"`
	UsedBytes  *uint64 `json:"used_bytes"`
}

type gpuTelemetrySnapshot struct {
	SampledAt time.Time                  `json:"sampled_at"`
	GTTMM     map[string]string          `json:"gtt_mm"`
	VRAM      map[string]gpuDeviceMemory `json:"vram"`
}

func readGPUTelemetryHelper(ctx context.Context, clock Clock, path string) (map[string]string, error) {
	s, err := readGPUTelemetrySnapshot(ctx, clock, path)
	if err != nil {
		return nil, err
	}
	return s.GTTMM, nil
}

func readGPUTelemetrySnapshot(ctx context.Context, clock Clock, path string) (*gpuTelemetrySnapshot, error) {
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
	return decodeGPUTelemetrySnapshot(out.Bytes(), started, finished)
}

func decodeGPUTelemetrySnapshot(data []byte, started, finished time.Time) (*gpuTelemetrySnapshot, error) {
	var sample gpuTelemetrySnapshot
	if err := json.Unmarshal(data, &sample); err != nil || sample.SampledAt.IsZero() || sample.SampledAt.Before(started) || sample.SampledAt.After(finished) || finished.Sub(started) > attemptMemorySamplePeriod || len(sample.GTTMM) == 0 {
		return nil, errAttemptMemoryUnsupported
	}
	return &sample, nil
}

// HostMemorySampling explains why host evidence was or was not promoted.
// Labels are worker-owned predicates, never raw errors or private paths.
// Failure-only proc details identify an unmeasured owner without a status dump.
type HostMemorySampling struct {
	Complete             bool    `json:"complete"`
	Reason               string  `json:"reason"`
	Samples              int     `json:"samples"`
	MaxCollectionSeconds float64 `json:"max_collection_seconds"`
	MaxIntervalSeconds   float64 `json:"max_interval_seconds"`
	FinalAgeSeconds      float64 `json:"final_age_seconds"`
	FailedPID            int     `json:"failed_pid,omitempty"`
	FailedState          string  `json:"failed_state,omitempty"`
	FailedCounter        string  `json:"failed_counter,omitempty"`
	FailedCounterReason  string  `json:"failed_counter_reason,omitempty"`
}

func cardTelemetry(m *Machine) *DeviceTelemetry {
	for _, d := range m.Devices {
		if d.ID == m.Device {
			return d.Telemetry
		}
	}
	return nil
}
