package myce

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type AcceleratorProfile struct {
	Type        string `json:"type"`
	Name        string `json:"name,omitempty"`
	MemoryBytes uint64 `json:"memory_bytes,omitempty"`
}

// CapabilityProfile records hardware, participation and measured reference throughput.
// Shared memory is counted once, in SystemMemoryBytes, and is also reachable by
// each accelerator. Zero sizes with an unavailable note mean unknown.
type CapabilityProfile struct {
	Benchmark         *BenchmarkResult     `json:"benchmark,omitempty"`
	Availability      *Availability        `json:"availability,omitempty"`
	Version           int                  `json:"version"`
	Onboarding        string               `json:"onboarding"`
	RecordedAt        time.Time            `json:"recorded_at"`
	OS                string               `json:"os"`
	Arch              string               `json:"arch"`
	LogicalCPUs       int                  `json:"logical_cpus"`
	MemoryLayout      string               `json:"memory_layout"`
	SystemMemoryBytes uint64               `json:"system_memory_bytes,omitempty"`
	Accelerators      []AcceleratorProfile `json:"accelerators"`
	DiskTotalBytes    uint64               `json:"disk_total_bytes,omitempty"`
	DiskFreeBytes     uint64               `json:"disk_free_bytes,omitempty"`
	Unavailable       []string             `json:"unavailable,omitempty"`
}
type ProfileProbe struct {
	Machine  MachineProbe
	ReadFile func(string) ([]byte, error)
	CPUs     func() int
	Disk     func(string) (uint64, uint64, error)
}

func LocalProfileProbe() ProfileProbe {
	return ProfileProbe{Machine: LocalMachineProbe(), ReadFile: os.ReadFile, CPUs: runtime.NumCPU, Disk: func(path string) (uint64, uint64, error) {
		var st syscall.Statfs_t
		err := syscall.Statfs(path, &st)
		return st.Blocks * uint64(st.Bsize), st.Bavail * uint64(st.Bsize), err
	}}
}
func (p ProfileProbe) Detect(ctx context.Context, m Machine, disk string, now time.Time) CapabilityProfile {
	profile := CapabilityProfile{Version: 0, Onboarding: "hardware-facts-only; throughput pending", RecordedAt: now, OS: p.Machine.OS, Arch: p.Machine.Arch, LogicalCPUs: p.CPUs(), MemoryLayout: "separate", Accelerators: []AcceleratorProfile{}}
	if m.UnifiedMemory {
		profile.MemoryLayout = "unified"
	}
	var raw []byte
	var err error
	if p.Machine.OS == "darwin" {
		// System facts must not depend on an interactive shell's PATH.
		raw, err = p.Machine.Command(ctx, "/usr/sbin/sysctl", "-n", "hw.memsize")
	} else {
		raw, err = p.ReadFile("/proc/meminfo")
		if err == nil {
			found := false
			for _, line := range strings.Split(string(raw), "\n") {
				fields := strings.Fields(line)
				if len(fields) == 3 && fields[0] == "MemTotal:" && fields[2] == "kB" {
					n, e := strconv.ParseUint(fields[1], 10, 64)
					err = e
					raw = []byte(strconv.FormatUint(n*1024, 10))
					found = true
					break
				}
			}
			if !found {
				err = fmt.Errorf("missing memory total")
			}
		}
	}
	if err == nil {
		profile.SystemMemoryBytes, err = strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	}
	if err != nil || profile.SystemMemoryBytes == 0 {
		profile.Unavailable = append(profile.Unavailable, "system memory size")
	}
	profile.DiskTotalBytes, profile.DiskFreeBytes, err = p.Disk(disk)
	if err != nil {
		profile.DiskTotalBytes, profile.DiskFreeBytes = 0, 0
		profile.Unavailable = append(profile.Unavailable, "local disk size")
	}
	if m.GPU == "cpu" {
		return profile
	}
	if m.GPU == "fake" {
		profile.Accelerators = append(profile.Accelerators, AcceleratorProfile{Type: "fake"})
		return profile
	}
	d, err := p.Machine.DetectDevice(ctx, m.Device)
	if err != nil || d.Platform != m.GPU {
		profile.Unavailable = append(profile.Unavailable, "accelerator facts")
		profile.Accelerators = append(profile.Accelerators, AcceleratorProfile{Type: m.GPU})
		return profile
	}
	accelerator := AcceleratorProfile{Type: d.Platform, Name: d.GPU}
	if d.MemoryMode == "unified" {
		profile.MemoryLayout = "unified"
		accelerator.MemoryBytes = profile.SystemMemoryBytes
	} else if d.MemoryGB > 0 {
		accelerator.MemoryBytes = uint64(d.MemoryGB * (1 << 30))
	}
	profile.Accelerators = append(profile.Accelerators, accelerator)
	return profile
}
func (p CapabilityProfile) Summary() string {
	resources := []string{}
	for _, a := range p.Accelerators {
		resources = append(resources, fmt.Sprintf("%s %.1f GiB", a.Type, float64(a.MemoryBytes)/(1<<30)))
	}
	return fmt.Sprintf("capability profile v%d: %s/%s, %d logical CPUs, %s memory, system %.1f GiB, resources [%s], disk %.1f GiB total / %.1f GiB free; %s; unavailable=%v", p.Version, p.OS, p.Arch, p.LogicalCPUs, p.MemoryLayout, float64(p.SystemMemoryBytes)/(1<<30), strings.Join(resources, ", "), float64(p.DiskTotalBytes)/(1<<30), float64(p.DiskFreeBytes)/(1<<30), p.Onboarding+"; "+p.Benchmark.Summary(), p.Unavailable)
}
