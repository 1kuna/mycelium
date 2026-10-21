package myce

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestCapabilityProfileMinimalPath(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS system utility lookup")
	}
	t.Setenv("PATH", "/usr/bin:/bin")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, gpu := range []string{"fake", "metal"} {
		if gpu == "metal" && runtime.GOARCH != "arm64" {
			continue
		}
		profile := LocalProfileProbe().Detect(ctx, Machine{GPU: gpu}, t.TempDir(), time.Time{})
		if profile.SystemMemoryBytes == 0 || len(profile.Unavailable) != 0 {
			t.Fatalf("%s hardware unavailable with minimal PATH: %v", gpu, profile.Unavailable)
		}
	}
}

func TestCapabilityProfileRealMemoryLayouts(t *testing.T) {
	for _, tc := range []struct {
		name, os, arch, gpu, output, layout string
		system, accelerator                 uint64
	}{
		{"Apple", "darwin", "arm64", "metal", "Metal Support: Metal 3", "unified", 32 << 30, 32 << 30},
		{"Spark", "linux", "arm64", "cuda", "NVIDIA GB10, N/A", "unified", 128 << 30, 128 << 30},
		{"Discrete", "linux", "amd64", "cuda", "RTX 4090, 24576", "separate", 64 << 30, 24 << 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := ProfileProbe{Machine: MachineProbe{OS: tc.os, Arch: tc.arch, Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "nvidia-smi" && strings.Contains(strings.Join(args, " "), "uuid,driver_version") {
					return []byte("GPU-fixture, driver"), nil
				}
				if name == "/usr/sbin/sysctl" {
					return []byte("34359738368"), nil
				}
				if name == "nvidia-smi" || name == "/usr/sbin/system_profiler" {
					return []byte(tc.output), nil
				}
				return nil, errors.New("unavailable")
			}}, CPUs: func() int { return 8 }, ReadFile: func(string) ([]byte, error) {
				if tc.name == "Spark" {
					return []byte("MemTotal: 134217728 kB\n"), nil
				}
				return []byte("MemTotal: 67108864 kB\n"), nil
			}, Disk: func(string) (uint64, uint64, error) { return 512 << 30, 123 << 30, nil }}
			if tc.gpu == "cuda" {
				probe.Machine = withCUDAFacts(probe.Machine, tc.layout == "unified", 1)
			}
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			profile := probe.Detect(context.Background(), Machine{GPU: tc.gpu}, "ignored", now)
			if profile.MemoryLayout != tc.layout || profile.SystemMemoryBytes != tc.system || len(profile.Accelerators) != 1 || profile.Accelerators[0].MemoryBytes != tc.accelerator || profile.DiskFreeBytes != 123<<30 || profile.RecordedAt != now || len(profile.Unavailable) > 0 {
				t.Fatalf("bad profile: %+v", profile)
			}
		})
	}
}
func TestCapabilityProfileUnknownIsNotBudget(t *testing.T) {
	probe := ProfileProbe{Machine: MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("missing") }}, CPUs: func() int { return 4 }, ReadFile: func(string) ([]byte, error) { return nil, errors.New("missing") }, Disk: func(string) (uint64, uint64, error) { return 0, 0, errors.New("missing") }}
	profile := probe.Detect(context.Background(), Machine{GPU: "cuda", FreeMemoryGB: 99}, "ignored", time.Time{})
	if profile.SystemMemoryBytes != 0 || profile.Accelerators[0].MemoryBytes != 0 || len(profile.Unavailable) != 3 {
		t.Fatalf("invented facts: %+v", profile)
	}
}
