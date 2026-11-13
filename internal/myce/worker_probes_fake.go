//go:build myce_test

package myce

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"testing/fstest"

	"mycelium/internal/probeguard"
)

// End-to-end tests build the ordinary CLI with this tag so its child workers
// inherit synthetic host facts and the same fail-closed probe boundary. Normal
// builds never include this file or expose a runtime switch for fake hardware.
func init() {
	workerHostHealthSource = nil
	workerMountRoots = func(context.Context) ([]string, error) { return []string{"/"}, nil }
	workerDeviceFaultSource = func() DeviceFaultSource { return nil }
	workerFaultHost = func() (map[string]string, []string) { return nil, []string{"synthetic_host"} }
	workerSignalEvidence = func(SignalCapture) *SignalEvidence { return &SignalEvidence{CoreCapture: "unavailable"} }
	SetOnboardingMountCommand(func(context.Context, string, ...string) ([]byte, error) { return nil, nil })
	extensionIdentityProbe = func() MachineProbe {
		return MachineProbe{Files: fstest.MapFS{"proc/cpuinfo": {Data: []byte("model name : synthetic CPU\n")}}, Command: func(context.Context, string, ...string) ([]byte, error) {
			return []byte("synthetic hardware identity"), nil
		}}
	}

	// MYCE_TEST_DISK_FREE_BYTES simulates a low-disk host (e.g. a laptop with
	// 15 GiB free) for these test binaries only.
	free := uint64(1 << 39)
	if v, err := strconv.ParseUint(os.Getenv("MYCE_TEST_DISK_FREE_BYTES"), 10, 64); err == nil {
		free = v
	}
	workerDiskProbe = func(string) (DiskReading, error) {
		return DiskReading{Volume: "synthetic", TotalBytes: 1 << 40, FreeBytes: free}, nil
	}
	workerProfileProbe = func() ProfileProbe {
		return ProfileProbe{Machine: MachineProbe{OS: runtime.GOOS, Arch: runtime.GOARCH, Files: fstest.MapFS{}, Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
			if name == "/usr/sbin/sysctl" {
				return []byte("137438953472"), nil
			}
			return nil, ErrNoGPU
		}}, ReadFile: func(string) ([]byte, error) { return []byte("MemTotal: 134217728 kB\n"), nil }, CPUs: func() int { return 20 }, Disk: func(string) (uint64, uint64, error) { return 1 << 40, free, nil }}
	}
	workerHostMemorySource = func(reserve float64) (MemorySource, error) {
		return func(context.Context) (float64, error) { return max(0, 128-reserve), nil }, nil
	}
	queueHostMemorySource = workerHostMemorySource
	workerOwnerSignals = func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} }
	workerProcessMemory = nil
	workerMemoryRSS = nil
	workerLivenessCPU = nil
	workerIOPressure = func(clock Clock) *IOPressure {
		return &IOPressure{SampledAt: clock.Now(), Some: &PressureStall{TotalUS: 1234}, Full: &PressureStall{}}
	}
	workerResourceCPU = func(context.Context, map[int]string, map[string]float64) (float64, error) { return 1.25, nil }
	probeguard.Before = func() { panic("test invoked a real hardware probe; inject a fake") }
}
