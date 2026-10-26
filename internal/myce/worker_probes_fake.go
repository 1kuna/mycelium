//go:build myce_test

package myce

import (
	"context"
	"runtime"
	"testing/fstest"

	"mycelium/internal/probeguard"
)

// End-to-end tests build the ordinary CLI with this tag so its child workers
// inherit synthetic host facts and the same fail-closed probe boundary. Normal
// builds never include this file or expose a runtime switch for fake hardware.
func init() {
	workerDeviceFaultSource = func() DeviceFaultSource { return nil }
	SetOnboardingMountCommand(func(context.Context, string, ...string) ([]byte, error) { return nil, nil })
	extensionIdentityProbe = func() MachineProbe {
		return MachineProbe{Files: fstest.MapFS{"proc/cpuinfo": {Data: []byte("model name : synthetic CPU\n")}}, Command: func(context.Context, string, ...string) ([]byte, error) {
			return []byte("synthetic hardware identity"), nil
		}}
	}

	workerDiskProbe = func(string) (DiskReading, error) {
		return DiskReading{Volume: "synthetic", TotalBytes: 1 << 40, FreeBytes: 1 << 39}, nil
	}
	workerProfileProbe = func() ProfileProbe {
		return ProfileProbe{Machine: MachineProbe{OS: runtime.GOOS, Arch: runtime.GOARCH, Files: fstest.MapFS{}, Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
			if name == "/usr/sbin/sysctl" {
				return []byte("137438953472"), nil
			}
			return nil, ErrNoGPU
		}}, ReadFile: func(string) ([]byte, error) { return []byte("MemTotal: 134217728 kB\n"), nil }, CPUs: func() int { return 20 }, Disk: func(string) (uint64, uint64, error) { return 1 << 40, 1 << 39, nil }}
	}
	workerHostMemorySource = func(reserve float64) (MemorySource, error) {
		return func(context.Context) (float64, error) { return max(0, 128-reserve), nil }, nil
	}
	workerOwnerSignals = func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} }
	workerProcessMemory = nil
	probeguard.Before = func() { panic("test invoked a real hardware probe; inject a fake") }
}
