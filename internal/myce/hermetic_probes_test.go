package myce

import (
	"context"
	"mycelium/internal/probeguard"
	"os"
	"runtime"
	"testing/fstest"
)

func syntheticProfileProbe() ProfileProbe {
	return ProfileProbe{Machine: MachineProbe{OS: runtime.GOOS, Arch: runtime.GOARCH, Files: fstest.MapFS{}, Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
		if name == "/usr/sbin/sysctl" {
			return []byte("137438953472"), nil
		}
		return nil, ErrNoGPU
	}}, ReadFile: func(string) ([]byte, error) { return []byte("MemTotal: 134217728 kB\n"), nil }, CPUs: func() int { return 20 }, Disk: func(string) (uint64, uint64, error) { return 1 << 40, 1 << 39, nil }}
}

func installSyntheticWorkerHost() {
	workerMountRoots = func(context.Context) ([]string, error) { return []string{"/"}, nil }
	workerCheckpointPrepare = func(j Job) (string, error) {
		dir := attemptCheckpoint(j)
		return dir, os.MkdirAll(dir, 0700)
	}
	extensionIdentityProbe = syntheticIdentityProbe
	workerDeviceFaultSource = func() DeviceFaultSource { return nil }
	workerDiskProbe = func(string) (DiskReading, error) {
		return DiskReading{Volume: "synthetic", TotalBytes: 1 << 40, FreeBytes: 1 << 39}, nil
	}
	workerProfileProbe = syntheticProfileProbe
	workerHostMemorySource = func(reserve float64) (MemorySource, error) {
		return func(context.Context) (float64, error) { return max(0, 128-reserve), nil }, nil
	}
	queueHostMemorySource = workerHostMemorySource
	workerOwnerSignals = func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} }
	workerProcessMemory = nil
	workerMemoryRSS = nil
	workerLivenessCPU = nil
	workerResourceCPU = nil
	workerIOPressure = nil
	probeguard.Before = func() { panic("test invoked a real hardware probe; inject a fake") }
}

// Simulate the conflicting GB10 inventory without executing a host tool or
// opening a device. The maintenance fixture must never consult this default.

func syntheticIdentityProbe() MachineProbe {
	return MachineProbe{Files: fstest.MapFS{"proc/cpuinfo": {Data: []byte("model name : synthetic CPU\n")}}, Command: func(context.Context, string, ...string) ([]byte, error) {
		return []byte("synthetic hardware identity"), nil
	}}
}

// A lifecycle fixture must not scan or copy the previous attempt's NAS state.
