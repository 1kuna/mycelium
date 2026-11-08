package myce

import (
	"context"
	"os"
	"runtime"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"mycelium/internal/probeguard"
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

func TestSparkInventoryUsesInjectedFilesystem(t *testing.T) {
	probe := MachineProbe{OS: "linux", Arch: "arm64", Files: fstest.MapFS{
		"proc/meminfo":                                        {Data: []byte("MemTotal: 134217728 kB\nMemAvailable: 33554432 kB\n")},
		"dev/dri/renderD128":                                  {},
		"sys/bus/pci/devices/0000:01:00.0/vendor":             {Data: []byte("0x10de")},
		"sys/bus/pci/devices/0000:01:00.0/class":              {Data: []byte("0x030000")},
		"sys/bus/pci/devices/0000:01:00.0/current_link_width": {Data: []byte("0")},
	}, Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "nvidia-smi" {
			t.Fatalf("unexpected device command %s", name)
		}
		if len(args) > 0 && strings.HasPrefix(args[0], "--query-gpu=uuid,driver_version") {
			return []byte("GPU-synthetic-gb10, 580.159.03"), nil
		}
		return []byte("NVIDIA GB10, [N/A]"), nil
	}, CommandEnv: func(context.Context, map[string]string, string, ...string) ([]byte, error) { return []byte("[]"), nil }}
	devices, err := (Worker{Machine: Machine{GPU: "cuda"}}).localInventoryWithProbe(context.Background(), probe)
	if err != nil || len(devices) != 1 {
		t.Fatal(devices, err)
	}
	d := devices[0]
	if d.Identity != "GPU-synthetic-gb10" || !d.UnifiedMemory || !d.MemoryMeasured || d.ReserveMemoryGB != 0 || d.FreeMemoryGB != 32 || d.BudgetGB != 32 || d.Reason != "" {
		t.Fatalf("injected unified-memory inventory: %+v", d)
	}
}

func syntheticIdentityProbe() MachineProbe {
	return MachineProbe{Files: fstest.MapFS{"proc/cpuinfo": {Data: []byte("model name : synthetic CPU\n")}}, Command: func(context.Context, string, ...string) ([]byte, error) {
		return []byte("synthetic hardware identity"), nil
	}}
}
