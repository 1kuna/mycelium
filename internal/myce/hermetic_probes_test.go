package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
		if !filepath.IsAbs(dir) {
			// Without a checkpoint root the path resolves against the package
			// directory; fail the test that caused it, not the whole binary.
			return "", fmt.Errorf("test worker has no checkpoint root: relative checkpoint %s", dir)
		}
		return dir, os.MkdirAll(dir, 0700)
	}
	extensionIdentityProbe = syntheticIdentityProbe
	workerDeviceFaultSource = func() DeviceFaultSource { return nil }
	workerFaultHost = func() (map[string]string, []string) { return nil, []string{"synthetic_host"} }
	workerSignalEvidence = func(SignalCapture) *SignalEvidence { return &SignalEvidence{CoreCapture: "unavailable"} }
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

func TestRealProbeGuard(t *testing.T) {
	for name, call := range map[string]func(){
		"io-pressure": func() { _ = localIOPressure(RealClock{}) },
		"resource-cpu": func() {
			_, _ = localResourceCPU(context.Background(), nil, nil)
		},
		"profile":  func() { _ = LocalProfileProbe().Detect(context.Background(), Machine{GPU: "metal"}, ".", time.Time{}) },
		"disk":     func() { _, _, _ = LocalProfileProbe().Disk(".") },
		"identity": func() { _, _ = localExtensionIdentityProbe().Command(context.Background(), "nvidia-smi") },
		"level-zero": func() {
			_, _ = LocalMachineProbe().CommandEnv(context.Background(), map[string]string{}, "python3", "-c", levelZeroInventory)
		},
		"metal": func() {
			p := LocalMachineProbe()
			p.OS, p.Arch = "darwin", "arm64"
			_, _ = p.Detect(context.Background())
		},
		"access":     func() { _, _ = LocalGPUAccessProbe().Glob("/dev/dri/renderD*") },
		"command":    func() { _, _ = LocalMachineProbe().Command(context.Background(), "nvidia-smi") },
		"filesystem": func() { _, _ = LocalMachineProbe().Files.Open("dev/dri") },
		"runtime":    func() { _ = (Worker{}).verifyCPUIsolation() },
		"memory": func() {
			source, _ := NewDeviceMemorySource("cuda", "0", 8, 0, "dedicated")
			_, _ = source(context.Background())
		},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if got := recover(); got == nil {
					t.Fatal("real probe escaped guard")
				} else if fmt.Sprint(got) != "test invoked a real hardware probe; inject a fake" {
					t.Fatal(got)
				}
			}()
			call()
		})
	}
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

// A lifecycle fixture must not scan or copy the previous attempt's NAS state.
func TestSyntheticWorkerCheckpointPreparation(t *testing.T) {
	w := recoveryWorker(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"sh", "-c", `printf ready > "$MYCE_CHECKPOINT_DIR/ready"`})
	j.Attempt = 2
	if err := os.MkdirAll(filepath.Join(j.Checkpoint, "attempt-1"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("absent", filepath.Join(j.Checkpoint, "attempt-1", "old-save")); err != nil {
		t.Fatal(err)
	}
	p, err := startAfterFixtureInputs(t, w, store, j)
	if err != nil {
		t.Fatalf("synthetic preparation consulted prior storage: %v", err)
	}
	if f := <-p.done; !f.result.Success {
		t.Fatal(f)
	}
	if data, err := os.ReadFile(filepath.Join(attemptCheckpoint(j), "ready")); err != nil || string(data) != "ready" {
		t.Fatal("attempt-local directory", err)
	}
}
