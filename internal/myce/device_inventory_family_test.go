package myce

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
)

func familyProbe(t *testing.T, nvidiaClass string) MachineProbe {
	t.Helper()
	files := fstest.MapFS{
		"sys/bus/pci/devices/0000:01:00.0/vendor":             {Data: []byte("0x8086\n")},
		"sys/bus/pci/devices/0000:01:00.0/class":              {Data: []byte("0x030000\n")},
		"sys/bus/pci/devices/0000:02:00.0/vendor":             {Data: []byte("0x8086\n")},
		"sys/bus/pci/devices/0000:02:00.0/class":              {Data: []byte("0x038000\n")},
		"sys/bus/pci/devices/0000:01:00.0/current_link_width": {Data: []byte("1")},
		"sys/bus/pci/devices/0000:02:00.0/current_link_width": {Data: []byte("1")},
		"dev/dri/renderD128":                                  {}, "dev/dri/renderD129": {},
	}
	if nvidiaClass != "" {
		files["sys/bus/pci/devices/0000:03:00.0/vendor"] = &fstest.MapFile{Data: []byte("0x10de\n")}
		files["sys/bus/pci/devices/0000:03:00.0/class"] = &fstest.MapFile{Data: []byte(nvidiaClass)}
	}
	return MachineProbe{OS: "linux", Arch: "amd64", Files: files,
		Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			// Optional card telemetry is unavailable in this inventory fixture;
			// it must not hide healthy devices or the other family's refusal.
			if name == "xpu-smi" {
				return nil, exec.ErrNotFound
			}
			if name != "nvidia-smi" {
				t.Fatalf("unexpected command: %s", name)
			}
			// A local shell exits with the same status as a discrete-GPU Linux host's installed nvidia-smi.
			return exec.CommandContext(ctx, "/bin/sh", "-c", "exit 9").Output()
		},
		CommandEnv: func(_ context.Context, env map[string]string, name string, args ...string) ([]byte, error) {
			if name != "python3" || env["ZE_ENABLE_PCI_ID_DEVICE_ORDER"] != "1" || env["ZE_AFFINITY_MASK"] != "" || env["ONEAPI_DEVICE_SELECTOR"] != "" {
				t.Fatalf("unexpected Level Zero probe: %s %v", name, env)
			}
			return []byte(`[{"identity":"b70-0","fingerprint":"B70","pci_address":"0000:01:00.0"},{"identity":"b70-1","fingerprint":"B70","pci_address":"0000:02:00.0"}]`), nil
		},
	}
}

func TestAcceleratorFamiliesIndependent(t *testing.T) {
	for _, class := range []string{"", "0x040300", "0x030000", "0x030200"} {
		t.Run("nvidia-class="+class, func(t *testing.T) {
			p := familyProbe(t, class)
			devices, err := p.Accelerators(context.Background())
			if len(devices) != 2 {
				t.Fatalf("Level Zero devices hidden: devices=%+v err=%v", devices, err)
			}
			for i, d := range devices {
				if d.Platform != "xpu" || d.Identity != fmt.Sprintf("b70-%d", i) || d.Selector != fmt.Sprint(i) || d.ID != fmt.Sprint(i) {
					t.Fatalf("unexpected B70 device: %+v", d)
				}
			}
			if strings.HasPrefix(class, "0x03") {
				if err == nil || !strings.Contains(err.Error(), "cuda") {
					t.Fatalf("CUDA refusal lost: %v", err)
				}
			} else if err != nil {
				t.Fatalf("absent NVIDIA display device refused inventory: %v", err)
			}
		})
	}
}

func TestDegradedInventoryZeroCustodyQuiescent(t *testing.T) {
	q, _ := fixture(t)
	m, _ := maintenanceMachines()
	m.Devices, m.MemoryError, m.InventoryReason = []MachineDevice{}, "device_probe_unavailable", "device_probe_unavailable"
	check(t, q, m, nil)
	status := maintenanceChange(t, q, "disable", true)
	if status.Healthy || !status.Quiescent || len(status.Attempts) != 0 || status.Calls != 0 || status.Preparations != 0 || status.Sidecars != 0 || len(status.PendingReleases) != 0 {
		t.Fatalf("inventory health blocks zero-custody maintenance: %+v", status)
	}
}

func TestAcceleratorLevelZeroFailurePreservesCUDA(t *testing.T) {
	p := familyProbe(t, "0x030000")
	p.Command = func(context.Context, string, ...string) ([]byte, error) { return []byte("GPU-card, driver"), nil }
	p.CommandEnv = func(context.Context, map[string]string, string, ...string) ([]byte, error) {
		return nil, errors.New("Level Zero unavailable")
	}
	devices, err := p.Accelerators(context.Background())
	if len(devices) != 1 || devices[0].Platform != "cuda" || err == nil || !strings.Contains(err.Error(), "xpu") {
		t.Fatalf("CUDA devices hidden by Level Zero: %+v %v", devices, err)
	}
}

func TestAcceleratorPartialInventoryWorker(t *testing.T) {
	q, _ := fixture(t)
	p := familyProbe(t, "0x030000")
	w := Worker{Machine: machine("mixed", "xpu", "mixed-host"), FixedAcceleratorMemory: true}
	configuration := w
	var calls atomic.Int32
	scan := make(chan struct{})
	release := sync.OnceFunc(func() { close(scan) })
	defer release()
	w.InventoryProbe = func(ctx context.Context) ([]MachineDevice, error) {
		if calls.Add(1) > 1 {
			select {
			case <-scan:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return configuration.localInventoryWithProbe(ctx, p)
	}
	runInventoryReviewWorker(t, q, w)
	// Hold the asynchronous scan after the synchronous startup probe. A first
	// check-in is registration, not proof that current inventory is available.
	initial := awaitInventoryReviewMachine(t, q, w.Machine.ID, nil)
	if initial.MemoryError != "device_probe_unavailable" {
		t.Fatalf("pending scan advertised current capacity: %+v", *initial)
	}
	release()
	observed := awaitInventoryReviewMachine(t, q, w.Machine.ID, func(m *Machine) bool {
		return strings.Contains(m.InventoryReason, "cuda") && m.MemoryError == ""
	})
	if len(observed.Devices) != 2 || !strings.Contains(observed.InventoryReason, "cuda") || observed.MemoryError != "" {
		t.Fatalf("partial inventory lost at worker API: %+v", *observed)
	}
	devices, probeErr := w.InventoryProbe(context.Background())
	if len(devices) != 2 {
		t.Fatalf("local inventory lost partial results: %+v %v", devices, probeErr)
	}
	w.Machine.Devices = devices
	for _, d := range devices {
		if d.Telemetry == nil || d.Telemetry.FreeGB != nil || d.Telemetry.ActivePercent != nil {
			t.Fatalf("unavailable optional telemetry became measured: %+v", d.Telemetry)
		}
	}
	if _, err := w.forAttempt(Job{AllocatedDevices: []string{"0"}}); err != nil {
		t.Fatalf("healthy XPU launch blocked by CUDA: %v", err)
	}
}
