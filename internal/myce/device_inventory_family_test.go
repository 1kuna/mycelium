package myce

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"testing/fstest"
)

func familyProbe(t *testing.T, nvidiaClass string) MachineProbe {
	t.Helper()
	files := fstest.MapFS{
		"sys/bus/pci/devices/0000:01:00.0/vendor": {Data: []byte("0x8086\n")},
		"sys/bus/pci/devices/0000:01:00.0/class":  {Data: []byte("0x030000\n")},
		"sys/bus/pci/devices/0000:02:00.0/vendor": {Data: []byte("0x8086\n")},
		"sys/bus/pci/devices/0000:02:00.0/class":  {Data: []byte("0x038000\n")},
		"dev/dri/renderD128":                      {}, "dev/dri/renderD129": {},
	}
	if nvidiaClass != "" {
		files["sys/bus/pci/devices/0000:03:00.0/vendor"] = &fstest.MapFile{Data: []byte("0x10de\n")}
		files["sys/bus/pci/devices/0000:03:00.0/class"] = &fstest.MapFile{Data: []byte(nvidiaClass)}
	}
	return MachineProbe{OS: "linux", Arch: "amd64", Files: files,
		Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			if name != "nvidia-smi" {
				t.Fatalf("unexpected command: %s", name)
			}
			// A local shell exits with the same status as the B70's installed nvidia-smi.
			return exec.CommandContext(ctx, "/bin/sh", "-c", "exit 9").Output()
		},
		CommandEnv: func(_ context.Context, env map[string]string, name string, args ...string) ([]byte, error) {
			if name != "python3" || env["ZE_ENABLE_PCI_ID_DEVICE_ORDER"] != "1" || env["ZE_AFFINITY_MASK"] != "" || env["ONEAPI_DEVICE_SELECTOR"] != "" {
				t.Fatalf("unexpected Level Zero probe: %s %v", name, env)
			}
			return []byte(`[{"identity":"b70-0","fingerprint":"B70"},{"identity":"b70-1","fingerprint":"B70"}]`), nil
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
	if status.Healthy || !status.Quiescent || len(status.Attempts) != 0 || status.Calls != 0 || status.Preparations != 0 || status.Sidecars != 0 || len(status.PendingReleases) != 0 || len(status.PendingCleanup) != 0 {
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
	w.InventoryProbe = func(ctx context.Context) ([]MachineDevice, error) {
		return configuration.localInventoryWithProbe(ctx, p)
	}
	runInventoryReviewWorker(t, q, w)
	awaitInventoryReview(t, func() bool { s, _ := q.Snapshot(); return len(s.Machines) == 1 })
	s, err := q.publicSnapshot()
	if err != nil || len(s.Machines[0].Devices) != 2 || !strings.Contains(s.Machines[0].InventoryReason, "cuda") || s.Machines[0].MemoryError != "" {
		t.Fatalf("partial inventory lost at worker API: %+v %v", s.Machines, err)
	}
	devices, probeErr := w.InventoryProbe(context.Background())
	if len(devices) != 2 {
		t.Fatalf("local inventory lost partial results: %+v %v", devices, probeErr)
	}
	w.Machine.Devices = devices
	if _, err := w.forAttempt(Job{AllocatedDevices: []string{"0"}}); err != nil {
		t.Fatalf("healthy XPU launch blocked by CUDA: %v", err)
	}
}
