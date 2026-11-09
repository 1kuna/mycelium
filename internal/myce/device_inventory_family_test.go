package myce

import (
	"context"
	"errors"

	"os/exec"
	"strings"
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
	for _, d := range devices {
		if d.Telemetry == nil || d.Telemetry.FreeGB != nil || d.Telemetry.ActivePercent != nil {
			t.Fatalf("unavailable optional telemetry became measured: %+v", d.Telemetry)
		}
	}
	if _, err := w.forAttempt(Job{AllocatedDevices: []string{"0"}}); err != nil {
		t.Fatalf("healthy XPU launch blocked by CUDA: %v", err)
	}

	t.Run("AcceleratorLevelZeroFailurePreservesCUDA", func(t *testing.T) {

		p := familyProbe(t, "0x030000")
		p.Command = func(context.Context, string, ...string) ([]byte, error) { return []byte("GPU-card, driver"), nil }
		p.CommandEnv = func(context.Context, map[string]string, string, ...string) ([]byte, error) {
			return nil, errors.New("Level Zero unavailable")
		}
		devices, err := p.Accelerators(context.Background())
		if len(devices) != 1 || devices[0].Platform != "cuda" || err == nil || !strings.Contains(err.Error(), "xpu") {
			t.Fatalf("CUDA devices hidden by Level Zero: %+v %v", devices, err)
		}

	})
}
