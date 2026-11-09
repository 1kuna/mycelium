package myce

import (
	"context"
	"errors"

	"strings"
	"testing"
)

func memoryRefusalReviewWorker(t *testing.T) Worker {
	t.Helper()
	p := familyProbe(t, "0x030000")
	inventoryCommand := p.Command
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name != "nvidia-smi" {
			return inventoryCommand(ctx, name, args...)
		}
		if len(args) > 0 && strings.Contains(args[0], "uuid,driver_version") {
			return []byte("GPU-card, driver"), nil
		}
		// UUID discovery succeeds, but the immediately current capacity probe fails.
		return []byte("NVIDIA card, invalid-memory"), nil
	}
	w := Worker{Machine: machine("memory-review", "cuda", "host"), FixedAcceleratorMemory: true}
	old := MachineDevice{ID: "0", Platform: "cuda", Identity: "GPU-card", Selector: "GPU-card", Fingerprint: "driver", MemoryMode: "dedicated", TotalMemoryGB: 10, ReserveMemoryGB: 2, BudgetGB: 8, FreeMemoryGB: 8}
	old.CapabilityProfile = w.Machine.CapabilityProfile
	w.Machine.Devices = []MachineDevice{old}
	configuration := w
	w.InventoryProbe = func(ctx context.Context) ([]MachineDevice, error) {
		return configuration.localInventoryWithProbe(ctx, p)
	}
	return w
}

func TestReviewCurrentCapacityFailureUnhealthy(t *testing.T) {
	q, _ := fixture(t)
	w := memoryRefusalReviewWorker(t)
	devices, err := w.InventoryProbe(context.Background())
	if err != nil || len(devices) != 3 || devices[0].Reason != "device_memory_unavailable" {
		t.Fatalf("current family evidence: %+v %v", devices, err)
	}
	probe := w.InventoryProbe
	healthy := append([]MachineDevice(nil), devices...)
	healthy[0] = w.Machine.Devices[0]
	w.Machine.Devices = healthy
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return healthy, nil }
	bound := make([]Worker, len(healthy))
	for i, device := range healthy {
		bound[i], err = w.forAttempt(Job{AllocatedDevices: []string{device.ID}})
		if err != nil {
			t.Fatal(err)
		}
	}
	w.InventoryProbe = probe
	for i, device := range healthy {
		err := bound[i].verifyDevice()
		if device.Platform == "cuda" {
			var rule *rejectionError
			if !errors.As(err, &rule) || rule.code != "device_memory_unavailable" {
				t.Fatalf("current %s refusal lost: %v", device.Platform, err)
			}
		} else if err != nil {
			t.Fatalf("healthy sibling launch refused: %v", err)
		}
	}
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return healthy, nil }
	for _, member := range bound {
		if err := member.verifyDevice(); err != nil {
			t.Fatalf("recovered family launch refused: %v", err)
		}
	}
	w.InventoryProbe = probe
	w.Machine.Devices = devices
	if _, err := w.forAttempt(Job{AllocatedDevices: []string{"0"}}); err == nil {
		t.Fatal("launch accepted stale healthy CUDA device despite current capacity-detection failure")
	}
	for _, d := range devices[1:] {
		if d.Platform != "xpu" || d.Reason != "" || d.BudgetGB <= 0 {
			t.Fatalf("healthy XPU capacity lost: %+v", d)
		}
		if _, err := w.forAttempt(Job{AllocatedDevices: []string{d.ID}}); err != nil {
			t.Fatalf("CUDA capacity refusal hid XPU: %v", err)
		}
	}
	runInventoryReviewWorker(t, q, w)
	awaitInventoryReview(t, func() bool { s, _ := q.Snapshot(); return len(s.Machines) == 1 })
	status, err := q.HostMaintenanceStatus("host")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range status.Workers[0].Machine.Devices {
		t.Logf("device platform=%s reason=%s", d.Platform, d.Reason)
	}
	if status.Healthy {
		t.Fatal("degraded capacity inventory reported Healthy=true")
	}

	status, err = q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "disable", Stop: true})
	if err != nil || status.Healthy || !status.Quiescent || len(status.Workers) != 1 || !status.Workers[0].Fresh {
		t.Fatalf("capacity health must remain separate from custody: %+v %v", status, err)
	}

	cpu := machine("cpu-only", "cpu", "cpu-only-host")
	cpu.Device = "0"
	runInventoryReviewWorker(t, q, Worker{Machine: cpu, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return []MachineDevice{}, nil }})
	awaitInventoryReview(t, func() bool { s, _ := q.Snapshot(); return len(s.Machines) == 2 })
	snapshot, _ := q.Snapshot()
	for _, m := range snapshot.Machines {
		if m.ID == cpu.ID && m.MemoryError != "" {
			t.Fatal("CPU-only inventory reported an accelerator probe error")
		}
	}
}
