package myce

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func memoryRefusalReviewWorker(t *testing.T) Worker {
	t.Helper()
	p := familyProbe(t, "0x030000")
	p.Command = func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "nvidia-smi" {
			t.Fatalf("unexpected command %s", name)
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

func TestReviewCurrentCapacityFailureRefusesLaunch(t *testing.T) {
	w := memoryRefusalReviewWorker(t)
	ds, err := w.InventoryProbe(context.Background())
	t.Logf("current devices=%+v error=%v", ds, err)
	if _, err := w.forAttempt(Job{AllocatedDevices: []string{"0"}}); err == nil {
		t.Fatal("launch accepted stale healthy CUDA device despite current capacity-detection failure")
	}
}

func TestReviewCurrentCapacityFailureUnhealthy(t *testing.T) {
	q, _ := fixture(t)
	w := memoryRefusalReviewWorker(t)
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
}

func TestCurrentDeviceRefusalIsFamilyLocal(t *testing.T) {
	for _, platform := range []string{"cuda", "xpu"} {
		for _, reason := range []string{"device_memory_unavailable", "device_budget_unavailable", "device_probe_unavailable"} {
			t.Run(platform+"/"+reason, func(t *testing.T) {
				devices := inventoryReviewDevices()
				devices[0].Platform, devices[1].Platform = "cuda", "xpu"
				w := Worker{Machine: Machine{Devices: devices}}
				failed := false
				w.InventoryProbe = func(context.Context) ([]MachineDevice, error) {
					current := append([]MachineDevice(nil), devices...)
					for i := range current {
						if failed && current[i].Platform == platform {
							current[i].Reason, current[i].FreeMemoryGB = reason, 0
						}
					}
					return current, nil
				}
				// Bind while healthy, then reprobe before each member exec.
				bound := make([]Worker, len(devices))
				for i := range devices {
					var err error
					bound[i], err = w.forAttempt(Job{AllocatedDevices: []string{fmt.Sprint(i)}})
					if err != nil {
						t.Fatal(err)
					}
				}
				failed = true
				for i, device := range devices {
					for _, launch := range []func() error{bound[i].verifyDevice, func() error {
						_, err := w.forAttempt(Job{AllocatedDevices: []string{device.ID}})
						return err
					}} {
						err := launch()
						if device.Platform == platform {
							var rule *rejectionError
							if !errors.As(err, &rule) || rule.code != reason {
								t.Fatalf("current %s refusal lost: %v", platform, err)
							}
						} else if err != nil {
							t.Fatalf("healthy sibling launch refused: %v", err)
						}
					}
				}
				failed = false
				for _, member := range bound {
					if err := member.verifyDevice(); err != nil {
						t.Fatalf("recovered family launch refused: %v", err)
					}
				}
			})
		}
	}
}

func TestCurrentCapacityFailureKeepsSiblingAndCustodyQuiescence(t *testing.T) {
	q, _ := fixture(t)
	w := memoryRefusalReviewWorker(t)
	devices, err := w.InventoryProbe(context.Background())
	if err != nil || len(devices) != 3 || devices[0].Reason != "device_memory_unavailable" {
		t.Fatalf("current family evidence: %+v %v", devices, err)
	}
	w.Machine.Devices = devices
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
	status, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "disable", Stop: true})
	if err != nil || status.Healthy || !status.Quiescent || len(status.Workers) != 1 || !status.Workers[0].Fresh {
		t.Fatalf("capacity health must remain separate from custody: %+v %v", status, err)
	}
}
