package myce

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestInventoryPendingPreservesCompletedObservation(t *testing.T) {
	for _, initiallyFailed := range []bool{false, true} {
		name := "healthy"
		if initiallyFailed {
			name = "failed"
		}
		t.Run(name, func(t *testing.T) {
			w := Worker{Machine: Machine{GPU: "xpu"}, Clock: RealClock{}}
			s := &workerStore{dir: t.TempDir()}
			devices := inventoryReviewDevices()
			devices[0].Platform, devices[1].Platform = "cuda", "xpu"
			for i := range devices {
				devices[i].CapabilityProfile = &CapabilityProfile{}
			}
			if err := w.refreshInventory(context.Background(), devices, nil, s); err != nil {
				t.Fatal(err)
			}
			if initiallyFailed {
				if err := w.refreshInventory(context.Background(), devices[:1], &acceleratorProbeError{xpu: true}, s); err != nil {
					t.Fatal(err)
				}
			}
			previous, previousReason := w.Machine.Devices, w.inventoryReason
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
					synctest.Wait()
				}()
				var calls atomic.Int32
				probe := func() ([]MachineDevice, error) {
					calls.Add(1)
					<-release
					return devices[:1], &acceleratorProbeError{xpu: true}
				}
				// Repeated heartbeat waits must neither change health nor launch
				// duplicate probes while the same observation is outstanding.
				for i := 0; i < 3; i++ {
					observed, probeErr := s.inventory.run("devices", probe)
					if probeErr == nil {
						t.Fatal("blocked probe unexpectedly completed")
					}
					if err := w.refreshInventory(context.Background(), observed, probeErr, s); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(previous, w.Machine.Devices) || previousReason != w.inventoryReason {
						t.Fatalf("pending probe changed completed observation: reason=%q devices=%+v", w.inventoryReason, w.Machine.Devices)
					}
				}
				if calls.Load() != 1 {
					t.Fatalf("pending probes accumulated: %d", calls.Load())
				}
				close(release)
				synctest.Wait()
				observed, probeErr := s.inventory.run("devices", probe)
				if err := w.refreshInventory(context.Background(), observed, probeErr, s); err != nil {
					t.Fatal(err)
				}
				if w.inventoryReason != "xpu: device_probe_unavailable" || w.Machine.Devices[0].Reason != "" || w.Machine.Devices[1].Reason != "device_probe_unavailable" || w.Machine.Devices[1].FreeMemoryGB != 0 {
					t.Fatalf("completed family failure lost: %+v reason=%s", w.Machine.Devices, w.inventoryReason)
				}
				observed, probeErr = s.inventory.run("devices", func() ([]MachineDevice, error) { return devices, nil })
				if err := w.refreshInventory(context.Background(), observed, probeErr, s); err != nil {
					t.Fatal(err)
				}
				if w.inventoryReason != "" || w.Machine.Devices[0].Reason != "" || w.Machine.Devices[1].Reason != "" || w.Machine.Devices[1].FreeMemoryGB == 0 {
					t.Fatalf("completed recovery remained stale: %+v reason=%s", w.Machine.Devices, w.inventoryReason)
				}
			})
		})
	}
}

func TestInventoryPendingStillAppliesProbeDeadline(t *testing.T) {
	w := Worker{Machine: Machine{GPU: "fake"}, Clock: RealClock{}}
	s := &workerStore{dir: t.TempDir()}
	devices := inventoryReviewDevices()
	for i := range devices {
		devices[i].CapabilityProfile = &CapabilityProfile{}
	}
	if err := w.refreshInventory(context.Background(), devices, nil, s); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer func() { cancel(); synctest.Wait() }()
		probe := func() ([]MachineDevice, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		observed, probeErr := s.inventory.run("devices", probe)
		if err := w.refreshInventory(context.Background(), observed, probeErr, s); err != nil {
			t.Fatal(err)
		}
		if w.inventoryReason != "" {
			t.Fatalf("responsiveness budget mistaken for probe deadline: %s", w.inventoryReason)
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		observed, probeErr = s.inventory.run("devices", probe)
		if !errors.Is(probeErr, context.DeadlineExceeded) {
			t.Fatalf("actual probe deadline lost: %v", probeErr)
		}
		if err := w.refreshInventory(context.Background(), observed, probeErr, s); err != nil {
			t.Fatal(err)
		}
		if w.inventoryReason != "device_probe_unavailable" {
			t.Fatalf("completed probe failure hidden: %s", w.inventoryReason)
		}
		for _, d := range w.Machine.Devices {
			if d.Reason != "device_probe_unavailable" || d.FreeMemoryGB != 0 {
				t.Fatalf("timed-out device advertised capacity: %+v", d)
			}
		}
	})
}
