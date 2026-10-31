package myce

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestTaskOutputsAdmissionRequiresCapableDestinationWorker(t *testing.T) {
	for _, missing := range []string{"protocol", "destination"} {
		t.Run(missing, func(t *testing.T) {
			q, c, _ := protectionQueue(t)
			bad, good := protectionMachine("unsupported", "fake"), protectionMachine("capable", "fake")
			good.Capabilities = append(good.Capabilities, taskOutputCapability)
			good.OutputDestinationIDs = []string{"archive"}
			if missing == "destination" {
				bad.Capabilities = append(bad.Capabilities, taskOutputCapability)
				bad.OutputDestinationIDs = []string{"another-destination"}
			}
			if err := q.ConfigureStorageProtection(context.Background(), protectionBackend(bad, good), protectionControlWork(t, c)); err != nil {
				t.Fatal(err)
			}
			check(t, q, bad, nil)
			check(t, q, good, nil)
			s := protectionSpec("experiment")
			s.OutputDestination = &OutputDestination{Protocol: taskOutputProtocol, Kind: "nas", ID: "archive"}
			j := submit(t, q, s)
			if offer := check(t, q, bad, nil).Job; offer != nil {
				t.Fatal("unsupported delivery worker received assignment", missing)
			}
			if len(q.s.StorageProtection.Allocations) != 0 {
				t.Fatal("unsupported worker acquired an allocation")
			}
			if offer := check(t, q, good, nil).Job; offer == nil || offer.ID != j.ID {
				t.Fatal("capable alternate was blocked", offer)
			}
			// An online but incapable alternate cannot hide the capable worker's
			// charged backlog in #507's capacity diagnostics.
			next := submit(t, q, s)
			if reason := q.protectedStorageWaitingReason(next, ""); !strings.Contains(reason, "custody budget full") {
				t.Fatal("incapable alternate hid retained-custody pressure", reason)
			}
			if q.s.StorageAdmissionActive {
				t.Fatal("delivery integration activated tier admission")
			}
		})
	}
}

func TestTaskOutputsDeliveryHealthFeedsAdmissionAlertsAndRecovery(t *testing.T) {
	f := newTaskOutputFixture(t, "nas", "")
	if err := os.Rename(f.nas, f.nas+"-offline"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Rename(f.nas+"-offline", f.nas) })
	f.pending(t)
	if !f.q.nasAvailable {
		t.Fatal("checkpoint health must remain healthy during destination failure")
	}
	// Explicit test activation exercises the existing production hold/alert
	// path. Integration itself must leave the production latch in shadow.
	f.q.s.StorageAdmissionActive = true
	f.q.deliveryRoot = t.TempDir()
	retentionUpload(t, f.w.Client, "occupied")
	f.q.files.limit = 8
	held := submit(t, f.q, storageAdmissionSpec("production", 6))
	want := "failed/" + f.j.ID + "@archive:publication_or_receipt_verification_failed"
	if !held.OutputAdmissionPending || !strings.Contains(held.Reason, want) {
		t.Fatal("production hold lost exact worker-local destination failure", held.Reason)
	}
	alerts := alertsOf(f.q, "production_storage_capacity")
	if len(alerts) != 1 || !strings.Contains(alerts[0].Reason, want) || alerts[0].Project != held.Spec.Project || len(alerts[0].Recipients) != 2 {
		t.Fatal("delivery failure did not reach routed production alert", alerts)
	}
	if err := os.Rename(f.nas+"-offline", f.nas); err != nil {
		t.Fatal(err)
	}
	f.c.Advance(deliveryRetryDelay)
	f.pending(t)
	if err := f.q.admitOutputRetries(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reason := job(t, f.q, held.ID).Reason; !strings.Contains(reason, "verified/"+f.j.ID+"@archive") || strings.Contains(reason, want) {
		t.Fatal("recovered delivery did not refresh hold diagnostics", reason)
	}
	if len(alertsOf(f.q, "production_storage_capacity")) != 1 {
		t.Fatal("delivery recovery flooded the existing pressure incident")
	}
	if err := f.q.update(func() error {
		f.q.touchJob(f.j.ID).Delivery.Execution.Attempt++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.q.mu.RLock()
	health := f.q.outputDeliveryHealth()
	f.q.mu.RUnlock()
	if health != "unknown" {
		t.Fatal("stale local delivery receipt advertised health", health)
	}
}
