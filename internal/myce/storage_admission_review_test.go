package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStorageAdmissionShadowPreservesLegacyAdmission(t *testing.T) {
	q, _ := fixture(t)
	q.deliveryRoot = q.root
	q.files.limit = 8
	low := submit(t, q, storageAdmissionSpec("experiment", 8))
	_, err := q.Submit(storageAdmissionSpec("production", 6))
	assertFilePolicyRefusal(t, err, "shortfall=6")
	if len(q.s.Jobs) != 1 || q.s.Jobs[low.ID].OutputAdmissionPending || q.s.Jobs[low.ID].OutputReservationBytes != nil {
		t.Fatal("shadow changed legacy commitment or created production hold")
	}
	// Existing accepted jobs are also shadowed without yielding either one.
	q.files.limit = 14
	high := submit(t, q, storageAdmissionSpec("production", 6))
	q.files.limit = 8
	snapshot, err := q.statusSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	report := snapshot.FileStore.Admission
	if report == nil || report.Mode != "shadow" || len(report.Proposals) != 2 || report.Proposals[0].JobID != high.ID || !report.Proposals[0].Admitted || report.Proposals[1].Admitted || len(report.YieldedCommitments) != 1 || report.YieldedCommitments[0] != low.ID {
		t.Fatalf("missing tier shadow: %+v", report)
	}
	for _, j := range q.s.Jobs {
		if j.OutputAdmissionPending || j.OutputReservationBytes != nil {
			t.Fatal("status mutated live admission", j.ID)
		}
	}
	if got := q.reservedOutputBytes(nil); got != 14 {
		t.Fatal("shadow changed live declared reservations", got)
	}
}

func TestStorageAdmissionShadowReportsLearningButKeepsDeclaredBudget(t *testing.T) {
	q, c := fixture(t)
	q.deliveryRoot = q.root
	_, source := retentionUpload(t, deliveryClient(t, q), "")
	s := storageAdmissionSpec("experiment", 8)
	s.Files = FileManifest{".myce-source.tar": source}
	m := machine("learned", "fake")
	check(t, q, m, nil)
	j := submit(t, q, s)
	key := q.storageKey(job(t, q, j.ID), &m)
	if key == nil {
		t.Fatal("no learned identity")
	}
	q.s.StorageObservations = map[string]StorageObservation{identityHash(key): {Key: *key, Output: StorageRange{Samples: 1, Min: 4, Peak: 4}, OutputRecordedAt: c.Now()}}
	q.files.limit = 12
	_, err := q.Submit(s)
	assertFilePolicyRefusal(t, err, "shortfall=4")
	snapshot, err := q.statusSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if p := snapshot.FileStore.Admission.Proposals; len(p) != 1 || p[0].OutputBytes != 4 || !p[0].Admitted {
		t.Fatalf("missing proposed learning: %+v", p)
	}
	if q.s.Jobs[j.ID].OutputReservationBytes != nil || q.reservedOutputBytes(nil) != 8 {
		t.Fatal("shadow froze learned need instead of legacy declaration")
	}
}

func TestStorageAdmissionShadowKeepsLegacyFIFORetries(t *testing.T) {
	q, c := fixture(t)
	q.deliveryRoot = q.root
	q.files.limit = 16
	low := submit(t, q, storageAdmissionSpec("experiment", 8))
	c.Advance(time.Second)
	high := submit(t, q, storageAdmissionSpec("production", 8))
	if err := q.update(func() error {
		for _, id := range []string{low.ID, high.ID} {
			q.touchJob(id).OutputAdmissionPending = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q.files.limit = 8
	if err := q.admitOutputRetries(context.Background()); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[low.ID].OutputAdmissionPending || !q.s.Jobs[high.ID].OutputAdmissionPending {
		t.Fatal("shadow replaced FIFO with tier retries")
	}
	if q.s.Jobs[low.ID].OutputReservationBytes != nil {
		t.Fatal("shadow froze retry budget")
	}
}

func TestStorageAdmissionShadowDoesNotCollectOnSubmit(t *testing.T) {
	q, c := fixture(t)
	order := []string{"z", "a"}
	q.orderStorageReclaim(order)
	if order[0] != "z" || order[1] != "a" {
		t.Fatal("shadow reordered collector candidates")
	}
	q.deliveryRoot = q.root
	client := deliveryClient(t, q)
	_, ref := retentionUpload(t, client, "retired!")
	q.s.RetiredJobFiles = map[string]bool{ref.SHA256: true}
	c.Advance(filePublicationGrace + time.Second)
	old := c.Now().Add(-filePublicationGrace - time.Second)
	if err := os.Chtimes(q.files.blob(ref), old, old); err != nil {
		t.Fatal(err)
	}
	q.files.limit = 8
	_, err := q.Submit(storageAdmissionSpec("production", 1))
	assertFilePolicyRefusal(t, err, "shortfall=1")
	if err := q.files.verify(context.Background(), ref); err != nil {
		t.Fatal("shadow submission collected bytes", err)
	}
}

func TestStorageAdmissionShadowDoesNotYieldForInputOrResult(t *testing.T) {
	t.Run("input", func(t *testing.T) {
		q, _ := fixture(t)
		q.deliveryRoot = q.root
		q.files.limit = 8
		low := submit(t, q, storageAdmissionSpec("experiment", 8))
		p := q.PriorityPolicy()
		p.Policy.Projects = map[string]ProjectPolicy{"service": {DefaultCallPriority: "production", Weight: 1}}
		if _, err := q.SetPriorityPolicy(p.Revision, p.Policy); err != nil {
			t.Fatal(err)
		}
		ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "service", Class: "production"})
		_, ref := policyInput(t, "sixsix")
		assertFilePolicyRefusal(t, deliveryClient(t, q).PreflightFileStore(ctx, FileManifest{"input": ref}), "shortfall=6")
		if q.s.Jobs[low.ID].OutputAdmissionPending {
			t.Fatal("shadow input yielded commitment")
		}
	})
	t.Run("result", func(t *testing.T) {
		q, _ := fixture(t)
		q.deliveryRoot = q.root
		q.files.limit = 16
		high := submit(t, q, storageAdmissionSpec("production", 8))
		assigned := check(t, q, machine("writer", "fake"), nil).Job
		if assigned == nil || assigned.ID != high.ID {
			t.Fatal("missing production assignment")
		}
		low := submit(t, q, storageAdmissionSpec("experiment", 8))
		assigned.Spec.Run.Workdir = t.TempDir()
		if err := os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("123456789"), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := (Worker{Client: deliveryClient(t, q)}).finishFileResult(*assigned, result(assigned, true), t.TempDir())
		assertFilePolicyRefusal(t, err, "shortfall=1")
		if q.s.Jobs[low.ID].OutputAdmissionPending || q.s.Jobs[high.ID].PendingOutputRefs != nil {
			t.Fatal("shadow result changed commitments")
		}
	})
}

func TestStorageAdmissionOutputDeliveryHealthUsesObservedFailure(t *testing.T) {
	q, _ := fixture(t)
	q.s.StorageAdmissionActive = true
	j := deliveryAssignment(t, q)
	r := deliveryResult(t, q, j, "12345678")
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	q.deliveryPublish = func(Job, *FileStore, string, func()) error { return errors.New("destination offline") }
	awaitDeliveryState(t, q, j.ID, "failed")
	if !q.nasAvailable {
		t.Fatal("test requires healthy checkpoint storage")
	}
	q.files.limit = 8
	held := submit(t, q, storageAdmissionSpec("production", 6))
	if !held.OutputAdmissionPending || !strings.Contains(held.Reason, "destination=failed/"+j.ID) || !strings.Contains(held.Reason, "publication_or_receipt_verification_failed") {
		t.Fatalf("failed delivery misreported: %s", held.Reason)
	}
	alerts := alertsOf(q, "production_storage_capacity")
	if len(alerts) != 1 || !strings.Contains(alerts[0].Reason, "destination=failed/"+j.ID) {
		t.Fatalf("alert lost delivery failure: %+v", alerts)
	}
}

func TestStorageAdmissionUnavailableAlternateDoesNotHideCapacityShortage(t *testing.T) {
	for _, state := range []string{"gone", "stale", "recovering", "leaving"} {
		t.Run(state, func(t *testing.T) {
			q, c, _ := protectionQueue(t)
			q.s.StorageAdmissionActive = true
			m, other := protectionMachine("full", "fake"), protectionMachine("alternate", "fake")
			b := protectionBackend(m, other)
			for i := range b.snapshot.Workers {
				b.snapshot.Workers[i].ExplorationBytes = 6
			}
			if err := q.ConfigureStorageProtection(context.Background(), b, protectionControlWork(t, c)); err != nil {
				t.Fatal(err)
			}
			check(t, q, m, nil)
			check(t, q, other, nil)
			switch state {
			case "gone":
				q.s.Machines[other.ID].Gone = true
				// A physical-disk shortage must not discard the full host
				// from the very diagnostic that explains its storage hold.
				m.DiskVolumes = []DiskVolume{{Roots: []string{"data"}, TotalBytes: 1 << 30, FreeBytes: 0, SampledAt: c.Now()}}
			case "stale":
				c.Advance(LossAfter + time.Second)
			case "recovering":
				q.s.Machines[other.ID].Recovering = true
			case "leaving":
				q.s.Machines[other.ID].Leaving = true
			}
			q.s.StorageProtection.Allocations["retained"] = StorageAllocation{ID: "retained", Execution: Execution{ID: "protected-holder"}, Backing: b.snapshot.Workers[0].ProductionBacking, Bytes: 100 << 20, Files: 100}
			j := submit(t, q, protectionSpec("production"))
			if offer := check(t, q, m, nil).Job; offer != nil {
				t.Fatal("unexpected placement", offer.ID)
			}
			alerts := alertsOf(q, "production_storage_capacity")
			if len(alerts) != 1 || !strings.Contains(alerts[0].Reason, "deficit=6 bytes") || !strings.Contains(job(t, q, j.ID).Reason, "protected-holder") {
				t.Fatalf("unavailable alternate hid shortage: alerts=%+v reason=%s", alerts, job(t, q, j.ID).Reason)
			}
		})
	}
}

func TestStorageAdmissionShadowPreservesProductionReserve(t *testing.T) {
	q, _ := fixture(t)
	q.deliveryRoot = q.root
	q.files.limit = 12
	q.fileStoreProductionReserve = 4
	q.files.productionReserve = 4
	low := submit(t, q, storageAdmissionSpec("experiment", 8))
	q.files.limit = 8
	snapshot, err := q.statusSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	p := snapshot.FileStore.Admission.Proposals
	if len(p) != 1 || p[0].Admitted || !strings.Contains(p[0].Reason, "free=4 bytes") {
		t.Fatalf("shadow ignored live reserve: %+v", p)
	}
	if q.s.Jobs[low.ID].OutputAdmissionPending || q.reservedOutputBytes(nil) != 8 {
		t.Fatal("reserve shadow mutated live admission")
	}
}

func TestStorageAdmissionDeliveryHealthIsUnknownWithoutObservation(t *testing.T) {
	q, _ := fixture(t)
	for _, healthy := range []bool{true, false} {
		q.nasAvailable = healthy
		if got := q.outputDeliveryHealth(); got != "unknown" {
			t.Fatalf("checkpoint probe became output observation: %s", got)
		}
	}
}
