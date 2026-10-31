package myce

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func assertStorageMigrationIndex(t *testing.T, q *Queue) {
	t.Helper()
	q.mu.RLock()
	inputs := q.captureStorageMigration()
	q.mu.RUnlock()
	full, err := q.recomputeStorageMigrationSnapshot(inputs)
	if err != nil {
		t.Fatal(err)
	}
	status, payloads, err := q.files.capacity(context.Background())
	if err != nil {
		status.Error = "file store capacity unavailable"
	}
	if !inputs.agreesWith(full) || !reflect.DeepEqual(inputs.status(status, payloads), full.status(status, payloads)) {
		t.Fatalf("maintained migration inputs differ from full SQLite recomputation: maintained=%+v full=%+v", inputs.status(status, payloads), full.status(status, payloads))
	}
}

func TestStorageMigrationIndexMutationRollbackPolicyAndRestart(t *testing.T) {
	f := migrationQualifiedCycle(t)
	q := f.q
	assertStorageMigrationIndex(t, q)
	_, ref := retentionUpload(t, deliveryClient(t, q), "unique progress")
	s := spec("experiment")
	s.SubmissionKey = "retained-progress"
	j := submit(t, q, s)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error { q.touchJob(j.ID).OutputRefs = FileManifest{"a": ref, "alias": ref}; return nil }); err != nil {
		t.Fatal(err)
	}
	assertStorageMigrationIndex(t, q)
	rollback := errors.New("abort mutation")
	if err := q.update(func() error { q.touchJob(j.ID).OutputRefs = nil; return rollback }); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	assertStorageMigrationIndex(t, q)
	if err := q.update(func() error { q.deleteJob(j.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	assertStorageMigrationIndex(t, q)
	p := q.priorityPolicyRecord()
	p.Policy.Tiers[0].Selectors = append(p.Policy.Tiers[0].Selectors, "*/experiment")
	if _, err := q.SetPriorityPolicy(p.Revision, p.Policy); err != nil {
		t.Fatal(err)
	}
	assertStorageMigrationIndex(t, q)
	root, files := q.root, q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.db, root, f.c, WithFileStore(files, 185<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertStorageMigrationIndex(t, reopened)
}

func TestStorageMigrationActivationAuditsMaintainedInputs(t *testing.T) {
	f := migrationQualifiedCycle(t)
	q := f.q
	assertStorageMigrationIndex(t, q)
	// Simulate an index bookkeeping defect which hides a real protected holder.
	_, ref := retentionUpload(t, deliveryClient(t, q), "audit must find protected progress")
	j := submit(t, q, spec("experiment"))
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error { q.touchJob(j.ID).OutputRefs = FileManifest{"out": ref}; return nil }); err != nil {
		t.Fatal(err)
	}
	q.records.migration.outputs.Delete(ref.SHA256)
	q.records.migration.count = 0
	q.records.migration.bytes = 0
	if !migrationAction(t, q, "status").Ready {
		t.Fatal("fixture did not hide maintained backlog")
	}
	if _, err := q.StorageAdmissionMigration(context.Background(), "activate"); err == nil || q.s.StorageAdmissionActive {
		t.Fatal("full audit did not refuse inconsistent index", err)
	}
	if migrationAction(t, q, "status").Ready {
		t.Fatal("audit disagreement did not invalidate shadow readiness")
	}
}

func TestStorageMigrationActivationRechecksAuditFence(t *testing.T) {
	f := migrationQualifiedCycle(t)
	q := f.q
	calls := 0
	q.checkpointProbe = func() bool {
		calls++
		if calls == 2 {
			// Commit after the full snapshot, before activation takes q.mu.
			if err := q.update(func() error { return nil }); err != nil {
				t.Fatal(err)
			}
		}
		return true
	}
	if _, err := q.StorageAdmissionMigration(context.Background(), "activate"); !errors.Is(err, ErrStale) || q.s.StorageAdmissionActive {
		t.Fatal("activation ignored a commit after its full audit", err)
	}
}
