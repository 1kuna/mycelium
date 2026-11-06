package myce

import (
	"testing"
	"time"
)

func TestCacheUploadCannotPromisePendingWorkerBlocks(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	worker, _ := syntheticCacheBudget(t, clock, 200<<30, 100<<30)
	queue := newCacheBudgets()
	queue.hostRoot, queue.probe = worker.hostRoot, worker.probe
	store := &FileStore{root: t.TempDir(), clock: clock, budgets: queue}
	held, err := worker.reserve("worker", 80<<30, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.reserveUpload("pending", 80<<30, false); err == nil {
		t.Fatal("queue promised the worker's blocks")
	}
	held.release()
	if err = store.reserveUpload("pending", 80<<30, false); err != nil {
		t.Fatal(err)
	}
	if _, err = worker.reserve("worker", 80<<30, clock); err == nil {
		t.Fatal("worker promised the queue's blocks")
	}
	store.closeHostClaims()
}
func TestCacheUploadRestartRetainsAcceptedCommitment(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(t, clock, 200<<30, 10<<30)
	store := &FileStore{root: t.TempDir(), clock: clock, budgets: b}
	// Recovery restores an already accepted length even if unrelated activity
	// consumed its room while the queue was down. It grants no new admission.
	if err := store.reserveUpload("interrupted", 80<<30, true); err != nil {
		t.Fatal(err)
	}
	defer store.closeHostClaims()
	if _, err := b.reserve("worker", 1, clock); err == nil {
		t.Fatal("cache ignored restored upload custody")
	}
}
