package myce

import (
	"context"
	"github.com/tus/tusd/v2/pkg/filestore"
	tus "github.com/tus/tusd/v2/pkg/handler"
	"os"
	"path/filepath"
	"strings"
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
	if err = store.reserveUpload("pending", 80<<30); err == nil {
		t.Fatal("queue promised the worker's blocks")
	}
	held.release()
	if err = store.reserveUpload("pending", 80<<30); err != nil {
		t.Fatal(err)
	}
	if _, err = worker.reserve("worker", 80<<30, clock); err == nil {
		t.Fatal("worker promised the queue's blocks")
	}
	store.closeHostClaims()
}
func TestCacheUploadRestartRetainsAcceptedCommitment(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, disk := syntheticCacheBudget(t, clock, 200<<30, 100<<30)
	root := t.TempDir()
	store := &FileStore{root: root, clock: clock, budgets: b}
	if err := store.reserveUpload("interrupted", 80<<30); err != nil {
		t.Fatal(err)
	}
	store.closeHostClaims() // Queue exit relinquishes handles, never accepted custody.
	disk.FreeBytes = 10 << 30
	if _, err := b.reserve("worker", 1, clock); err == nil {
		t.Fatal("queue downtime released accepted upload custody")
	}
	restarted := &FileStore{root: root, clock: clock, budgets: b}
	if err := restarted.reserveUpload("interrupted", 80<<30); err != nil {
		t.Fatal("could not reacquire the existing durable promise", err)
	}
	restarted.releaseUpload("interrupted")
	next, err := b.reserve("worker", 1, clock)
	if err != nil {
		t.Fatal("authorized completion/reclamation did not release custody", err)
	}
	next.release()
	if err := restarted.reserveUpload("unaccounted", 80<<30); err == nil {
		t.Fatal("restart invented an unaccounted promise beyond capacity")
	}
}

func TestCacheUploadRestoreDoesNotDeduplicateIndependentPartial(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(t, clock, 200<<30, 100<<30)
	root := t.TempDir()
	for _, dir := range []string{"uploads", "blobs"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	hash := strings.Repeat("a", 64)
	store := &FileStore{root: root, clock: clock, budgets: b}
	if err := store.reserveUpload(hash, 80<<30); err != nil {
		t.Fatal(err)
	}
	_, err := filestore.New(filepath.Join(root, "uploads")).NewUpload(context.Background(), tus.FileInfo{ID: hash, Size: 80 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.blob(FileRef{SHA256: hash}), []byte("independent publisher"), 0600); err != nil {
		t.Fatal(err)
	}
	store.closeHostClaims()
	restarted := &FileStore{root: root, clock: clock, budgets: b}
	if err := restarted.restoreUploadClaims(); err != nil {
		t.Fatal(err)
	}
	if restarted.hostPending[hash] == nil {
		t.Fatal("blob deduplication hid a separate resumable upload")
	}
	restarted.releaseUpload(hash)
}
