package myce

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openShardQueue(t *testing.T, db, root string, c Clock) *Queue {
	t.Helper()
	q, err := Open(db, root, c, WithCheckpointShards())
	if err != nil {
		t.Fatal(err)
	}
	if done := q.kickCheckpointShards(); done != nil {
		<-done
	}
	return q
}

func writeSave(t *testing.T, dir string, generation string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"myce_checkpoint":{"generation":`+generation+`},"value":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
}

// #894: new jobs shard; jobs recorded with flat folders keep them and resume.
func TestCheckpointShardsKeepLegacyFlatJobsResumable(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, c)
	if err != nil {
		t.Fatal(err)
	}
	legacy := submit(t, q, spec("experiment"))
	if legacy.Checkpoint != filepath.Join(root, legacy.ID) {
		t.Fatalf("flat layout changed: %s", legacy.Checkpoint)
	}
	writeSave(t, filepath.Join(legacy.Checkpoint, "attempt-1"), "3")
	q.Close()

	q = openShardQueue(t, db, root, c)
	defer q.Close()
	for _, shard := range []string{"00", "7f", "ff"} {
		if info, err := os.Stat(filepath.Join(root, shard)); err != nil || !info.IsDir() {
			t.Fatalf("shard %s not prepared: %v", shard, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, checkpointShardMarker)); err != nil {
		t.Fatal("marker missing after preparation")
	}
	fresh := submit(t, q, spec("experiment"))
	if fresh.Checkpoint != filepath.Join(root, fresh.ID[:2], fresh.ID) {
		t.Fatalf("new job not sharded: %s", fresh.Checkpoint)
	}
	q.mu.Lock()
	kept := q.s.Jobs[legacy.ID].Checkpoint
	q.mu.Unlock()
	if kept != filepath.Join(root, legacy.ID) {
		t.Fatalf("legacy job moved: %s", kept)
	}
	// Predecessor workers only require the folder's parent to exist already.
	if info, err := os.Stat(filepath.Dir(fresh.Checkpoint)); err != nil || !info.IsDir() {
		t.Fatal("sharded parent missing for predecessor workers")
	}

	resumed := Job{ID: legacy.ID, Attempt: 2, Checkpoint: kept}
	dest, err := prepareCheckpoint(resumed)
	if err != nil {
		t.Fatal(err)
	}
	if progress, err := checkpointProgress(dest); err != nil || progress != 3 {
		t.Fatalf("legacy flat checkpoint did not resume: %d %v", progress, err)
	}

	// A removed shard is recreated beneath an existing root; a missing root is
	// still refused rather than created.
	if err := os.Remove(filepath.Dir(fresh.Checkpoint)); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCheckpoint(Job{ID: fresh.ID, Attempt: 1, Checkpoint: fresh.Checkpoint}); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "unmounted")
	if _, err := prepareCheckpoint(Job{ID: fresh.ID, Attempt: 1, Checkpoint: filepath.Join(missing, fresh.ID[:2], fresh.ID)}); err == nil {
		t.Fatal("missing root accepted")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing root was created")
	}
}

func TestCheckpointShardPreparationRetriesWithoutCreatingRoot(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	root := filepath.Join(t.TempDir(), "unmounted")
	q := openShardQueue(t, filepath.Join(t.TempDir(), "queue.db"), root, c)
	defer q.Close()
	if q.shards.ready.Load() {
		t.Fatal("missing root reported ready")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("preparation created the root")
	}
	if j := submit(t, q, spec("experiment")); j.Checkpoint != filepath.Join(root, j.ID) {
		t.Fatal("unprepared root assigned a shard")
	}
	if q.kickCheckpointShards() != nil {
		t.Fatal("retried before the backoff")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	c.Advance(checkpointShardRetry)
	if done := q.kickCheckpointShards(); done == nil {
		t.Fatal("did not retry after the backoff")
	} else {
		<-done
	}
	if !q.shards.ready.Load() {
		t.Fatal("not ready after the root appeared")
	}
}
