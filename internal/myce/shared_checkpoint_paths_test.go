package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Members mount the checkpoint share at different paths (#2146). A job
// submitted under one member is offered by the next with its folder under the
// shared root, and a worker on the next member's host runs it. Before the fix
// the offer escaped the root and the worker exited.
func TestSharedQueuePostgresCheckpointRootMountedElsewhere(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	ctx := t.Context()
	clock := proofClock()
	firstMount := filepath.Join(dir, "first-mount")
	if err := os.MkdirAll(filepath.Join(firstMount, "checkpoints"), 0700); err != nil {
		t.Fatal(err)
	}
	old, first := proofOpenSharedQueueRoots(t, a, dir, "old", filepath.Join(firstMount, "checkpoints"), filepath.Join(firstMount, "store"), true, clock)
	if err := a.renew(ctx, first, time.Minute, true); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-ec", `printf '{"myce_checkpoint":{"generation":1}}' > "$MYCE_CHECKPOINT_DIR/save.json"`}
	j, err := old.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.renew(ctx, first, 0, false); err != nil {
		t.Fatal(err)
	}
	// The successor's host mounts the same share somewhere else.
	secondMount := filepath.Join(dir, "second-mount")
	if err := os.Rename(firstMount, secondMount); err != nil {
		t.Fatal(err)
	}
	checkpoints := filepath.Join(secondMount, "checkpoints")
	next, second := proofOpenSharedQueueRoots(t, a, dir, "next", checkpoints, filepath.Join(secondMount, "store"), true, clock)
	if err := a.renew(ctx, second, time.Minute, true); err != nil {
		t.Fatal(err)
	}
	fakeCheckpointAvailability(t, next, true)
	api := httptest.NewServer(API{Queue: next, Token: "test"}.Handler())
	defer api.Close()

	w := recoveryWorker(t)
	w.CheckpointRoot, w.Client = checkpoints, Client{URL: api.URL, Token: "test"}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()
	state := func() string {
		next.mu.RLock()
		defer next.mu.RUnlock()
		return next.s.Jobs[j.ID].State
	}
	var runErr error
	exited := false
	await(t, func() bool {
		select {
		case runErr = <-done:
			exited = true
			return true
		default:
			return state() == "succeeded"
		}
	})
	cancel()
	if exited {
		t.Fatalf("worker exited on the successor's offer: %v", runErr)
	}
	if runErr = <-done; runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatal(runErr)
	}
	if generation, err := checkpointProgress(filepath.Join(checkpoints, j.ID, "attempt-1")); err != nil || generation != 1 {
		t.Fatalf("checkpoint under the shared root: generation=%d err=%v", generation, err)
	}
}

// A folder another host recorded keeps its layout under this queue's root;
// folders already under the root, and paths in no known layout, are unchanged.
func TestRebaseCheckpointRecordedOnAnotherMount(t *testing.T) {
	root := filepath.Join(t.TempDir(), "here")
	const id = "ab12cd34"
	for _, c := range []struct{ name, recorded, want string }{
		{"flat", "/another-mount/checkpoints/" + id, filepath.Join(root, id)},
		{"sharded", "/another-mount/checkpoints/ab/" + id, filepath.Join(root, "ab", id)},
		{"local flat", filepath.Join(root, id), filepath.Join(root, id)},
		{"local sharded", filepath.Join(root, "ab", id), filepath.Join(root, "ab", id)},
		{"unknown layout", "/another-mount/elsewhere", "/another-mount/elsewhere"},
		{"unset", "", ""},
	} {
		j := &Job{ID: id, Checkpoint: c.recorded}
		rebaseCheckpoint(root, j)
		if j.Checkpoint != c.want {
			t.Errorf("%s: %q, want %q", c.name, j.Checkpoint, c.want)
		}
	}
}

// A checkpoint folder the worker cannot resolve under its root refuses that
// one attempt with a reason; the worker keeps running (#2146).
func TestWorkerRefusesCheckpointOutsideRoot(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	// Recorded in no known layout, so the queue offers it as is.
	if err := q.update(func() error {
		q.touchJob(j.ID).Checkpoint = "/another-mount/elsewhere"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	w := recoveryWorker(t)
	w.CheckpointRoot, w.Client = t.TempDir(), Client{URL: api.URL, Token: "test"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	refused := func() bool {
		q.mu.RLock()
		defer q.mu.RUnlock()
		record, err := json.Marshal(q.s.Jobs[j.ID])
		return err == nil && strings.Contains(string(record), checkpointOutsideRoot)
	}
	var runErr error
	exited := false
	await(t, func() bool {
		select {
		case runErr = <-done:
			exited = true
			return true
		default:
			return refused()
		}
	})
	cancel()
	if exited {
		t.Fatalf("worker exited: %v", runErr)
	}
	if runErr = <-done; runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatal(runErr)
	}
}
