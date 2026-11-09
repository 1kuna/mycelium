package myce

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// These tests assert the pressure plan, rather than the current implementation.

func TestProductionDuringMissingNAS(t *testing.T) {
	q, clock := fixture(t)
	{
		// Keep the concurrent-outage scenario independent of production admission.
		q, clock := fixture(t)
		first := submit(t, q, spec("experiment"))
		running := check(t, q, machine("worker", "fake"), nil).Job
		if running == nil {
			t.Fatal("missing assignment")
		}
		q.checkpointProbe = func() bool { return false }
		clock.Advance(10 * time.Minute)
		snapshot, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(snapshot.Warning, "network") || !strings.Contains(snapshot.Warning, "NAS") {
			t.Fatalf("concurrent outage warning overwritten: %q", snapshot.Warning)
		}
		requireState(t, q, first.ID, "running")
		queued := submit(t, q, spec("experiment"))
		if check(t, q, machine("new-worker", "fake"), nil).Job != nil {
			t.Fatal("warning aggregation weakened NAS admission")
		}
		requireState(t, q, queued.ID, "queued")

	}
	s := spec("production")
	s.Resumable = false
	j := submit(t, q, s)
	fakeCheckpointAvailability(t, q, false)
	m := machine("synthetic-only", "fake")
	m.CheckpointUnavailable = true
	if reply := check(t, q, m, nil); reply.Job == nil {
		clock.Advance(31 * time.Second)
		t.Fatalf("production blocked by NAS: %s", job(t, q, j.ID).State)
	}
}

func TestProductionPreemptsDuringNASLoss(t *testing.T) {
	q, clock := fixture(t)
	exp := submit(t, q, spec("experiment"))
	m := machine("worker", "fake")
	running := check(t, q, m, nil).Job
	if running == nil {
		t.Fatal("experiment did not start")
	}
	fakeCheckpointAvailability(t, q, false)
	m.CheckpointUnavailable = true
	s := spec("production")
	s.Resumable = false
	prod := submit(t, q, s)
	reply := check(t, q, m, running)
	if !reply.Stop {
		t.Fatal("NAS outage prevents production preemption")
	}
	clock.Advance(10 * time.Second)
	if !check(t, q, m, running).Kill {
		t.Fatal("experiment not force-stopped at grace")
	}
	// Physical release is reported by the worker; never credit only a deadline.
	if err := q.Complete(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: exp.ID, Attempt: running.Attempt}, Interrupted: true}); err != nil && err != ErrStale {
		t.Fatal(err)
	}
	if reply := check(t, q, m, nil); reply.Job == nil || reply.Job.ID != prod.ID {
		t.Fatal("production not assigned after release")
	}
}

func TestBlockedNASDoesNotHoldQueue(t *testing.T) {
	q, clock := fixture(t)
	s := spec("production")
	s.Resumable = false
	submit(t, q, s)
	m := machine("worker", "fake")
	if first, err := q.Checkin(Checkin{Machine: m}); err != nil || first.Job != nil {
		t.Fatal("cold legacy response did not stay idle", first, err)
	}
	clock.Advance(time.Second) // The faulted check-in is the measured second send.
	// Drain a probe left pending by setup before replacing the fault seam.
	await(t, func() bool {
		_, err := q.nasProbe.run(q.root, func() (bool, error) { return true, nil })
		return err == nil
	})
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		close(release)
		// Join the uncancellable fake syscall before fixture cleanup.
		select {
		case <-entered:
			<-exited
		default:
		}
	}()
	var calls atomic.Int32
	// This fixture is about blocked storage, so retain the real bounded I/O
	// adapter explicitly rather than the synchronous availability fake.
	q.checkpointProbe = func() bool {
		// Check at the synchronous probe boundary, before its timeout can
		// release a wrongly held lock. No scheduler/timer race decides this.
		if !q.mu.TryLock() {
			t.Error("NAS probe called while queue lock is held")
		} else {
			q.mu.Unlock()
		}
		available, _ := q.nasProbe.run(q.root, func() (bool, error) {
			if calls.Add(1) == 1 {
				defer close(exited)
				close(entered)
			}
			<-release
			return false, nil
		})
		return available
	}
	done := make(chan Reply, 1)
	go func() { r, _ := q.Checkin(Checkin{Machine: m}); done <- r }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("filesystem fault did not enter")
	}
	select {
	case r := <-done:
		if r.Job == nil {
			t.Fatal("production blocked by hung NAS")
		}
	case <-time.After(q.nasProbe.timeout + 5*time.Second):
		t.Fatal("checkin hangs behind NAS syscall")
	}
	tick := make(chan error, 1)
	go func() { tick <- q.Tick() }()
	select {
	case err := <-tick:
		if err != nil {
			t.Fatal(err)
		}
	// Tick performs two bounded availability waits. This is only a hang
	// backstop; the direct lock assertion above detects the regression.
	case <-time.After(2*q.nasProbe.timeout + 5*time.Second):
		t.Fatal("NAS holds queue loop")
	}
	if calls.Load() != 1 {
		t.Fatal("hung probes accumulate")
	}
}

func TestReconciliationUsesDurableGeneration(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("old", "fake")
	old := check(t, q, m, nil).Job
	clock.Advance(LossAfter)
	check(t, q, observer(), nil)
	n := machine("current", "fake")
	current := check(t, q, n, nil).Job
	dir := filepath.Join(q.root, current.Checkpoint, fmt.Sprintf("attempt-%d", current.Attempt))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "checkpoint.json")
	if err := os.WriteFile(path, []byte(`{"myce_checkpoint":{"generation":9},"value":9}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Unix(1, 0), time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	reply, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: old.ID, Attempt: old.Attempt, Progress: 2}})
	if err != nil || !reply.Kill {
		t.Fatalf("durable newer save lost: %+v %v", reply, err)
	}
}

func TestWorkerForceStopsBlockedSaveAtGrace(t *testing.T) {
	w := recoveryWorker(t)
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	w.Clock = clock
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"sh", "-c", `trap 'echo saving; cat "$MYCE_CHECKPOINT_DIR/save-block"; exit 0' TERM; echo ready; while :; do sleep 0.01; done`})
	w = workerWithPreparedCheckpoint(t, w, store, j)
	p, err := startAfterFixtureInputs(t, w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.stop(clock.Now(), 0, true)
	if err := syscall.Mkfifo(filepath.Join(attemptCheckpoint(j), "save-block"), 0600); err != nil {
		t.Fatal(err)
	}
	logged := func(text string) bool {
		data, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
		return strings.Contains(string(data), text)
	}
	await(t, func() bool { return logged("ready") })
	p.stop(clock.Now(), 10*time.Second, false)
	await(t, func() bool { return logged("saving") })
	clock.Advance(10 * time.Second)
	p.enforceGrace(clock.Now())
	select {
	case f := <-p.done:
		if !f.result.Interrupted || f.result.Success {
			t.Fatalf("blocked save not interrupted: %+v", f.result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("force-stop waits on checkpoint save")
	}
}

func TestFailedPreparationDoesNotPublishGeneration(t *testing.T) {
	j := recoveryJob(t, []string{"true"})
	j.Attempt = 2
	source := filepath.Join(j.Checkpoint, "attempt-1")
	if err := os.MkdirAll(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "a-checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":9},"value":9}`), 0600); err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(source, "z-payload")
	if err := syscall.Mkfifo(blocked, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCheckpoint(j); err == nil {
		t.Fatal("unsupported payload should fail preparation")
	}
	if _, err := os.Stat(attemptCheckpoint(j)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed preparation published an incomplete attempt: %v", err)
	}
	if err := os.Rename(blocked, blocked+".tmp"); err != nil {
		t.Fatal(err)
	}
	dest, err := prepareCheckpoint(j)
	if err != nil {
		t.Fatal(err)
	}
	if generation, err := checkpointProgress(dest); err != nil || generation != 9 {
		t.Fatalf("retry lost the durable checkpoint: %d %v", generation, err)
	}
	saved := filepath.Join(dest, "a-checkpoint.json")
	if err := os.WriteFile(saved, []byte(`{"myce_checkpoint":{"generation":10},"value":10}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCheckpoint(j); err != nil {
		t.Fatal(err)
	}
	if generation, err := checkpointProgress(dest); err != nil || generation != 10 {
		t.Fatalf("prepared retry overwrote its own save: %d %v", generation, err)
	}
}

func TestPreparationRejectsConcurrentPublication(t *testing.T) {
	j := recoveryJob(t, []string{"true"})
	j.Attempt = 2
	source := filepath.Join(j.Checkpoint, "attempt-1")
	if err := os.MkdirAll(source, 0700); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(source, "checkpoint.json")
	if err := os.WriteFile(manifest, []byte(`{"myce_checkpoint":{"generation":9},"value":9}`), 0600); err != nil {
		t.Fatal(err)
	}
	copyFile := func(dst io.Writer, src io.Reader) (int64, error) {
		// Publish after WalkDir enumerates files; the new payload is outside that snapshot.
		if err := os.WriteFile(filepath.Join(source, "payload"), []byte("new durable payload"), 0600); err != nil {
			return 0, err
		}
		if err := os.WriteFile(manifest+".tmp", []byte(`{"myce_checkpoint":{"generation":10},"value":{"payload":"payload"}}`), 0600); err != nil {
			return 0, err
		}
		if err := os.Rename(manifest+".tmp", manifest); err != nil {
			return 0, err
		}
		current, err := os.Open(manifest)
		if err != nil {
			return 0, err
		}
		defer current.Close()
		return io.Copy(dst, current)
	}
	if _, err := prepareCheckpointCopy(j, copyFile); err == nil || !strings.Contains(err.Error(), "changed during preparation") {
		t.Fatalf("accepted a manifest without its concurrent payload: %v", err)
	}
	if _, err := os.Stat(attemptCheckpoint(j)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published concurrent partial snapshot: %v", err)
	}
	dest, err := prepareCheckpoint(j)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(dest, "payload")); err != nil || string(data) != "new durable payload" {
		t.Fatalf("retry omitted the durable payload: %s %v", data, err)
	}
}
