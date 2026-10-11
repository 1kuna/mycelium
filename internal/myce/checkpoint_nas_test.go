package myce

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// These tests assert the pressure plan, rather than the current implementation.
func TestVersoPressureCheckpointSkew(t *testing.T) {
	for _, offset := range []int{-120, -30, -5, 5, 30, 120} {
		t.Run(strconv.Itoa(offset), func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "job")
			now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
			for attempt, unit := range map[int]string{1: "2", 2: "9"} {
				dir := filepath.Join(root, "attempt-"+strconv.Itoa(attempt))
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				p := filepath.Join(dir, "checkpoint.json")
				if err := os.WriteFile(p, []byte(unit), 0600); err != nil {
					t.Fatal(err)
				}
				stamp := now
				if offset > 0 && attempt == 1 || offset < 0 && attempt == 2 {
					stamp = now.Add(time.Duration(offset) * time.Second)
				}
				if err := os.Chtimes(p, stamp, stamp); err != nil {
					t.Fatal(err)
				}
			}
			dest, err := prepareCheckpoint(Job{ID: "synthetic-skew", Attempt: 3, Checkpoint: root})
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(dest, "checkpoint.json"))
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != "9" {
				t.Fatalf("completed progress regressed: resumed=%s want=9 skew_seconds=%d", data, offset)
			}
		})
	}
}

func TestProductionDuringMissingNAS(t *testing.T) {
	q, clock := fixture(t)
	s := spec("production")
	s.Resumable = false
	j := submit(t, q, s)
	root, away := q.root, q.root+"-unavailable"
	if err := os.Rename(root, away); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(away, root)
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
	away := q.root + "-away"
	if err := os.Rename(q.root, away); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(away, q.root)
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

func TestWorkerProductionWithoutNAS(t *testing.T) {
	w := recoveryWorker(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"true"})
	j.Spec.Priority = "production"
	j.Spec.Resumable = false
	j.Checkpoint = filepath.Join(t.TempDir(), "missing", "job")
	p, err := w.start(store, j)
	if err != nil {
		t.Fatalf("production requires NAS: %v", err)
	}
	f := <-p.done
	if !f.result.Success {
		t.Fatalf("production failed: %+v", f)
	}
}

func TestBlockedNASDoesNotHoldQueue(t *testing.T) {
	q, _ := fixture(t)
	s := spec("production")
	s.Resumable = false
	submit(t, q, s)
	// Drain a probe left pending by setup before replacing the fault seam.
	await(t, func() bool {
		_, err := q.nasProbe.run(q.root, func() (bool, error) { return true, nil })
		return err == nil
	})
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	q.checkpointProbe = func() bool {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return false
	}
	done := make(chan Reply, 1)
	go func() { r, _ := q.Checkin(Checkin{Machine: machine("worker", "fake")}); done <- r }()
	select {
	case <-entered:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("filesystem fault did not enter")
	}
	select {
	case r := <-done:
		if r.Job == nil {
			t.Fatal("production blocked by hung NAS")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("checkin hangs behind NAS syscall")
	}
	tick := make(chan error, 1)
	go func() { tick <- q.Tick() }()
	select {
	case err := <-tick:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("NAS holds queue loop")
	}
	if calls.Load() != 1 {
		t.Fatal("hung probes accumulate")
	}
}

func TestBlockedCheckpointPreparationIsBounded(t *testing.T) {
	w := recoveryWorker(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	w.checkpointPrepare = func(j Job) (string, error) { close(entered); <-release; return "", errors.New("NAS unreachable") }
	done := make(chan error, 1)
	go func() { _, err := w.start(store, recoveryJob(t, []string{"true"})); done <- err }()
	select {
	case <-entered:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("filesystem fault did not enter")
	}
	select {
	case err := <-done:
		if !errors.Is(err, errWorkerCheckpoint) {
			t.Fatalf("%v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("checkpoint preparation hangs start")
	}
}

func TestCheckpointGenerationOverridesAttemptAndMtime(t *testing.T) {
	for _, offset := range []int{-120, -30, -5, 5, 30, 120} {
		t.Run(strconv.Itoa(offset), func(t *testing.T) {
			j := recoveryJob(t, []string{"true"})
			j.Attempt = 3
			clock := &fakeClock{now: time.Unix(1000, 0)}
			for attempt, generation := range map[int]int{1: 9, 2: 2} {
				dir := filepath.Join(j.Checkpoint, fmt.Sprintf("attempt-%d", attempt))
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(dir, "checkpoint.json")
				data := fmt.Sprintf(`{"myce_checkpoint":{"generation":%d},"value":{"unit":%d}}`, generation, generation)
				if err := os.WriteFile(path, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
				stamp := clock.Now().Add(time.Duration(offset*attempt) * time.Second)
				if err := os.Chtimes(path, stamp, stamp); err != nil {
					t.Fatal(err)
				}
			}
			dest, err := prepareCheckpoint(j)
			if err != nil {
				t.Fatal(err)
			}
			generation, err := checkpointProgress(dest)
			if err != nil || generation != 9 {
				t.Fatalf("generation %d: %v", generation, err)
			}
		})
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
	p, err := w.start(store, j)
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

func TestLegacyCachedTimestampCannotOverrideGeneration(t *testing.T) {
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
	if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":2},"value":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	// A queue restored from the old protocol may contain a machine-clock mtime.
	q.s.Jobs[current.ID].Progress = clock.Now().UnixNano()
	reply, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: old.ID, Attempt: old.Attempt, Progress: 9}})
	if err != nil || reply.Kill || reply.Hold || job(t, q, old.ID).Machine != m.ID {
		t.Fatalf("cached timestamp overruled generation: %+v %v", reply, err)
	}
}
