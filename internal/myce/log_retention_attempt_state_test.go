package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #216: cleared attempts retain diagnosis for the log window; held and replay
// pending custody, the machine index, and Docker cleanup retain their evidence.
func TestWorkerRetentionPrunesClearedAttemptStateAfterLogWindow(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)}
	ids := map[string]string{}
	for _, kind := range []string{"cleared", "live", "replay", "indexed", "docker", "recent"} {
		id := ID()
		ids[kind] = id
		state := workerState{Identity: id}
		if kind == "live" || kind == "replay" {
			state.Job = &Job{ID: kind, Attempt: 1}
		}
		if kind == "replay" {
			state.Result = &Result{Success: true}
		}
		if err := saveWorkerJSON(dir, "state-attempt-"+kind+".json", state); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{id + ".result.json", id + ".outcome.json", id + ".cid", id + "-helper.cid", id + ".docker-evidence.json", id + ".docker-events.json", "attempt-" + id + ".json", "native-" + id + ".json"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := saveWorkerJSON(dir, "state.json", workerState{Attempts: []string{"state-attempt-indexed.json"}}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ids["docker"]+".docker.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if err := os.Chtimes(filepath.Join(dir, e.Name()), clock.Now(), clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	pass := func() {
		t.Helper()
		if err := workerRetention(dir, clock.Now(), DefaultLogRetention); err != nil {
			t.Fatal(err)
		}
	}
	pass()
	clock.Advance(DefaultLogRetention - time.Second)
	pass()
	assertPresent := func(name string, want bool) {
		t.Helper()
		_, err := os.Lstat(filepath.Join(dir, name))
		if want && err != nil || !want && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s present=%t: %v", name, want, err)
		}
	}
	assertPresent("state-attempt-cleared.json", true)
	clock.Advance(2 * time.Second)
	// Freshly cleared custody gets a full window even when its command was old.
	if err := os.Chtimes(filepath.Join(dir, "state-attempt-recent.json"), clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	pass()
	for kind, id := range ids {
		keep := kind != "cleared"
		assertPresent("state-attempt-"+kind+".json", keep)
		for _, name := range []string{id + ".result.json", id + ".outcome.json", id + ".cid", id + "-helper.cid", id + ".docker-evidence.json", id + ".docker-events.json", "attempt-" + id + ".json", "native-" + id + ".json"} {
			assertPresent(name, keep)
		}
	}
	// Once the existing Docker collector removes custody, old private evidence goes.
	if err := os.Remove(filepath.Join(dir, ids["docker"]+".docker.json")); err != nil {
		t.Fatal(err)
	}
	pass()
	assertPresent(ids["docker"]+".docker-evidence.json", false)
	assertPresent("state-attempt-docker.json", false)
}

// Review regression: refused Docker jobs have no execution identity, and their
// launch diagnostics use the job/attempt name rather than an execution identity.
func TestWorkerRetentionPrunesAcknowledgedLaunchRefusals(t *testing.T) {
	for _, docker := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "docker"}[docker], func(t *testing.T) {
			dir := t.TempDir()
			clock := &fakeClock{now: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)}
			root := &workerStore{dir: dir}
			job := Job{ID: "refused", Attempt: 1}
			job.Spec.Run.Docker = docker
			store, err := newAttemptStore(root, job)
			if err != nil {
				t.Fatal(err)
			}
			cause := store.startFailure(job, launchRefused, errors.New("refused before launch"))
			result := startFailureOutcome(Result{}, cause)
			store.state.Result = &result
			if err := store.save(); err != nil {
				t.Fatal(err)
			}
			outcome := store.state.LaunchOutcome
			if outcome == "" || docker && store.state.Identity != "" {
				t.Fatal("bad refusal fixture")
			}
			for _, name := range []string{store.stateName, outcome} {
				if err := os.Chtimes(filepath.Join(dir, name), clock.Now(), clock.Now()); err != nil {
					t.Fatal(err)
				}
			}
			pass := func() {
				t.Helper()
				if err := workerRetention(dir, clock.Now(), DefaultLogRetention); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(2 * DefaultLogRetention)
			pass() // Result replay remains pending regardless of diagnostic age.
			for _, name := range []string{store.stateName, outcome} {
				if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
					t.Fatal("replay evidence removed", err)
				}
			}
			store.state.Job, store.state.Result = nil, nil
			if err := store.save(); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(filepath.Join(dir, store.stateName), clock.Now(), clock.Now()); err != nil {
				t.Fatal(err)
			}
			pass() // Acknowledgement starts the retention window, not the refusal date.
			if _, err := os.Stat(filepath.Join(dir, outcome)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(DefaultLogRetention)
			pass()
			for _, name := range []string{store.stateName, outcome} {
				if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("expired refusal %s retained: %v", name, err)
				}
			}
		})
	}
}

func TestWorkerRetentionNativeStartFailureAcknowledgement(t *testing.T) {
	w := recoveryWorker(t)
	clock := &fakeClock{now: time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)}
	w.Clock = clock
	w.control = &processControl{}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	job := recoveryJob(t, []string{"sh", "-c", "exit 0"})
	job.Spec.Resumable = false
	job.Spec.Run.Workdir = "{work}/not-a-directory"
	store, err := newAttemptStore(root, job)
	if err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(store.dir, "job-files", job.ID, "1")
	if err := os.MkdirAll(work, 0700); err != nil {
		t.Fatal(err)
	}
	// Resolve inside managed work, then fail the OS chdir after launch intent.
	if err := os.WriteFile(filepath.Join(work, "not-a-directory"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	p, cause := w.start(store, job)
	if p != nil || cause == nil || !store.state.LaunchPending {
		t.Fatalf("did not reach native cmd.Start failure: p=%v error=%v state=%+v", p, cause, store.state)
	}
	result := startFailureOutcome(Result{}, cause)
	store.state.Result = &result
	if err := store.save(); err != nil {
		t.Fatal(err)
	}
	actor := &attemptSupervisor{store: store, job: job, worker: w, connected: true, done: make(chan struct{}), wake: make(chan struct{}, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	actor.run(ctx)
	_, _, finished, err := actor.snapshot()
	if err != nil || !finished || store.state.Job != nil || store.state.Result != nil || store.state.LaunchPending || store.state.PID != 0 {
		t.Fatalf("acknowledged failure kept stale custody: finished=%t error=%v state=%+v", finished, err, store.state)
	}
	if err := os.Chtimes(filepath.Join(store.dir, store.stateName), clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(store.dir, store.state.LaunchOutcome), clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultLogRetention)
	if err := workerRetention(store.dir, clock.Now(), DefaultLogRetention); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{store.stateName, store.state.LaunchOutcome} {
		if _, err := os.Stat(filepath.Join(store.dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("acknowledged native failure retained %s: %v", name, err)
		}
	}
}
