package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestWorkerPreparationCancellationRetainsUnconfirmedHelper(t *testing.T) {
	ps, err := exec.LookPath("ps")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	denied := filepath.Join(bin, "deny-inspection")
	if err := os.WriteFile(filepath.Join(bin, "ps"), []byte("#!/bin/sh\nif [ -f \"$MYCE_TEST_PS_DENY\" ]; then exit 1; fi\nexec \"$MYCE_TEST_REAL_PS\" \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TEST_PS_DENY", denied)
	t.Setenv("MYCE_TEST_REAL_PS", ps)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	w := recoveryWorker(t)
	clock := &workerFakeClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
	w.Clock = clock
	w.environmentCache = &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}
	if err := w.environmentCache.init(); err != nil {
		t.Fatal(err)
	}
	recipe, err := snapshotEnvironment(EnvironmentRecipe{Build: []string{"sh", "-c", "touch '{root}/helper-ready'; while :; do sleep .05; done"}}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	j := recoveryJob(t, []string{"touch", filepath.Join(root.dir, "payload-started")})
	j.Session = w.Machine.Session
	j.EnvironmentRecipes = map[string]EnvironmentRecipe{"fake": recipe}
	store, err := newAttemptStore(root, j)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := startAfterFixtureInputsContext(t, ctx, w, store, j)
		done <- err
	}()
	await(t, func() bool {
		_, err := os.Stat(filepath.Join(w.environmentCache.Root, recipe.Key, "helper-ready"))
		return err == nil
	})
	if err := os.WriteFile(denied, nil, 0600); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, errWorkerReleasePending) {
			t.Fatalf("cancel released unconfirmed builder custody: %v", err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("builder did not respond to cancellation")
	}
	if store.state.Preparation == "" {
		t.Fatal("builder custody was not transferred to the attempt")
	}
	store.state.Result = &Result{Machine: w.Machine.ID, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, ReleasePending: true}
	if err := store.save(); err != nil {
		t.Fatal(err)
	}
	// Re-read the durable attempt as recovery would; unavailable inspection
	// cannot be replaced by a nil process pointer or an expired deadline.
	data, err := os.ReadFile(filepath.Join(store.dir, store.stateName))
	if err != nil {
		t.Fatal(err)
	}
	recovered := &workerStore{dir: store.dir, stateName: store.stateName}
	if err := decodeWorkerState(data, &recovered.state); err != nil {
		t.Fatal(err)
	}
	if _, err := w.recoverAttempt(recovered); err == nil || !recovered.state.Result.ReleasePending {
		t.Fatal("recovery released without process inspection", err)
	}
	if err := os.Remove(denied); err != nil {
		t.Fatal(err)
	}
	if p, err := w.recoverAttempt(recovered); err != nil || p != nil || recovered.state.Result.ReleasePending {
		t.Fatal("confirmed helper exit did not release", p, err)
	}
	if _, err := os.Stat(filepath.Join(root.dir, "payload-started")); !os.IsNotExist(err) {
		t.Fatal("cancelled helper launched payload", err)
	}
}

// Enumeration succeeding does not prove that a member disappearing from a
// per-PID inspection has exited. In particular, a scrubbed helper marker is
// not permission to release its reservation.
func TestWorkerPreparationUnidentifiedLiveMember(t *testing.T) {
	realPS, err := exec.LookPath("ps")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "ps"), []byte("#!/bin/sh\nif [ \"$1\" = eww ]; then exit 1; fi\nexec \"$MYCE_TEST_REAL_PS\" \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TEST_REAL_PS", realPS)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	cmd := exec.Command("sleep", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	clock := &workerFakeClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
	store := &workerStore{state: workerState{PID: cmd.Process.Pid, Identity: ID()}}
	if err := store.cleanupOrphanCustody(clock, true); err == nil {
		t.Fatal("released live helper after unavailable member inspection")
	}
	if err := os.Remove(filepath.Join(bin, "ps")); err != nil {
		t.Fatal(err)
	}
	if err := store.cleanupOrphanCustody(clock, true); err == nil {
		t.Fatal("released live helper with missing marker")
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal("unauthenticated helper was signalled", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if err := store.cleanupOrphanCustody(clock, true); err != nil {
		t.Fatal("empty group retained custody", strconv.Itoa(cmd.Process.Pid), err)
	}
}

func TestWorkerOrdinaryStagingCancellationAndLateResult(t *testing.T) {
	w := recoveryWorker(t)
	clock := &workerFakeClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
	w.Clock = clock
	w.JobFileRoot = t.TempDir()
	if err := os.WriteFile(filepath.Join(w.JobFileRoot, "input"), []byte("value"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	j := recoveryJob(t, []string{"true"})
	j.Spec.Inputs = map[string]string{"input": "input"}
	store, err := newAttemptStore(root, j)
	if err != nil {
		t.Fatal(err)
	}
	entered, unblock := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, ioProgressKey{}, func() { close(entered); <-unblock })
	done := make(chan error, 1)
	go func() { _, err := w.startContext(ctx, store, j); done <- err }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("ordinary staging ignored cancel", err)
	}
	next := j
	next.ID = ID()
	nextStore, err := newAttemptStore(root, next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.startContext(context.Background(), nextStore, next); !errors.Is(err, errWorkerCheckpoint) {
		t.Fatal("detached stage was duplicated", err)
	}
	store.extensionFiles.mu.Lock()
	pending := store.extensionFiles.pending
	store.extensionFiles.mu.Unlock()
	close(unblock)
	<-pending.done
	nextStore.extensionFiles.mu.Lock()
	nextPending := nextStore.extensionFiles.pending
	nextStore.extensionFiles.mu.Unlock()
	if nextPending != nil {
		<-nextPending.done
	}
	if !errors.Is(pending.err, context.Canceled) {
		t.Fatal("late stage returned success", pending.err)
	}
	if root.inputStaging.Load() {
		t.Fatal("completed cancelled stage retained worker slot")
	}
	if _, err := os.Stat(filepath.Join(root.dir, "job-files", j.ID, strconv.Itoa(j.Attempt))); !os.IsNotExist(err) {
		t.Fatal("late ordinary stage published", err)
	}
}
