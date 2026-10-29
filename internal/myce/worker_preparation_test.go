package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
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
