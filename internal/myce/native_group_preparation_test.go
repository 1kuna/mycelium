package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Pre-anchor preparation must remain reachable through the same controller as
// running members. Hold the real build lock throughout cancellation: releasing
// that lock is not allowed to make the test pass.
func TestNativeMinimalRootCancellationPreparing(t *testing.T) {
	for _, cause := range []string{"user-cancel", "priority-preemption", "startup-context"} {
		t.Run(cause, func(t *testing.T) {
			cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}}
			recipes := map[string]EnvironmentRecipe{}
			for _, name := range []string{"ready", "waiting"} {
				binary := filepath.Join(t.TempDir(), name)
				if err := os.WriteFile(binary, []byte("unused fixture binary"), 0700); err != nil {
					t.Fatal(err)
				}
				r, err := snapshotEnvironment(EnvironmentRecipe{Binary: binary}, "fake")
				if err != nil {
					t.Fatal(err)
				}
				recipes[name] = r
			}
			_, release, err := cache.Acquire(context.Background(), recipes["ready"])
			if err != nil {
				t.Fatal(err)
			}
			release()
			held, err := cache.lock(context.Background(), recipes["waiting"].Key+".build", syscall.LOCK_EX)
			if err != nil {
				t.Fatal(err)
			}
			useLocked := func(name string) bool {
				t.Helper()
				f, err := os.OpenFile(filepath.Join(cache.Root, "locks", recipes[name].Key+".use"), os.O_CREATE|os.O_RDWR, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer unlockEnvironment(f)
				err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
				if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
					return true
				}
				if err != nil {
					t.Fatal(err)
				}
				return false
			}
			control := &processControl{clock: RealClock{}}
			w := Worker{NativeMinimalRoot: true, Clock: RealClock{}, Machine: machine("prepare", "fake"), environmentCache: cache, control: control}
			j := Job{ID: "pre-anchor", Attempt: 1, Session: "prepare-session", Spec: spec("experiment"), SidecarEnvironmentRecipes: map[string]map[string]EnvironmentRecipe{}}
			j.Spec.Run.Sidecars = nil
			for _, name := range []string{"ready", "waiting"} {
				j.Spec.Run.Sidecars = append(j.Spec.Run.Sidecars, NativeSidecar{Name: name, Environment: name})
				j.SidecarEnvironmentRecipes[name] = map[string]EnvironmentRecipe{"fake": recipes[name]}
			}
			store := &workerStore{dir: t.TempDir(), state: workerState{Session: j.Session}}
			ctx, cancel := context.WithCancel(context.Background())
			type outcome struct {
				p   *process
				err error
			}
			result := make(chan outcome, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				p, err := w.startNativeGroup(ctx, store, j, "pre-anchor", nil, jobBaseEnvironment(), "")
				result <- outcome{p, err}
			}()
			t.Cleanup(func() {
				cancel()
				unlockEnvironment(held)
				select {
				case <-done:
				case <-time.After(testWaitTimeout):
					t.Error("preparation goroutine did not return")
				}
			})
			await(t, func() bool { return useLocked("waiting") })
			if !useLocked("ready") {
				t.Fatal("first environment not retained while acquiring second")
			}
			control.mu.Lock()
			p := control.processes[jobKey(j)]
			control.mu.Unlock()
			if p == nil {
				t.Fatal("pre-anchor preparation absent from process controller")
			}
			p.mu.Lock()
			anchor := p.native.supervisor
			p.mu.Unlock()
			if anchor != nil {
				t.Fatal("anchor started before sidecar environments were acquired")
			}
			if cause == "startup-context" {
				cancel()
			} else {
				event := WorkerEvent{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, UserCancel: cause == "user-cancel", deadline: w.Clock.Now().Add(time.Minute)}
				if cause == "priority-preemption" {
					event.PriorityStop = &PriorityPreemption{Cause: priorityPreemptionCause, Demand: "production", Victim: j.ID}
				}
				control.mu.Lock()
				control.pendingEvents = map[attemptKey]WorkerEvent{eventKey(event): event}
				control.apply()
				control.mu.Unlock()
			}
			select {
			case got := <-result:
				if got.p != nil || !errors.Is(got.err, context.Canceled) || !startFailureOutcome(Result{}, got.err).Interrupted {
					t.Fatalf("cancelled preparation did not return an interrupted refusal: %+v", got)
				}
			case <-time.After(testWaitTimeout):
				t.Fatal("cancellation waited for the held build lock")
			}
			control.mu.Lock()
			remaining := len(control.processes)
			control.mu.Unlock()
			if remaining != 0 || store.state.PID != 0 || store.state.LaunchPending || p.native.supervisor != nil {
				t.Fatal("cancelled pre-anchor preparation retained registration or launched a process")
			}
			for _, name := range []string{"ready", "waiting"} {
				if useLocked(name) {
					t.Errorf("cancelled preparation retained %s environment use lock", name)
				}
			}
			if cause == "priority-preemption" {
				if err := store.restorePriorityStop(); err != nil || store.state.PriorityStop == nil || store.state.PriorityStop.Demand != "production" {
					t.Fatal("pre-anchor priority intent was not durable", err)
				}
			}
		})
	}
}
