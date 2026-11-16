package myce

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// A local availability pause can race assignment delivery. Recovering that
// signal must not inherit the NAS-discovery retry interval.
func TestCheckpointPreparationRetryDoesNotDelayLocalAvailability(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := recoveryWorker(t)
		clock := &workerFakeClock{now: time.Unix(100, 0)}
		w.Clock, w.control = clock, &processControl{}
		w.CPUIsolationProbe = func(context.Context) error { return nil }
		w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
		w.availability = newAvailabilityMonitor(clock, false, false, true)
		defer w.availability.cancel()
		w.availability.sample(OwnerSignals{Power: "battery"})
		root, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer root.close()
		j := recoveryJob(t, []string{"true"})
		j.Spec.Needs.Platform.Any = false
		j.Spec.Needs.Platform.Required = []string{"cpu"}
		store, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		prepared := make(chan struct{}, 1)
		w.checkpointPrepare = func(Job) (string, error) {
			prepared <- struct{}{}
			return "", errors.New("fixture stops before opening any checkpoint")
		}
		ctx, cancel := context.WithCancel(context.Background())
		a := &attemptSupervisor{store: store, job: j, worker: w, connected: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go a.run(ctx)
		defer func() { cancel(); <-a.done }()
		synctest.Wait()
		if r, _, finished, err := a.snapshot(); r.ID != j.ID || r.Started || finished || err != nil {
			t.Fatal("actor did not observe the local availability pause", r, finished, err)
		}
		select {
		case <-prepared:
			t.Fatal("preparation ran while local availability was withdrawn")
		default:
		}
		w.availability.sample(OwnerSignals{Power: "desktop"})
		// Advance the injected scheduler clock with virtual actor time, including
		// any local input-staging completion before checkpoint preparation.
		for range 1000 {
			clock.Advance(w.Interval)
			time.Sleep(w.Interval)
			synctest.Wait()
			select {
			case <-prepared:
				return
			default:
			}
		}
		cancel()
		<-a.done
		r, _, finished, actorErr := a.snapshot()
		t.Fatal("recovered local availability did not reach checkpoint preparation", r, finished, actorErr, store.state.Result)
	})
}

func TestNewAttemptBlockedCheckpointPreparationIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := recoveryWorker(t)
		root, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer root.close()
		j := recoveryJob(t, []string{"true"})
		store, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		release, joined := make(chan struct{}), make(chan struct{})
		w.checkpointPrepare = func(Job) (string, error) {
			defer close(joined)
			<-release
			return "", errors.New("NAS unreachable")
		}
		started := time.Now()
		p, err := startAfterFixtureInputs(t, w, store, j)
		elapsed := time.Since(started)
		close(release)
		<-joined
		if p != nil || !errors.Is(err, errWorkerCheckpoint) {
			t.Fatalf("blocked NAS preparation must defer attempt startup: process=%v err=%v", p, err)
		}
		if elapsed != checkpointPreparationBudget {
			t.Fatalf("new attempt checkpoint preparation waited %s; want budget %s", elapsed, checkpointPreparationBudget)
		}
	})
}
