package myce

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDevWaitReadyRequiresEveryCurrentWorker(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		first, second := machine("fake-1", "fake"), machine("fake-2", "fake")
		check(t, q, first, nil)
		check(t, q, second, nil)
		d := &Dev{Queue: q, workers: map[string]time.Time{first.ID: clock.Now(), second.ID: clock.Now()}}
		assertPending := func() {
			t.Helper()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if err := d.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("accepted stale or partial inventory: %v", err)
			}
		}
		assertPending() // Previous runtime's accepted inventory is insufficient.
		clock.Advance(time.Second)
		check(t, q, first, nil)
		assertPending() // One worker cannot stand in for the other.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- d.WaitReady(ctx) }()
		check(t, q, second, nil)
		if err := <-done; err != nil {
			t.Fatal("committed check-in did not release readiness", err)
		}

	})

	t.Run("DevWaitReadyStopsOnCancellationOrFailure", func(t *testing.T) {
		q, _ := fixture(t)
		stopped := make(chan struct{})
		d := &Dev{Queue: q, workers: map[string]time.Time{"fake-1": {}}, errors: make(chan error, 1), stopped: stopped}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := d.WaitReady(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal("caller cancellation", err)
		}
		failure := errors.New("worker startup failed")
		d.errors <- failure
		if err := d.WaitReady(context.Background()); !errors.Is(err, failure) {
			t.Fatal("runtime error", err)
		}
		close(stopped)
		if err := d.WaitReady(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatal("runtime stopped", err)
		}

	})
}
