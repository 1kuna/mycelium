package myce

import (
	"testing"
	"time"
)

// Replay the incident shape through real admission/accounting. Previous large
// service creates debt; repeated tiny jobs must not suppress a waiting large
// project for a fleet-specific number of hours. Rescale the entire workload to
// prove that the convergence decision follows work, not the wall-time unit.
func TestAdaptiveFairnessLargeWaitConverges(t *testing.T) {
	// Keep a large/small service imbalance at all three time scales without
	// replaying dozens of identical submit/checkin/finish transactions.
	const largeQuanta = 16
	var firstTurn int
	for _, quantum := range []time.Duration{time.Millisecond, time.Second, 3 * time.Hour} {
		t.Run(quantum.String(), func(t *testing.T) {
			q, clock := fixture(t)
			q.lossTimeouts = [3]time.Duration{365 * 24 * time.Hour, 365 * 24 * time.Hour, 365 * 24 * time.Hour}
			m := machine("worker", "fake")
			submit(t, q, projectSpec("large"))
			run := check(t, q, m, nil).Job
			clock.Advance(largeQuanta * quantum)
			finish(t, q, run, true)
			large := submit(t, q, projectSpec("large"))
			served := 0
			for ; served < largeQuanta; served++ {
				small := submit(t, q, projectSpec("small"))
				run = check(t, q, m, nil).Job
				if run == nil {
					t.Fatal("idle with fitting demand")
				}
				if run.ID == large.ID {
					break
				}
				if run.ID != small.ID {
					t.Fatal("unexpected assignment", run)
				}
				clock.Advance(quantum)
				finish(t, q, run, true)
			}
			if served == 0 || served == largeQuanta {
				t.Fatalf("large project did not recover a fair turn: tiny jobs served=%d", served)
			}
			if firstTurn == 0 {
				firstTurn = served
			} else if served != firstTurn {
				t.Fatalf("time scale changed convergence: got %d want %d", served, firstTurn)
			}
			t.Logf("large job recovered after %d tiny services; workload quantum %s", served, quantum)
			// Once large work runs, do not preempt it merely to rebalance.
			clock.Advance(8 * quantum)
			check(t, q, m, run)
			if got := job(t, q, large.ID); got.State != "running" {
				t.Fatal("same-tier fairness stopped active work", got.State)
			}
		})
	}
}
