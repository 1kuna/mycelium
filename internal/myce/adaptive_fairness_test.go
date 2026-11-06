package myce

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// Replay the incident shape through real admission/accounting. Previous large
// service creates debt; repeated tiny jobs must not suppress a waiting large
// project for a fleet-specific number of hours. Rescale the entire workload to
// prove that the convergence decision follows work, not the wall-time unit.
func TestAdaptiveFairnessLargeWaitConverges(t *testing.T) {
	var firstTurn int
	for _, quantum := range []time.Duration{time.Millisecond, time.Second, 3 * time.Hour} {
		t.Run(quantum.String(), func(t *testing.T) {
			q, clock := fixture(t)
			q.lossTimeouts = [3]time.Duration{365 * 24 * time.Hour, 365 * 24 * time.Hour, 365 * 24 * time.Hour}
			m := machine("worker", "fake")
			submit(t, q, projectSpec("large"))
			run := check(t, q, m, nil).Job
			clock.Advance(64 * quantum)
			finish(t, q, run, true)
			large := submit(t, q, projectSpec("large"))
			served := 0
			for ; served < 64; served++ {
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
			if served == 0 || served == 64 {
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

func TestAdaptiveFairnessDemandCyclesAndRestart(t *testing.T) {
	t.Parallel()
	q, clock := fixture(t)
	m := machine("worker", "fake")
	submit(t, q, projectSpec("solo"))
	run := check(t, q, m, nil).Job
	clock.Advance(20 * time.Second)
	finish(t, q, run, true)
	key := q.fairKey(run.Spec)
	before := q.s.FairUsage[key]
	if before.HorizonSeconds != 20 || before.ServiceSeconds != 20 {
		t.Fatal("service did not set cycle", before)
	}
	clock.Advance(20 * time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenTierQueue(t, q, clock)
	if u := q.s.FairUsage[key]; math.Abs(u.Seconds-before.Seconds/2) > 1e-9 || u.HorizonSeconds != 20 {
		t.Fatal("idle cycle or debt lost on restart", u)
	}
	// A fresh burst replaces the old long cycle with its own measured service.
	submit(t, q, projectSpec("solo"))
	run = check(t, q, m, nil).Job
	clock.Advance(10 * time.Millisecond)
	finish(t, q, run, true)
	if u := q.s.FairUsage[key]; math.Abs(u.HorizonSeconds-.01) > 1e-9 {
		t.Fatal("tiny burst inherited long history", u)
	}
	// Conversely, a sustained allocation grows the history with actual service.
	submit(t, q, projectSpec("solo"))
	run = check(t, q, m, nil).Job
	for range 4 {
		clock.Advance(10 * time.Second)
		check(t, q, m, run)
	}
	if u := q.s.FairUsage[key]; u.HorizonSeconds != 40 || u.ServiceSeconds != 40 {
		t.Fatal("sustained service kept tiny horizon", u)
	}
	// Single-project debt cannot withhold idle capacity.
	finish(t, q, run, true)
	next := submit(t, q, projectSpec("solo"))
	if got := check(t, q, m, nil).Job; got == nil || got.ID != next.ID {
		t.Fatal("single project blocked by debt")
	}
}

func TestAdaptiveFairnessManyProjectsAndStatus(t *testing.T) {
	t.Parallel()
	for _, count := range []int{2, 32} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			q, clock := fixture(t)
			for i := range count {
				for range 2 {
					submit(t, q, projectSpec(fmt.Sprintf("project-%02d", i)))
				}
			}
			m := machine("worker", "fake")
			seen := map[string]bool{}
			for range count {
				run := check(t, q, m, nil).Job
				if run == nil || seen[run.Spec.Project] {
					t.Fatal("project missed first service round", run)
				}
				seen[run.Spec.Project] = true
				clock.Advance(time.Second)
				finish(t, q, run, true)
			}
			check(t, q, m, nil)
			snapshot, err := q.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			for _, j := range snapshot.Jobs {
				if j.State != "queued" {
					continue
				}
				for _, part := range []string{"retained ", "weighted share ", "debt ", "horizon ", "wait ", "observed service age ", "competing projects"} {
					if !strings.Contains(j.Reason, part) {
						t.Fatalf("missing %q: %s", part, j.Reason)
					}
				}
			}
		})
	}
}

func TestAdaptiveFairnessUnknownCoverageAndOldDebt(t *testing.T) {
	t.Parallel()
	q, clock := fixture(t)
	m := machine("worker", "fake")
	submit(t, q, projectSpec("served"))
	run := check(t, q, m, nil).Job
	clock.Advance(2 * time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	u := q.s.FairUsage[q.fairKey(run.Spec)]
	// The default experiment loss timeout supplies only 60s of coverage.
	if u.ServiceSeconds != 60 || u.Seconds > 60 {
		t.Fatal("unknown outage invented service", u)
	}
	// A pre-adaptive persisted record has no guessed fleet half-life. Local
	// demand supplies one when an update first observes it.
	q.s.FairUsage[q.fairKey(run.Spec)] = FairUsage{Seconds: 100, At: clock.Now()}
	q.s.Jobs[run.ID].State = "succeeded"
	waiting := submit(t, q, projectSpec("served"))
	clock.Advance(7 * time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	u = q.s.FairUsage[q.fairKey(waiting.Spec)]
	if u.WaitSeconds != 7 || u.HorizonSeconds != 7 || math.Abs(u.Seconds-50) > 1e-9 {
		t.Fatal("old debt not adapted to measured demand", u)
	}
}
