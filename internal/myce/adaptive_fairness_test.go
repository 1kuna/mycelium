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

func TestAdaptiveFairnessDemandCyclesAndRestart(t *testing.T) {
	t.Parallel()
	q, clock := fixture(t)
	m := machine("worker", "fake")
	submit(t, q, projectSpec("solo"))
	run := check(t, q, m, nil).Job
	clock.Advance(20 * time.Second)
	finish(t, q, run, true)
	q.mu.RLock()
	key := q.fairKey(run.Spec)
	before := q.s.FairUsage[key]
	q.mu.RUnlock()
	if before.HorizonSeconds != 20 || before.ServiceSeconds != 20 {
		t.Fatal("service did not set cycle", before)
	}
	clock.Advance(20 * time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenTierQueue(t, q, clock)
	if u := testFairUsage(q, key); math.Abs(u.Seconds-before.Seconds/2) > 1e-9 || u.HorizonSeconds != 20 {
		t.Fatal("idle cycle or debt lost on restart", u)
	}
	// A fresh burst replaces the old long cycle with its own measured service.
	submit(t, q, projectSpec("solo"))
	run = check(t, q, m, nil).Job
	clock.Advance(10 * time.Millisecond)
	finish(t, q, run, true)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if u := testFairUsage(q, key); math.Abs(u.HorizonSeconds-.01) > 1e-9 {
		t.Fatal("tiny burst inherited long history", u)
	}
	// Conversely, a sustained allocation grows the history with actual service.
	submit(t, q, projectSpec("solo"))
	run = check(t, q, m, nil).Job
	for range 40 {
		clock.Advance(time.Second)
		check(t, q, m, run)
	}
	if u := testFairUsage(q, key); u.HorizonSeconds != 40 || u.ServiceSeconds != 40 {
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
	for _, count := range []int{2, 8} {
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
	q.mu.RLock()
	key := q.fairKey(run.Spec)
	q.mu.RUnlock()
	u := testFairUsage(q, key)
	// The default experiment loss timeout supplies only 60s of coverage.
	if u.ServiceSeconds != 60 || u.Seconds > 60 {
		t.Fatal("unknown outage invented service", u)
	}
	// A pre-adaptive persisted record has no guessed fleet half-life. Local
	// demand supplies one when an update first observes it.
	q.mu.Lock()
	q.s.FairUsage[key] = FairUsage{Seconds: 100, At: clock.Now()}
	q.s.Jobs[run.ID].State = "succeeded"
	q.mu.Unlock()
	waiting := submit(t, q, projectSpec("served"))
	clock.Advance(7 * time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q.mu.RLock()
	key = q.fairKey(waiting.Spec)
	q.mu.RUnlock()
	u = testFairUsage(q, key)
	if u.WaitSeconds != 7 || u.HorizonSeconds != 7 || u.Seconds != 100 || u.QuantumSeconds != 0 {
		t.Fatal("legacy debt changed before observed service supplied a scale", u)
	}
}

// A queue read/check-in may split an observation interval without changing the
// demand, service, final debt, or next winner. Exercise both learning and a
// measured horizon that crosses from constant to linearly growing.
func TestAdaptiveFairnessAccountingCadenceInvariant(t *testing.T) {
	t.Parallel()
	for _, quantum := range []float64{0, 20} {
		t.Run(fmt.Sprint(quantum), func(t *testing.T) {
			replay := func(steps int) (float64, float64, string) {
				q, clock := fixture(t)
				q.lossTimeouts = [3]time.Duration{time.Hour, time.Hour, time.Hour}
				m := machine("worker", "fake")
				submit(t, q, projectSpec("a"))
				run := check(t, q, m, nil).Job
				b := submit(t, q, projectSpec("b"))
				// Admission capture can still account usage after check-in returns.
				q.mu.Lock()
				q.s.FairUsage[q.fairKey(b.Spec)] = FairUsage{Seconds: 100, At: clock.Now(), HorizonSeconds: quantum, QuantumSeconds: quantum}
				q.mu.Unlock()
				for range steps {
					clock.Advance(time.Minute / time.Duration(steps))
					check(t, q, m, run)
				}
				q.mu.Lock()
				aUsage := q.s.FairUsage[q.fairKey(run.Spec)].Seconds
				bUsage := q.s.FairUsage[q.fairKey(b.Spec)].Seconds
				q.mu.Unlock()
				finish(t, q, run, true)
				a := submit(t, q, projectSpec("a"))
				got := check(t, q, m, nil).Job
				if got == nil {
					t.Fatal("no next assignment")
				}
				if got.ID != a.ID && got.ID != b.ID {
					t.Fatal("unexpected assignment", got.ID)
				}
				return aUsage, bUsage, got.Spec.Project
			}
			a, b, winner := replay(1)
			for _, splits := range []int{2, 60} {
				x, y, next := replay(splits)
				if math.Abs(a-x) > 1e-9 || math.Abs(b-y) > 1e-9 || winner != next {
					t.Fatalf("%d splits changed admission: once=(%.12g, %.12g, %s), split=(%.12g, %.12g, %s)", splits, a, b, winner, x, y, next)
				}
			}
		})
	}
}

func TestAdaptiveFairnessReconnectKeepsCoveredAge(t *testing.T) {
	t.Parallel()
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprint(restart), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			submit(t, q, projectSpec("served"))
			run := check(t, q, m, nil).Job
			clock.Advance(2 * time.Hour)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			q.mu.RLock()
			key := q.fairKey(run.Spec)
			q.mu.RUnlock()
			if u := testFairUsage(q, key); u.ServiceSeconds != 60 {
				t.Fatal("initial coverage", u)
			}
			if restart {
				q = reopenTierQueue(t, q, clock)
			}
			check(t, q, m, run)
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if u := testFairUsage(q, key); u.ServiceSeconds != 61 || u.HorizonSeconds != 61 {
				t.Fatal("downtime became observed service", u)
			}
		})
	}
}
