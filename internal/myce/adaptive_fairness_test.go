package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Previous large service creates debt; repeated tiny jobs must not suppress a
// waiting large project. Exercise bounded waiting through real admission/accounting.
func TestAdaptiveFairnessLargeWaitConverges(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, quantum := range []time.Duration{time.Second} {
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
				t.Logf("large job recovered after %d tiny services; workload quantum %s", served, quantum)
				// Once large work runs, do not preempt it merely to rebalance.
				clock.Advance(8 * quantum)
				check(t, q, m, run)
				if got := job(t, q, large.ID); got.State != "running" {
					t.Fatal("same-tier fairness stopped active work", got.State)
				}
			})
		}

	})

	t.Run("SingleProjectDebtCannotWithholdCapacity", func(t *testing.T) {
		q, clock := fixture(t)
		m := machine("worker", "fake")
		submit(t, q, projectSpec("solo"))
		run := check(t, q, m, nil).Job
		clock.Advance(20 * time.Second)
		finish(t, q, run, true)
		next := submit(t, q, projectSpec("solo"))
		if got := check(t, q, m, nil).Job; got == nil || got.ID != next.ID {
			t.Fatal("single project blocked by debt")
		}
	})
}

func TestAdaptiveFairnessManyProjectsAndStatus(t *testing.T) {
	t.Parallel()
	for _, count := range []int{2} {
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
				for _, part := range []string{"wait ", "competing projects"} {
					if !strings.Contains(j.Reason, part) {
						t.Fatalf("missing %q: %s", part, j.Reason)
					}
				}
			}
		})
	}
}

// A queue read/check-in may split an observation interval without changing the
// demand, service, final debt, or next winner. Exercise both learning and a
// measured horizon that crosses from constant to linearly growing.
func TestAdaptiveFairnessAccountingCadenceInvariant(t *testing.T) {
	t.Parallel()
	for _, quantum := range []float64{20} {
		t.Run(fmt.Sprint(quantum), func(t *testing.T) {
			replay := func(steps int) (float64, float64, string) {
				q, clock := fixture(t)
				q.lossTimeouts = [3]time.Duration{time.Hour, time.Hour, time.Hour}
				m := machine("worker", "fake")
				submit(t, q, projectSpec("a"))
				run := check(t, q, m, nil).Job
				b := submit(t, q, projectSpec("b"))
				q.s.FairUsage[q.fairKey(b.Spec)] = FairUsage{Seconds: 100, At: clock.Now(), HorizonSeconds: quantum, QuantumSeconds: quantum}
				for range steps {
					clock.Advance(time.Minute / time.Duration(steps))
					check(t, q, m, run)
				}
				aUsage := q.s.FairUsage[q.fairKey(run.Spec)].Seconds
				bUsage := q.s.FairUsage[q.fairKey(b.Spec)].Seconds
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
			for _, splits := range []int{2} {
				x, y, next := replay(splits)
				if winner != next {
					t.Fatalf("%d splits changed admission: once=(%.12g, %.12g, %s), split=(%.12g, %.12g, %s)", splits, a, b, winner, x, y, next)
				}
			}
		})
	}
}

func TestAdaptiveFairnessReconnectKeepsCoveredAge(t *testing.T) {
	t.Parallel()

	t.Run("contract", func(t *testing.T) {
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
				if u := q.s.FairUsage[q.fairKey(run.Spec)]; u.ServiceSeconds != 60 {
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
				if u := q.s.FairUsage[q.fairKey(run.Spec)]; u.ServiceSeconds != 61 || u.HorizonSeconds != 61 {
					t.Fatal("downtime became observed service", u)
				}
			})
		}

	})

}
