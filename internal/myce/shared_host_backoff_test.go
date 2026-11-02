package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSharedHostBackoffLatencyBackoff(t *testing.T) {
	for _, elsewhere := range []bool{false, true} {
		t.Run(fmt.Sprint(elsewhere), func(t *testing.T) {
			q, clock, m, run := sharedRunning(t, false)
			id := run.ID
			for n := 0; n < sharedLatencyThreshold; n++ {
				clock.Advance(time.Minute)
				m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, WakeDelayMS: sharedWakeHighMS}
				directive := check(t, q, m, run)
				if !directive.Stop || !directive.Kill {
					t.Fatal("no latency stop")
				}
				// Native Stop && Kill sends cleanup as a release-only result.
				released := result(run, false)
				released.Interrupted, released.Forced, released.ReleaseOnly = true, true, true
				released.PriorityStop = directive.PriorityStop
				if err := q.Complete(released); err != nil {
					t.Fatal(err)
				}
				// A lost acknowledgement can replay without counting twice.
				if err := q.Complete(released); err != nil {
					t.Fatal(err)
				}
				if n < sharedLatencyThreshold-1 && len(job(t, q, id).SharedHostBackoff[hostID(&m)].Stops) != n+1 {
					t.Fatal("release stop not recorded exactly once")
				}
				m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
				next := check(t, q, m, nil).Job
				if n < sharedLatencyThreshold-1 {
					if next == nil {
						t.Fatal("premature cooldown")
					}
					run = next
					if err := q.Started(result(run, false)); err != nil {
						t.Fatal(err)
					}
				} else if next != nil {
					t.Fatal("latency loop restarted")
				}
			}
			got := job(t, q, id)
			if !strings.Contains(got.Reason, "latency backoff") {
				t.Fatal("missing waiting reason", got.Reason)
			}
			if got.InfrastructureRetries != 0 || got.Losses != 0 {
				t.Fatal("charged planned stop")
			}
			if elsewhere {
				other := cpuMachine("other", "other", "cpu", 4, 16)
				if next := check(t, q, other, nil).Job; next == nil || next.ID != id {
					t.Fatal("did not move to eligible host")
				}
			} else {
				clock.Advance(sharedLatencyBackoff - time.Second)
				if check(t, q, m, nil).Job != nil {
					t.Fatal("early admission")
				}
				clock.Advance(time.Second)
				if next := check(t, q, m, nil).Job; next == nil || next.ID != id {
					t.Fatal("backoff did not expire")
				}
			}
		})
	}
}

func TestSharedHostBackoffLatencyWindow(t *testing.T) {
	q, clock, m, run := sharedRunning(t, false)
	for n := 0; n < sharedLatencyThreshold+1; n++ {
		clock.Advance(sharedLatencyWindow + time.Second)
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, WakeDelayMS: sharedWakeHighMS}
		check(t, q, m, run)
		finishInterrupted(t, q, run)
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
		run = check(t, q, m, nil).Job
		if run == nil {
			t.Fatal("old stops accumulated")
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
	}
}
