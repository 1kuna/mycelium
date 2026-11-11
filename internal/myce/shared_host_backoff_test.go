package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSharedHostBackoffMeasuredRecovery(t *testing.T) {
	for _, elsewhere := range []bool{false, true} {
		t.Run(fmt.Sprint(elsewhere), func(t *testing.T) {
			q, clock, m, run := sharedRunning(t, false)
			clock.Advance(time.Minute) // This attempt's observed time to interference.
			m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, WakeDelayMS: 20}
			directive := check(t, q, m, run)
			if !directive.Stop || !directive.Kill {
				t.Fatal("no latency stop")
			}
			released := result(run, false)
			released.Interrupted, released.Forced, released.ReleaseOnly = true, true, true
			released.PriorityStop = directive.PriorityStop
			if err := q.Complete(released); err != nil {
				t.Fatal(err)
			}
			if err := q.Complete(released); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, run.ID)
			b := got.SharedHostBackoff[hostID(&m)]
			if b.RecoverySeconds != 60 {
				t.Fatal("measured recovery lost or replay counted", b)
			}
			m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
			if check(t, q, m, nil).Job != nil {
				t.Fatal("latency loop restarted after first interruption")
			}
			if !strings.Contains(job(t, q, run.ID).Reason, "latency backoff") {
				t.Fatal("missing reason")
			}
			if got.InfrastructureRetries != 0 || got.Losses != 0 {
				t.Fatal("charged planned stop")
			}
			if elsewhere {
				other := cpuMachine("other", "other", "cpu", 4, 16)
				if next := check(t, q, other, nil).Job; next == nil || next.ID != run.ID {
					t.Fatal("did not move to eligible host")
				}
				return
			}
			// Wall time and a stale report are not recovery evidence.
			clock.Advance(time.Hour)
			if check(t, q, m, nil).Job != nil {
				t.Fatal("timer alone cleared hold")
			}
			for i := 0; i < 12; i++ {
				clock.Advance(5 * time.Second)
				next := check(t, q, m, nil).Job
				if i < 11 && next != nil {
					t.Fatal("early admission")
				}
				if i == 11 && (next == nil || next.ID != run.ID) {
					t.Fatal("measured quiet recovery did not resume")
				}
			}
		})
	}
}

func TestSharedHostBackoffFailureBreaksRecovery(t *testing.T) {
	q, clock, m, run := sharedRunning(t, false)
	clock.Advance(20 * time.Second)
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, WakeDelayMS: 20}
	check(t, q, m, run)
	finishInterrupted(t, q, run)
	good := &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
	m.SharedHostPressure = good
	check(t, q, m, nil)
	clock.Advance(10 * time.Second)
	check(t, q, m, nil)
	m.SharedHostPressure = nil
	check(t, q, m, nil)
	m.SharedHostPressure = good
	clock.Advance(10 * time.Second)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("failure counted toward recovery")
	}
	for i := 0; i < 2; i++ {
		clock.Advance(10 * time.Second)
		next := check(t, q, m, nil).Job
		if i == 0 && next != nil {
			t.Fatal("early recovery")
		}
		if i == 1 && next == nil {
			t.Fatal("did not recover")
		}
	}
}
