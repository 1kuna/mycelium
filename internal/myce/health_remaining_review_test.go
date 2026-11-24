package myce

import (
	"testing"
	"time"
)

func TestRemainingHealthOOMFinalClassification(t *testing.T) {
	q, c := fixture(t)
	for _, id := range []string{"first", "second"} {
		c.Advance(time.Second)
		// Replay retryOOM's nested requeue/final-ledger sequence without coupling
		// the diagnostic check to memory admission or worker release custody.
		if err := q.update(func() error {
			j := &Job{ID: id, Attempt: 1, Machine: "worker", Session: "session", State: "running", Spec: cpuSpec(1, 1)}
			final := attemptOutcome(j, Result{Interrupted: true, OutOfMemory: true})
			q.requeue(j, "out of memory; waiting for room above the previous envelope", false)
			q.recordAttemptOutcome(j, final)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		requireIncident(t, q, "infrastructure_loss_streak", "infrastructure-streak/worker", 0, false)
	}
}

func TestRemainingHealthStagingNeedsPositiveRecovery(t *testing.T) {
	q, c := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	submit(t, q, cpuSpec(1, 1))
	for range 2 {
		a := check(t, q, m, nil).Job
		if a == nil {
			t.Fatal("missing staging retry")
		}
		c.Advance(time.Second)
		r := result(a, false)
		r.Interrupted = true
		r.ExecutionCause = &RuntimeCause{Operation: "input_staging", Class: "timeout"}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		requireIncident(t, q, "staging_cache_refused", "attempt-staging/worker", 1, true)
	}
	a := check(t, q, m, nil).Job
	if a == nil {
		t.Fatal("missing recovered assignment")
	}
	c.Advance(time.Second)
	if err := q.Complete(result(a, true)); err != nil {
		t.Fatal(err)
	}
	requireIncident(t, q, "staging_cache_refused", "attempt-staging/worker", 1, false)
	// A new verified refusal after successful staging is a second episode.
	submit(t, q, cpuSpec(1, 1))
	a = check(t, q, m, nil).Job
	r := result(a, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "input_staging", Class: "timeout"}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	requireIncident(t, q, "staging_cache_refused", "attempt-staging/worker", 2, true)
}
