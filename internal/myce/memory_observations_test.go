package myce

import (
	"errors"
	"testing"
	"time"
)

// A planner can read memory evidence before an OOM invalidates it in the same
// transaction. The next read must see that invalidation, while rollback and
// restart must retain exactly the committed evidence.
func TestMemoryObservationIndexInvalidationRollbackAndRestart(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&first, true)
	host := 2.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	estimate := func() memoryEstimate {
		return q.memoryEstimate(q.s.Jobs[first.ID], q.s.Machines[m.ID])
	}
	accepted := func() {
		t.Helper()
		p := estimate()
		if p.host == nil || *p.host.HostGB != host || p.samples != 1 {
			t.Fatalf("accepted evidence changed: %+v", p)
		}
	}
	abort := errors.New("reject after invalidating evidence")
	invalidate := func() error {
		accepted() // Populate the transaction's index before the write.
		q.recordWorkOOM(q.touchJob(first.ID), Result{OutOfMemory: true}, true)
		if p := estimate(); p.host == nil || p.samples != 0 || !p.escalation || p.measuredHostGB != host+2*minimumHostMemoryGB {
			t.Fatalf("planner reused evidence invalidated in this transaction: %+v", p)
		}
		return nil
	}
	if err := q.update(func() error {
		invalidate()
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	accepted()
	if err := q.update(invalidate); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, clock)
	check(t, q, m, nil)
	if p := estimate(); p.host == nil || p.samples != 0 || !p.escalation || p.measuredHostGB != host+2*minimumHostMemoryGB {
		t.Fatalf("restart restored invalidated evidence: %+v", p)
	}
}
