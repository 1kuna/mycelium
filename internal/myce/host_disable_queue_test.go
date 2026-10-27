package myce

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestAttemptHistoryLateRefund(t *testing.T) {
	q, clock := fixture(t)
	machines := []Machine{machine("one", "fake"), machine("two", "fake"), machine("three", "fake")}
	j := submit(t, q, spec("experiment"))
	var attempts []*Job
	// Each lost copy retains its slot while the next attempt uses a free machine.
	for _, m := range machines {
		a := check(t, q, m, nil).Job
		if a == nil {
			t.Fatal("missing attempt")
		}
		attempts = append(attempts, a)
		clock.Advance(LossAfter)
		check(t, q, observer(), nil)
	}
	before := job(t, q, j.ID)
	if before.State != "failed" || len(before.AttemptHistory) != 3 {
		t.Fatal("missing third-loss outcome", before)
	}
	r := operatorResult(result(attempts[2], false))
	r.Interrupted = true
	foreign := r
	foreign.Session = "foreign"
	if err := q.Complete(foreign); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.AttemptHistory, job(t, q, j.ID).AttemptHistory) {
		t.Fatal("foreign refund changed history")
	}
	clock.Advance(time.Second)
	if err := q.Complete(r); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, clock)
	got := job(t, q, j.ID)
	if got.State != "queued" || got.Losses != 2 || got.InfrastructureRetries != 0 || len(got.AttemptHistory) != 3 {
		t.Fatal("refund accounting changed", got)
	}
	for i, h := range got.AttemptHistory {
		if i < 2 && !reflect.DeepEqual(h, before.AttemptHistory[i]) {
			t.Fatal("another attempt changed", h)
		}
		if i == 2 && (h.State != "queued" || h.Reason != "requeued: stopped by operator" || !h.OperatorStop || !h.EndedAt.Equal(before.AttemptHistory[i].EndedAt)) {
			t.Fatalf("stale refund decision: %+v", h)
		}
	}
}
