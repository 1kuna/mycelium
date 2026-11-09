package myce

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestAttemptHistoryLateRefund(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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
			advanceReportingPeer(t, q, clock, observer(), LossAfter)
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

		if e := q.Complete(r); !errors.Is(e, ErrStale) {
			t.Fatalf("obsolete operator result accepted: %v", e)
		}
		if got := job(t, q, j.ID); got.Losses != 2 {
			t.Fatal("duplicate refunded another attempt")
		}

	})

	t.Run("OperatorStopRefundsExactLossOnce", func(t *testing.T) {
		q, c := fixture(t)
		m := machine("operator", "fake")
		check(t, q, m, nil)
		j := submit(t, q, spec("experiment"))
		running := check(t, q, m, nil).Job
		c.Advance(10 * time.Minute)
		check(t, q, observer(), nil)
		if e := q.Tick(); e != nil {
			t.Fatal(e)
		}
		if job(t, q, j.ID).Losses != 1 {
			t.Fatal("loss not charged")
		}
		wrong := operatorResult(result(running, false))
		wrong.Session = "wrong"
		_ = q.Complete(wrong)
		if job(t, q, j.ID).Losses != 1 {
			t.Fatal("wrong session refunded")
		}
		r := operatorResult(result(running, false))
		r.Interrupted = true
		if e := q.Complete(r); !errors.Is(e, ErrStale) {
			t.Fatalf("obsolete operator result accepted: %v", e)
		}
		if got := job(t, q, j.ID); got.Losses != 0 || got.State != "queued" {
			t.Fatalf("not refunded: %+v", got)
		}
		if e := q.Complete(r); !errors.Is(e, ErrStale) {
			t.Fatalf("obsolete operator result accepted: %v", e)
		}
		if job(t, q, j.ID).Losses != 0 {
			t.Fatal("duplicate changed losses")
		}

	})

	t.Run("OperatorStopBeforeLossIsFree", func(t *testing.T) {
		q, _ := fixture(t)
		m := machine("operator", "fake")
		check(t, q, m, nil)
		j := submit(t, q, spec("experiment"))
		running := check(t, q, m, nil).Job
		r := operatorResult(result(running, false))
		r.Interrupted = true
		if e := q.Complete(r); e != nil {
			t.Fatal(e)
		}
		if got := job(t, q, j.ID); got.Losses != 0 || got.State != "queued" {
			t.Fatalf("operator stop charged: %+v", got)
		}

	})

}
