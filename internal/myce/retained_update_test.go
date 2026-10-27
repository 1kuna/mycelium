package myce

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Deadline maintenance may run before a stale result aborts the transaction.
// Both the job and its retained keyed answer must return to the prior state.
func TestRetainedKeyAnswerSurvivesRejectedResult(t *testing.T) {
	q, c := fixture(t)
	s := spec("production")
	s.SubmissionKey = "rollback-deadline"
	submit(t, q, s)
	c.Advance(ProductionStartTimeout + time.Second)
	before, err := json.Marshal(q.s)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(Result{Execution: Execution{ID: "missing", Attempt: 1}, Machine: "missing", Session: "missing"}); !errors.Is(err, ErrStale) {
		t.Fatalf("expected stale result: %v", err)
	}
	after, err := json.Marshal(q.s)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("rejected result changed the job or its retained keyed answer")
	}
}

func retainedLossFixture(t *testing.T) (*Queue, *fakeClock, *Job, Result) {
	t.Helper()
	q, c := fixture(t)
	m := machine("operator", "fake")
	s := spec("experiment")
	s.SubmissionKey = "retained-loss"
	j := submit(t, q, s)
	first := check(t, q, m, nil).Job
	c.Advance(LossAfter)
	check(t, q, observer(), nil)
	// Keep the old copy unresolved until its original release below.
	second := check(t, q, machine("retry", "fake"), nil).Job
	if second == nil {
		t.Fatal("lost attempt was not retried")
	}
	finish(t, q, second, true)
	r := result(first, false)
	r.ReleaseOnly = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	finished := q.s.Jobs[j.ID]
	if finished.State != "succeeded" || !finished.ReleaseConfirmed || finished.Losses != 1 {
		t.Fatal("expected a completed retry with one earlier infrastructure loss")
	}
	for _, copy := range finished.Copies {
		if !copy.ReleaseConfirmed {
			t.Fatal("fixture retained an unreleased execution")
		}
	}
	return q, c, j, result(first, false)
}

// An exact old-attempt stop can arrive after a successful retry and all release
// acknowledgements. Its infrastructure-loss refund still has to be durable.
func TestRetainedOperatorRefundSurvivesRestart(t *testing.T) {
	q, c, j, r := retainedLossFixture(t)
	r.OperatorStop, r.Interrupted, r.ReleasePending = true, true, true
	if err := q.Complete(r); !errors.Is(err, ErrStale) {
		t.Fatalf("old-attempt payload was not rejected: %v", err)
	}
	if q.s.Jobs[j.ID].Losses != 0 {
		t.Fatal("exact operator stop did not refund the loss")
	}
	q = reopenCancellationQueue(t, q, c)
	if got := q.s.Jobs[j.ID]; got.Losses != 0 || got.State != "succeeded" {
		t.Fatal("operator refund or successful retry changed on restart")
	}
}

func TestRetainedOperatorRefundRollsBackOnWriteFailure(t *testing.T) {
	q, _, _, r := retainedLossFixture(t)
	r.OperatorStop, r.Interrupted = true, true
	before, err := json.Marshal(q.s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`CREATE TRIGGER reject_refund BEFORE INSERT ON queue_state BEGIN SELECT RAISE(ABORT,'write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err == nil || errors.Is(err, ErrStale) {
		t.Fatalf("expected persistence failure: %v", err)
	}
	after, err := json.Marshal(q.s)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("failed write retained an operator refund or changed its keyed answer")
	}
}

// An unowned release acknowledgement must not create a retained answer or
// prevent the queue from reopening.
func TestRetainedUnknownReleaseSurvivesRestart(t *testing.T) {
	q, c := fixture(t)
	r := Result{Execution: Execution{ID: "missing", Attempt: 1}, Machine: "missing", Session: "missing", ReleaseOnly: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, c)
	if len(q.s.Jobs) != 0 || len(q.s.Submissions) != 0 {
		t.Fatal("unowned release acknowledgement created a job or submission answer")
	}
}
