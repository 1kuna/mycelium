package myce

import (
	"testing"
	"time"
)

func TestInputStagingReportsRejectStaleAttemptsAndResetOnRetry(t *testing.T) {
	q, clock := fixture(t)
	m := machine("staging", "fake")
	submitted := submit(t, q, spec("experiment"))
	assigned := check(t, q, m, nil).Job
	staged := clock.Now().Add(time.Second)
	report := AttemptReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt + 1}, Session: assigned.Session, InputsStagedAt: &staged}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, submitted.ID); got.InputsStagedAt != nil {
		t.Fatal("stale report supplied staging evidence")
	}
	report.Attempt = assigned.Attempt
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, submitted.ID)
	if got.InputsStagedAt == nil || !got.InputsStagedAt.Equal(staged) || got.StartedAt != nil {
		t.Fatal("prelaunch staging report was lost or started the payload", got)
	}
	r := result(assigned, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, submitted.ID); got.State != "queued" || got.InputsStagedAt != nil {
		t.Fatal("retry retained previous-attempt staging evidence", got)
	}
}
