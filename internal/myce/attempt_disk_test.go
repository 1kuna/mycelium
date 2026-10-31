package myce

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestAttemptDiskLimitFailure(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		t.Run(fmt.Sprint("stopping=", stopping), func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("disk-worker", "fake")
			j := submit(t, q, spec("experiment"))
			assigned := check(t, q, m, nil).Job
			if assigned == nil {
				t.Fatal("missing assignment")
			}
			if err := q.Started(result(assigned, false)); err != nil {
				t.Fatal(err)
			}
			if stopping {
				if err := q.Leave(m.ID, true); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(time.Second)
			denied := fmt.Errorf("write failed: %w", &os.PathError{Op: "write", Path: "/private/attempt/output", Err: syscall.EDQUOT})
			r := result(assigned, false)
			r.Error = denied.Error()
			r.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("outputs", denied))
			r.Interrupted = stopping // A stop cannot disguise a confirmed quota failure.
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.State != "failed" || got.Attempt != assigned.Attempt || got.Losses != 0 || got.InfrastructureRetries != 0 || got.ExecutionCause == nil || got.ExecutionCause.Class != diskLimitExceeded {
				t.Fatalf("disk denial retried, charged as infrastructure, or lost: %+v", got)
			}
			if got.FinishedAt == nil || !got.FinishedAt.Equal(clock.Now()) || len(got.AttemptHistory) == 0 {
				t.Fatal("disk failure missing clock/attempt evidence")
			}
			outcome := got.AttemptHistory[len(got.AttemptHistory)-1]
			if outcome.Interrupted || outcome.OutOfMemory || outcome.ExecutionCause == nil || outcome.ExecutionCause.Class != diskLimitExceeded {
				t.Fatalf("disk failure poisoned memory or stop evidence: %+v", outcome)
			}
			if strings.Contains(got.Reason, "/private/") || strings.Contains(outcome.Reason, "/private/") {
				t.Fatal("quota diagnostics exposed a private path")
			}
			if err := q.Complete(r); err != nil {
				t.Fatal("lost completion acknowledgement was not replayable", err)
			}
			r.Attempt++
			if err := q.Complete(r); err != ErrStale {
				t.Fatalf("wrong attempt quota report accepted: %v", err)
			}
		})
	}
}

func TestAttemptDiskDenialEvidence(t *testing.T) {
	full := runtimeDiagnostic(runtimeErrorFor("outputs", syscall.ENOSPC))
	if full.Class != "storage_full" {
		t.Fatal("uncontained ENOSPC was mistaken for an attempt quota")
	}
	r := Result{Error: "disk_limit_exceeded: no space left on device", ExecutionCause: full}
	if attemptDiskLimitFailure(r) {
		t.Fatal("text/uncontained ENOSPC established a quota failure")
	}
	r.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("outputs", syscall.EDQUOT))
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	facts := attemptCause{memberOutcome: memberOutcome{Signals: []deliveredSignal{{Signal: "TERM", At: at}}, Exit: &memberExit{Status: 1, ObservedAt: at.Add(time.Second)}}}
	r = classifyAttemptOutcome(r, facts)
	if !attemptDiskLimitFailure(r) || r.Interrupted || r.OutOfMemory {
		t.Fatalf("quota denial became an interruption or memory failure: %+v", r)
	}
}
