package myce

import (
	"errors"
	"testing"
	"time"
)

func TestWallTimeExpiryCustodyRestart(t *testing.T) {
	q, clock := fixture(t)
	m := sharedDiscreteMachine("xpu")
	s := sharedSpec("experiment", 3)
	s.WallTime = "10s"
	first := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != first.ID {
		t.Fatal("no assignment")
	}
	clock.Advance(time.Hour) // Preparation is not execution.
	machineCheck(t, q, m, run)
	if wallTimeStopped(job(t, q, run.ID)) {
		t.Fatal("preparation consumed execution allowance")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	started := clock.Now()
	clock.Advance(9 * time.Second)
	machineCheck(t, q, m, run)
	q = reopenCancellationQueue(t, q, clock)
	if got := job(t, q, run.ID); got.StartedAt == nil || !got.StartedAt.Equal(started) || wallTimeStopped(got) {
		t.Fatal("restart renewed/expired allowance", got)
	}
	clock.Advance(time.Second)
	machineCheck(t, q, m, run)
	got := job(t, q, run.ID)
	if got.State != "stopping" || !wallTimeStopped(got) || got.ReleaseConfirmed {
		t.Fatal("limit did not retain exact stop custody", got)
	}
	q.projectWallTime(got)
	if got.WallTime == nil || got.WallTime.Limit != "10s" || got.WallTime.RemainingSeconds == nil || *got.WallTime.RemainingSeconds != 0 {
		t.Fatal("missing limit status", got.WallTime)
	}
	clock.Advance(StopGrace + time.Second)
	machineCheck(t, q, m, run)
	if !attemptReleasePending(job(t, q, run.ID)) {
		t.Fatal("elapsed grace manufactured release")
	}
	// A wrong key cannot discharge custody, and a late success cannot publish.
	ack := result(run, false)
	ack.ReleaseOnly = true
	ack.Session += "-stale"
	_ = q.Complete(ack)
	if !attemptReleasePending(job(t, q, run.ID)) {
		t.Fatal("stale key released attempt")
	}
	if err := q.Complete(result(run, true)); !errors.Is(err, ErrStale) {
		t.Fatal("late success accepted", err)
	}
	got = job(t, q, run.ID)
	if got.State != "failed" || got.ExecutionCause == nil || got.ExecutionCause.Class != wallTimeLimit || got.Losses != 0 || got.InfrastructureRetries != 0 || !got.ReleaseConfirmed {
		t.Fatal("wrong limit outcome", got)
	}
	q = reopenCancellationQueue(t, q, clock)
	got = job(t, q, run.ID)
	if got.State != "failed" || got.AttemptHistory[len(got.AttemptHistory)-1].ExecutionCause.Class != wallTimeLimit {
		t.Fatal("typed cause lost on restart", got)
	}
}

func TestWallTimeOrdinaryFailurePolicy(t *testing.T) {
	for _, mode := range []string{"success-before", "omitted", "independent-failure", "cancel", "release-only", "loss-after-expiry"} {
		t.Run(mode, func(t *testing.T) {
			q, c := fixture(t)
			m := sharedDiscreteMachine("xpu")
			s := sharedSpec("production", 1)
			if mode != "omitted" {
				s.WallTime = "2s"
			}
			submit(t, q, s)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil {
				t.Fatal("no assignment")
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "success-before":
				c.Advance(time.Second)
			case "omitted":
				c.Advance(time.Hour)
				machineCheck(t, q, m, run)
			default:
				c.Advance(2 * time.Second)
				machineCheck(t, q, m, run)
			}
			r := result(run, true)
			want := "failed"
			switch mode {
			case "success-before", "omitted":
				want = "succeeded"
			case "cancel":
				if err := q.Cancel(run.ID); err != nil {
					t.Fatal(err)
				}
				r.Success = false
				r.Interrupted = true
				want = "cancelled"
			case "independent-failure":
				r.Success = false
				r.ExecutionCause = &RuntimeCause{Operation: "command", Class: "command_exit"}
			case "release-only":
				r.Success = false
				r.ReleaseOnly = true
			case "loss-after-expiry":
				c.Advance(ProductionLossAfter + time.Second)
				check(t, q, observer(), nil)
				got := job(t, q, run.ID)
				if got.State != "failed" || got.Losses != 0 || got.InfrastructureRetries != 0 || !attemptReleasePending(got) {
					t.Fatal("expiry charged loss or lost custody", got)
				}
				return
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, run.ID)
			if got.State != want || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatal("wrong failure/retry policy", got)
			}
			if mode == "release-only" && (got.ExecutionCause == nil || got.ExecutionCause.Class != wallTimeLimit) {
				t.Fatal("release lost typed cause", got)
			}
			if mode == "independent-failure" && got.ExecutionCause.Class != "command_exit" {
				t.Fatal("expiry hid independent exit", got)
			}
		})
	}
}

func TestWallTimeValidation(t *testing.T) {
	for _, v := range []string{"0s", "-1s", "bogus", "100000000000000000h"} {
		s := spec("experiment")
		s.WallTime = v
		if s.Validate() == nil {
			t.Fatal("accepted invalid duration", v)
		}
	}
	s := spec("experiment")
	s.WallTime = "250ms"
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
}
