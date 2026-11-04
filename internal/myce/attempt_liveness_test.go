package myce

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAttemptLivenessAdvancingIdleUnknownAndFencing(t *testing.T) {
	q, clock, m, spec := observationFixture(t)
	j := startObservation(t, q, m, spec)
	report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true}
	send := func() {
		t.Helper()
		if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
			t.Fatal(err)
		}
	}
	read := func(state string) *Job {
		t.Helper()
		got := runtimeJob(t, q, j.ID)
		if got.Liveness == nil || got.Liveness.State != state {
			t.Fatalf("want %s: %+v", state, got.Liveness)
		}
		receipt, err := q.jobSnapshot(j.ID)
		if err != nil || receipt.Liveness == nil || receipt.Liveness.State != state {
			t.Fatalf("receipt: %v %+v", err, receipt)
		}
		if len(got.Copies) == 0 || got.Copies[len(got.Copies)-1].Liveness.State != state {
			t.Fatalf("copy: %+v", got.Copies)
		}
		return got
	}
	// Legacy workers supply no liveness. No application progress/log/checkpoint is needed.
	send()
	read("unknown")
	var cpu, dir livenessCounter
	var r LivenessReport
	sample := func(c, d float64, cpuErr, dirErr error) {
		r.CPU = cpu.observe(c, cpuErr, clock.Now(), r.CPU)
		r.WorkDir = dir.observe(d, dirErr, clock.Now(), r.WorkDir)
		report.Liveness = r.aged(clock.Now())
		send()
	}
	sample(1, 100, nil, nil)
	read("unknown") // A baseline is not idle.
	clock.Advance(5 * time.Second)
	sample(2, 100, nil, nil)
	got := read("advancing")
	if !strings.Contains(JobTiming(got), "alive and advancing") || got.Progress != 0 || got.ApplicationProgress != nil {
		t.Fatal("not independent/advisory", got)
	}
	clock.Advance(5 * time.Second)
	sample(2, 200, nil, nil)
	read("advancing") // Disk-only advance with a live process tree.
	for i := 0; i < 24; i++ {
		clock.Advance(5 * time.Second)
		sample(2, 200, nil, nil)
	}
	got = read("idle")
	if got.Liveness.IdleSeconds == nil || *got.Liveness.IdleSeconds != 120 || !strings.Contains(JobTiming(got), "idle for 2.0 min") {
		t.Fatal("idle duration", got.Liveness)
	}
	clock.Advance(31 * time.Second)
	got = read("unknown")
	if got.Liveness.CPU.AgeSeconds != 31 || got.Liveness.WorkDir.AgeSeconds != 31 {
		t.Fatal("ages", got.Liveness)
	}
	// A repeated cached worker snapshot is aged before sending, never refreshed.
	report.Liveness = r.aged(clock.Now())
	send()
	read("unknown")
	sample(2, 200, nil, nil)
	read("unknown") // Observation gap breaks idle continuity.
	clock.Advance(5 * time.Second)
	sample(2, 200, nil, errors.New("scan incomplete"))
	read("unknown")
	clock.Advance(5 * time.Second)
	sample(3, 200, nil, errors.New("scan incomplete"))
	read("advancing")
	// Missing CPU cannot establish that the payload is alive, even if files grow.
	clock.Advance(5 * time.Second)
	sample(3, 300, errors.New("process unavailable"), nil)
	read("unknown")
	clock.Advance(5 * time.Second)
	sample(3, 300, nil, nil)
	read("unknown")
	clock.Advance(5 * time.Second)
	sample(3, 300, nil, nil)
	read("idle")
	// New execution resets evidence; the old heartbeat and result cannot change it.
	if err := q.Complete(Result{Execution: report.Execution, Machine: m.ID, Session: j.Session, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	next := check(t, q, m, nil).Job
	if next == nil || next.Attempt == j.Attempt {
		t.Fatal("not retried", next)
	}
	send()
	if err := q.Complete(Result{Execution: report.Execution, Machine: m.ID, Session: j.Session, Success: true}); err == nil {
		t.Fatal("stale result accepted")
	}
	got = runtimeJob(t, q, j.ID)
	if got.Attempt != next.Attempt || got.Liveness != nil {
		t.Fatal("stale evidence survived", got.Liveness)
	}
}
