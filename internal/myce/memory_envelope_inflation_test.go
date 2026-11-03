package myce

import (
	"testing"
	"time"
)

func TestMemoryEnvelopeInterruptedConstraintDoesNotEscalate(t *testing.T) {
	for _, operation := range []string{"start_authorization", "launch", "teardown", "start_deadline", ""} {
		t.Run(operation, func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			s.Needs.Memory = "auto"
			first := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			peak := 2.0
			r := result(&first, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			next := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			r = result(&next, false)
			r.Interrupted = true
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
			if operation != "" {
				r.ExecutionCause = &RuntimeCause{Operation: operation, Class: "io_error"}
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			candidate := first
			candidate.State = "queued"
			got := q.memoryCandidate(&candidate, q.s.Machines[m.ID])
			if got.MemoryEnvelope.HostGB != 2.5 || got.MemoryEnvelope.Basis != "learned" {
				t.Fatalf("infrastructure interruption escalated accepted peak: %+v", got.MemoryEnvelope)
			}
		})
	}
}

func TestMemoryEnvelopeIncidentInterruptedSamplesRecoverAcceptedPeaks(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, 0)
	start := func() Job {
		t.Helper()
		queued := submit(t, q, s)
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.ID != queued.ID {
			t.Fatalf("not assigned: %+v", job(t, q, queued.ID))
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		return *run
	}
	var first Job
	for _, peak := range []float64{15, 17, 18.7} {
		first = start()
		clock.Advance(time.Second)
		r := result(&first, true)
		device := .0625
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, DeviceGB: &device}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	// Recreate the old writer's durable damage after each interrupted attempt.
	// The accepted sample facts remain intact; no fresh success may be required.
	for i := 0; i < 3; i++ {
		candidate := first
		candidate.State = "queued"
		run := start()
		clock.Advance(time.Second)
		r := result(&run, false)
		peak := 18.7
		r.Interrupted = true
		r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "io_error"}
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if err := q.Cancel(run.ID); err != nil {
			t.Fatal(err)
		}
		legacyInterruptedMemoryInvalidation(t, q, r)
		q = memoryFactsRestart(t, q, clock)
		machineCheck(t, q, m)
		owner := q.s.Machines[m.ID]
		target := allocationMachine(owner, []string{"0"})
		got := q.memoryCandidate(&candidate, target)
		want := hostNeed(target, .0625*1.25, 18.7*1.25)
		t.Logf("incident envelope: host %.6f GiB device %.6f GiB source %s", got.MemoryEnvelope.HostGB, got.MemoryEnvelope.DeviceGB, got.MemoryEnvelope.Source)
		if got.MemoryEnvelope.HostGB != want || got.MemoryEnvelope.Basis != "learned" || got.MemoryEnvelope.Evidence != 3 {
			t.Fatalf("incident did not self-correct from retained samples: want %.6f GiB, got %+v", want, got.MemoryEnvelope)
		}
		pending := submit(t, q, s)
		view, _, err := q.captureObservation(pending.ID)
		if err != nil {
			t.Fatal(err)
		}
		projected := view.memoryCandidate(view.s.Jobs[pending.ID], allocationMachine(view.s.Machines[m.ID], []string{"0"}))
		if projected.MemoryEnvelope.HostGB != want {
			t.Fatalf("status lost recovered accepted evidence: %+v", projected.MemoryEnvelope)
		}
		if err := q.Cancel(pending.ID); err != nil {
			t.Fatal(err)
		}

	}
}

// Reproduce exactly the old invalidation, including immutable execution/key
// provenance, after the fixed Complete path has retained the interrupted result.
func legacyInterruptedMemoryInvalidation(t *testing.T, q *Queue, r Result) {
	t.Helper()
	if err := q.update(func() error {
		for id, point := range q.s.WorkObservations {
			old := point
			point.MemoryPeak, point.MemoryRecordedAt = r.MemoryPeak, time.Time{}
			point.MemoryRange, point.GPUHostRange, point.MemorySamples = nil, nil, 0
			q.putWorkObservation(id, point)
			q.appendJobLedger(q.touchJob(r.ID), JobLedgerEntry{Attempt: r.Attempt, Machine: r.Machine, Session: r.Session, Kind: "work_learning", Reason: "memory_invalidated", WorkLearning: &WorkLearningChange{Key: point.Key, Before: curveMeasurementDigest(old), After: curveMeasurementDigest(point)}})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryEnvelopeInterruptedRecoveryReassignmentAndOOM(t *testing.T) {
	for _, ending := range []string{"OOM without peak", "identical constrained peak"} {
		t.Run(ending, func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			s.Needs.Memory = "auto"
			first := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			peak := 2.0
			r := result(&first, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			run := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			r = result(&run, false)
			r.Interrupted = true
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
			r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "timeout"}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			legacyInterruptedMemoryInvalidation(t, q, r)
			q = memoryFactsRestart(t, q, clock)
			retry := assignedJob(machineCheck(t, q, m))
			if retry == nil || retry.ID != run.ID || retry.Attempt != run.Attempt+1 {
				t.Fatal("retry not reassigned", retry)
			}
			candidate := first
			candidate.State = "queued"
			if got := q.memoryCandidate(&candidate, q.s.Machines[m.ID]); got.MemoryEnvelope.HostGB != 2.5 {
				t.Fatalf("reassignment erased recovery: %+v", got.MemoryEnvelope)
			}
			if err := q.Started(result(retry, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			r = result(retry, false)
			wantSource := boundedQuarter
			if ending == "OOM without peak" {
				r.OutOfMemory = true // A real OOM, with no final peak, supersedes interruption.
			} else {
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
				wantSource = "exploratory (previous peak constrained) (bounded: 25% of host allowance)"
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if err := q.Cancel(retry.ID); err != nil {
				t.Fatal(err)
			}
			got := q.memoryCandidate(&candidate, q.s.Machines[m.ID])
			if got.MemoryEnvelope.Source != wantSource || got.MemoryEnvelope.HostGB != 16 {
				t.Fatalf("recovery weakened real OOM exploration: %+v", got.MemoryEnvelope)
			}
		})
	}
}

func TestMemoryEnvelopeInterruptedRecoveryKeepsCorrectedXPUAttribution(t *testing.T) {
	q, clock, m, s := gpuHostFixture(t)
	start := func() *Job {
		t.Helper()
		queued := submit(t, q, s)
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.ID != queued.ID {
			t.Fatal("not assigned", run)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		return run
	}
	complete := func(p *AttemptMemoryPeak) {
		t.Helper()
		run := start()
		clock.Advance(time.Second)
		r := result(run, true)
		r.MemoryPeak = p
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	host, outside, device := 10.75, 8.0, 8.0
	legacy := &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &host, DeviceGB: &device, Events: &MemoryEventCounts{}, GPUHost: &GPUHostMemory{DriverGB: &outside, OutsideGB: &outside, HostDropGB: &host}}
	complete(legacy)
	corrected := sampleXPUAttribution(t, 7.25, .25, 0, 8)
	corrected.DeviceGB = &device
	complete(corrected)
	complete(legacy) // A late result from an old worker must stay superseded.
	run := start()
	clock.Advance(time.Second)
	r := result(run, false)
	r.Interrupted = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, Constrained: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	legacyInterruptedMemoryInvalidation(t, q, r)
	q = memoryFactsRestart(t, q, clock)
	// Reconcile the worker's inventory/qualification before offering new work.
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Recovering: true}); err != nil {
		t.Fatal(err)
	}
	retry := assignedJob(machineCheck(t, q, m))

	if retry == nil || retry.ID != run.ID || retry.Spec.Needs.HostMemoryGB != 9.375 {
		t.Fatalf("recovery restored superseded XPU peak: envelope=%+v needs=%+v", retry.MemoryEnvelope, retry.Spec.Needs)
	}
}

func TestMemoryEnvelopePrelaunchConstraintDoesNotEscalate(t *testing.T) {
	for _, operation := range []string{"launch", "start_authorization"} {
		t.Run(operation, func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			s.Needs.Memory = "auto"
			first := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			peak := 2.0
			r := result(&first, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			queued := submit(t, q, s)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != queued.ID {
				t.Fatal("not assigned")
			}
			clock.Advance(time.Second)
			r = result(run, false) // No Started or Interrupted flag: authorization refused exec.
			r.ExecutionCause = &RuntimeCause{Operation: operation, Class: "timeout"}
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			candidate := first
			candidate.State = "queued"
			if got := q.memoryCandidate(&candidate, q.s.Machines[m.ID]); got.MemoryEnvelope.HostGB != 2.5 {
				t.Fatalf("prelaunch refusal invalidated accepted peak: %+v", got.MemoryEnvelope)
			}
		})
	}
}

func TestMemoryEnvelopeInterruptedRecoveryVetoesCrossHostConstraint(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	peak := 2.0
	r := result(&first, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	interrupted := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r = result(&interrupted, false)
	r.Interrupted = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	legacyInterruptedMemoryInvalidation(t, q, r)
	if err := q.Cancel(interrupted.ID); err != nil {
		t.Fatal(err)
	}
	other := m
	other.ID, other.HostID, other.Session = "second", "second-host", "second-session"
	report := newExtensionReport(q.s.Extensions[s.Extension], &other, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "synthetic-check"
	other.ExtensionReports = []ExtensionReport{report}
	check(t, q, other, nil)
	s.AllowedMachines = []string{other.ID} // Exercise another exact physical key.
	queued := submit(t, q, s)
	assigned := check(t, q, other, nil).Job
	if assigned == nil || assigned.ID != queued.ID {
		t.Fatal("cross-host assignment:", job(t, q, queued.ID).Reason)
	}
	if err := q.Started(result(assigned, false)); err != nil {
		t.Fatal(err)
	}
	failed := *assigned
	clock.Advance(time.Second)
	r = result(&failed, false)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// The same diagnostic peak leaves the aggregate unchanged. A different
	// physical key must not hide this newer real failure from host A's recovery.
	candidate := first
	candidate.State = "queued"
	got := q.memoryCandidate(&candidate, q.s.Machines[m.ID])
	if got.MemoryEnvelope.HostGB != 16 || got.MemoryEnvelope.Basis != "exploratory" {
		t.Fatalf("cross-host constrained failure allowed recovery: %+v", got.MemoryEnvelope)
	}
}
