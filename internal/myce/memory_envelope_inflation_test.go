package myce

import (
	"strings"
	"testing"
	"time"
)

func TestMemoryEnvelopeInfrastructureSampleFilter(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		started, interrupted bool
		cause                *RuntimeCause
	}{
		{name: "lost before start"},
		{name: "lost after start", started: true, interrupted: true},
		{name: "start authorization timeout", started: true, cause: &RuntimeCause{Operation: "start_authorization", Class: "timeout"}},
		{name: "start authorization io error", started: true, cause: &RuntimeCause{Operation: "start_authorization", Class: "io_error"}},
		{name: "launch authorization refusal", started: true, cause: &RuntimeCause{Operation: "launch", Class: launchRefused, Predicate: "launch_authorization", PrelaunchRefused: true}},
		{name: "teardown", started: true, interrupted: true, cause: &RuntimeCause{Operation: "teardown", Class: "io_error"}},
		{name: "start deadline", started: true, interrupted: true, cause: &RuntimeCause{Operation: "start_deadline", Class: "timeout"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			if tc.started {
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(time.Second)
			r = result(run, false)
			r.Interrupted, r.ExecutionCause = tc.interrupted, tc.cause
			// A much larger diagnostic peak and raw ceiling events must not
			// invalidate the accepted estimate or raise an exploratory floor.
			pressure := 12.0
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &pressure, Constrained: true, Events: &MemoryEventCounts{High: 1}}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, run.ID)
			if len(got.MemoryAttempts) != 1 {
				t.Fatal("missing diagnostic facts")
			}
			f := got.MemoryAttempts[0]
			if f.LearningReason != "infrastructure_interrupted" || f.MemoryPeak.Constrained || f.MemoryPeak.Events.High != 1 || *f.MemoryPeak.HostGB != pressure {
				t.Fatalf("interruption recorded constrained learning or lost raw counters: %+v", f)
			}
			// Classification belongs to the sample, independent of outcome
			// retention and mutable retry timing. Reload the durable facts.
			q = memoryFactsRestart(t, q, clock)
			check(t, q, m, nil) // Refresh qualification after queue restart.
			candidate := first
			candidate.State = "queued"
			if envelope := q.memoryCandidate(&candidate, q.s.Machines[m.ID]).MemoryEnvelope; envelope.HostGB != 2+minimumHostMemoryGB || envelope.Basis != "learned" {
				t.Fatalf("interruption escalated accepted peak: %+v", envelope)
			}
			candidate.MemoryAttempts = job(t, q, run.ID).MemoryAttempts
			if lower := q.constrainedMemoryLower(&candidate, memoryEstimate{}); lower != 0 {
				t.Fatalf("interrupted diagnostic became a retry floor: %v", lower)
			}
		})
	}
}

func TestMemoryEnvelopeGenuineConstraintEscalationBounded(t *testing.T) {
	for _, ending := range []string{"constrained completion", "constrained completion without host peak", "OOM with peak", "OOM without peak", "OOM racing interruption", "payload exit with interrupted flag"} {
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
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
			switch ending {
			case "constrained completion without host peak":
				r.MemoryPeak = &AttemptMemoryPeak{Constrained: true}
			case "OOM with peak":
				r.OutOfMemory = true
			case "OOM without peak":
				r.OutOfMemory, r.MemoryPeak = true, nil
			case "OOM racing interruption":
				r.OutOfMemory, r.Interrupted = true, true
				r.ExecutionCause = &RuntimeCause{Operation: "teardown", Class: "io_error"}
			case "payload exit with interrupted flag":
				code := 1
				r.Interrupted = true
				r.ExecutionCause = &RuntimeCause{Operation: "command", Class: "command_exit", ExitCode: &code}
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			candidate := first
			candidate.State = "queued"
			got := q.memoryCandidate(&candidate, q.s.Machines[m.ID]).MemoryEnvelope
			if got.HostGB != 32-minimumHostMemoryGB || got.Basis != "exploratory" || !strings.Contains(got.Source, "accepted prior invalidated") {
				t.Fatalf("genuine failure lost measured exploration bound: %+v", got)
			}
			retryState := job(t, q, run.ID)
			if ending == "payload exit with interrupted flag" && retryState.State != "failed" {
				t.Fatal("payload exit became an infrastructure retry", retryState.State)
			}
			if retryState.MemoryOOMFloor != nil {
				retryCandidate := *retryState
				retryCandidate.State = "queued"
				floor := retryState.MemoryOOMFloor.HostMemoryGB
				if e := q.memoryCandidate(&retryCandidate, q.s.Machines[m.ID]).MemoryEnvelope; e.HostGB < floor {
					t.Fatalf("accepted bound erased OOM floor %.3f: %+v", floor, e)
				}
			}
			q = memoryFactsRestart(t, q, clock)
			check(t, q, m, nil)
			if e := q.memoryCandidate(&candidate, q.s.Machines[m.ID]).MemoryEnvelope; e.HostGB < 2+minimumHostMemoryGB || e.Basis != "exploratory" {
				t.Fatalf("restart lost bounded prior: %+v", e)
			}

			// Existing writer re-teaches an invalidated aggregate only after
			// a clean accepted completion; no historical reconstruction.
			if terminal(job(t, q, run.ID).State) {
				submit(t, q, s)
			}
			retry := check(t, q, m, nil).Job
			if retry == nil {
				t.Fatal("missing retry")
			}
			if err := q.Started(result(retry, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			accepted := result(retry, true)
			accepted.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
			if err := q.Complete(accepted); err != nil {
				t.Fatal(err)
			}
			if got := q.memoryCandidate(&candidate, q.s.Machines[m.ID]).MemoryEnvelope; got.HostGB != 2+minimumHostMemoryGB || got.Basis != "learned" {
				t.Fatalf("clean completion did not re-teach aggregate: %+v", got)
			}
		})
	}
}

func TestMemoryEnvelopeIncidentSamplesIgnoreInfrastructure(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, 0)
	start := func() Job {
		t.Helper()
		queued := submit(t, q, s)
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.ID != queued.ID {
			t.Fatal("not assigned")
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		return *run
	}
	var first Job
	device := .0625
	for _, peak := range []float64{15, 17, 18.7} {
		first = start()
		clock.Advance(time.Second)
		r := result(&first, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, DeviceGB: &device}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		run := start()
		clock.Advance(time.Second)
		r := result(&run, false)
		pressure := 18.7
		r.Interrupted = true
		r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "io_error"}
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &pressure, Constrained: true}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if err := q.Cancel(run.ID); err != nil {
			t.Fatal(err)
		}
	}
	q = memoryFactsRestart(t, q, clock)
	machineCheck(t, q, m)
	candidate := first
	candidate.State = "queued"
	target := allocationMachine(q.s.Machines[m.ID], []string{"0"})
	got := q.memoryCandidate(&candidate, target).MemoryEnvelope
	want := hostNeed(target, device+minimumHostMemoryGB, 18.7+minimumHostMemoryGB)
	t.Logf("incident envelope: host %.6f GiB device %.6f GiB source %s", got.HostGB, got.DeviceGB, got.Source)
	if got.HostGB != want || got.Basis != "learned" || got.Evidence != 3 {
		t.Fatalf("interruptions changed accepted incident peaks: want %.6f, got %+v", want, got)
	}
}

func TestMemoryEnvelopeExhaustedLossLateReleaseSample(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory, s.Priority = "auto", "production"
	if err := q.SetProductionInfrastructureRetries(0); err != nil {
		t.Fatal(err)
	}
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	peak := 2.0
	r := result(&first, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	lost := startObservation(t, q, m, s)
	clock.Advance(ProductionLossAfter + time.Second)
	check(t, q, observer(), nil)
	if got := job(t, q, lost.ID); got.State != "failed" || got.Reason != "infrastructure_retries_exhausted" || got.Machine != m.ID {
		t.Fatalf("loss did not exhaust retry budget with retained identity: %+v", got)
	}
	// A late release can still add diagnostic counters to this exact exhausted
	// attempt. It cannot turn queue-observed infrastructure loss into pressure.
	pressure := 12.0
	r = result(&lost, false)
	r.ReleaseOnly = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &pressure, Constrained: true, Events: &MemoryEventCounts{Max: 1}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	q = memoryFactsRestart(t, q, clock)
	check(t, q, m, nil)
	retained := job(t, q, lost.ID)
	key := *retained.Timing.Key
	configuration := memoryConfiguration(retained.Spec.Needs, retained.Spec.Args, retained.Spec.Program)
	if lower := q.sameOSMemoryLowerBound(key, configuration); lower != 0 {
		t.Fatalf("late release after infrastructure loss raised future exploration: %v", lower)
	}
	facts := job(t, q, lost.ID).MemoryAttempts
	if len(facts) != 1 || facts[0].LearningReason != "infrastructure_interrupted" || facts[0].MemoryPeak.Constrained || facts[0].MemoryPeak.Events.Max != 1 {
		t.Fatalf("late release lost the sample's infrastructure classification: %+v", facts)
	}
	candidate := first
	candidate.State = "queued"
	if got := q.memoryCandidate(&candidate, q.s.Machines[m.ID]).MemoryEnvelope; got.HostGB != 2+minimumHostMemoryGB || got.Basis != "learned" {
		t.Fatalf("loss changed accepted memory: %+v", got)
	}
}
