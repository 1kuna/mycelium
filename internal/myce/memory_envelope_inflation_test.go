package myce

import (
	"strings"
	"testing"
	"time"
)

func TestMemoryEnvelopeGenuineConstraintEscalationBounded(t *testing.T) {
	for _, ending := range []string{"constrained completion", "OOM racing interruption"} {
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
			if ending == "OOM racing interruption" {
				r.OutOfMemory, r.Interrupted = true, true
				r.ExecutionCause = &RuntimeCause{Operation: "teardown", Class: "io_error"}
			}

			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			candidate := first
			candidate.State = "queued"
			got := q.memoryCandidate(&candidate, q.s.Machines[m.ID]).MemoryEnvelope
			if got.HostGB != 2+2*minimumHostMemoryGB || got.Basis != "exploratory" || !strings.Contains(got.Source, "accepted prior invalidated") {
				t.Fatalf("genuine failure lost measured exploration bound: %+v", got)
			}
			retryState := job(t, q, run.ID)
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
	for i := 0; i < 1; i++ {
		run := start()
		clock.Advance(time.Second)
		r := result(&run, false)
		pressure := 18.7
		r.Interrupted = true
		r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "io_error"}
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &pressure, Constrained: true, Events: &MemoryEventCounts{High: 1}}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		facts := job(t, q, run.ID).MemoryAttempts
		if len(facts) != 1 {
			t.Fatal("missing diagnostic facts")
		}
		f := facts[0]
		if f.LearningReason != "infrastructure_interrupted" || f.MemoryPeak.Constrained || f.MemoryPeak.Events.High != 1 || *f.MemoryPeak.HostGB != pressure {
			t.Fatalf("interruption recorded constrained learning or lost raw counters: %+v", f)
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
	facts := job(t, q, lost.ID).MemoryAttempts
	if len(facts) != 1 || facts[0].LearningReason != "infrastructure_interrupted" || facts[0].MemoryPeak.Constrained || facts[0].MemoryPeak.Events.Max != 1 {
		t.Fatalf("late release lost the sample's infrastructure classification: %+v", facts)
	}
	next := startObservation(t, q, m, s)
	if got := next.MemoryEnvelope; got.HostGB != 2+minimumHostMemoryGB || got.Basis != "learned" {
		t.Fatalf("loss changed accepted memory: %+v", got)
	}
}
