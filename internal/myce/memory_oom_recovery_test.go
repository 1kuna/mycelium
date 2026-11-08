package myce

import (
	"context"
	"testing"
	"time"
)

func TestOOMRecoveryUsesDemandNotFreeRAM(t *testing.T) {
	for _, cpu := range []bool{false, true} {
		t.Run(map[bool]string{false: "model check", true: "production CPU"}[cpu], func(t *testing.T) {
			q, clock, m, s := coldProbeAdmissionFixture(t, cpu)
			if cpu {
				s.Priority, s.Class = "production", "production"
			}
			queued := submit(t, q, s)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != queued.ID {
				t.Fatal("not assigned")
			}
			// Replay the frozen pre-fix offer from the real incident.
			run.Spec.Needs.HostMemoryGB, run.MemoryEnvelope.HostGB = 2.582660675048828, 2.582660675048828
			q.s.Jobs[run.ID].Spec.Needs.HostMemoryGB = run.Spec.Needs.HostMemoryGB
			q.s.Jobs[run.ID].MemoryEnvelope = run.MemoryEnvelope
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			peak, growth := 2.752429962158203, .16203492395862437
			r := result(run, false)
			r.OutOfMemory = true
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak, GrowthGB: &growth, Events: &MemoryEventCounts{OOM: 2, OOMKill: 6}}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			pending := q.s.Jobs[run.ID]
			var bound float64
			for _, free := range []float64{19.52, 8, 23} {
				target := *q.s.Machines[m.ID]
				target.FreeHostMemoryGB, target.HostAvailableMemoryGB = free, free
				candidate := q.memoryCandidate(pending, &target)
				if bound == 0 {
					bound = candidate.Spec.Needs.HostMemoryGB
				}
				if candidate.Spec.Needs.HostMemoryGB != bound || bound <= peak || bound >= 4 {
					t.Fatalf("recovery follows availability: %+v", candidate.MemoryEnvelope)
				}
				w := Worker{Machine: target, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return free - .4, nil }}
				if err := localAttemptAdmission(w, nil, *candidate, nil); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExactLaunchGrowthFitsOwnedEnvelope(t *testing.T) {
	m := cpuMachine("worker", "host", "cpu", 8, 24)
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	j := Job{ID: "job", Attempt: 1, Session: "session", State: "running", Spec: cpuSpec(1, 3)}
	j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB = 0, 3 // Resolved CPU host vector.
	rt := modelRuntime{clock: clock, memoryMachine: m, batchJobs: []Job{j}}
	sample := hostMemorySample{at: clock.Now(), gb: .1, growthGB: .5}
	samples := map[attemptKey]hostMemorySample{jobKey(j): sample}
	need := hostNeed(&m, 0, j.Spec.Needs.HostMemoryGB)
	if err := rt.checkHostAdmissionLocked(need, true, samples, jobKey(j)); err != nil {
		t.Fatal("exact launch demanded more than its envelope", err)
	}
	// A neighbor must retain its unreached reservation. Its growth forecast
	// overlaps that bound, so do not require additive reservation + growth.
	neighborRoom := need - sample.gb - minimumHostMemoryGB
	if err := rt.checkHostAdmissionLocked(neighborRoom, true, samples); err == nil {
		t.Fatal("neighbor's remaining reservation was discounted")
	}
}

func TestRecoveryRefusalKeepsSourceAndLearnsDeficit(t *testing.T) {
	for _, source := range []string{measuredOOMRecoverySource, "exploratory OOM recovery; measured peak + growth; physical headroom"} {
		t.Run(source, func(t *testing.T) {
			q, clock, m, s := coldProbeAdmissionFixture(t, true)
			submit(t, q, s)
			run := assignedJob(machineCheck(t, q, m))
			run.MemoryEnvelope.Source = source
			q.s.Jobs[run.ID].MemoryEnvelope.Source = source
			w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 19, nil }}
			d := admissionDetail(localAttemptAdmission(w, nil, *run, nil), *run)
			if d == nil || d.EnvelopeSource != source {
				t.Fatalf("recovery source stripped: %+v", d)
			}
			r := result(run, false)
			r.Interrupted = true
			r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity", PrelaunchRefused: true, Admission: d}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if q.jobProbeDriftMarginGB(q.s.Jobs[run.ID], q.s.Machines[m.ID]) < .48 {
				t.Fatal("exact recovery deficit did not teach launch drift")
			}
		})
	}
}

func TestOOMRecoveryWithEventOnlyPeak(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, true)
	submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(run, false)
	r.OutOfMemory = true
	r.MemoryPeak = &AttemptMemoryPeak{Events: &MemoryEventCounts{OOM: 1}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	pending := q.s.Jobs[run.ID]
	candidate := q.memoryCandidate(pending, q.s.Machines[m.ID])
	if candidate.Spec.Needs.HostMemoryGB < pending.MemoryOOMFloor.HostMemoryGB {
		t.Fatal("incomplete telemetry erased the durable OOM floor")
	}
}

func TestOOMRecoveryClipsForecastNotMeasuredNeed(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, true)
	submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil {
		t.Fatal("not assigned")
	}
	// A retained small execution can OOM while a delayed sampling horizon
	// forecasts more growth than the physical allowance (#884).
	run.Spec.Needs.HostMemoryGB, run.MemoryEnvelope.HostGB = 3.9, 3.9
	q.s.Jobs[run.ID].Spec.Needs.HostMemoryGB = 3.9
	q.s.Jobs[run.ID].MemoryEnvelope = run.MemoryEnvelope
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	peak, growth := 4.0, 12.0
	r := result(run, false)
	r.OutOfMemory = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, GrowthGB: &growth, Events: &MemoryEventCounts{OOM: 1}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	pending := q.s.Jobs[run.ID]
	target := *q.s.Machines[m.ID]
	profile := *target.CapabilityProfile
	profile.SystemMemoryBytes = 8 << 30
	target.CapabilityProfile = &profile
	target.HostMemoryBudgetGB, target.EffectiveHostMemoryBudgetGB = 8, 8
	target.FreeHostMemoryGB, target.HostAvailableMemoryGB = 8, 8
	candidate := q.memoryCandidate(pending, &target)
	if pending.State != "queued" || candidate.Spec.Needs.HostMemoryGB <= peak || candidate.MemoryEnvelope.HostGB > 8 {
		t.Fatalf("forecast stranded a physically fitting recovery: state=%s envelope=%+v", pending.State, candidate.MemoryEnvelope)
	}
}

func TestOOMRecoveryDoesNotTransferHistoricalDeviceChargeToHost(t *testing.T) {
	q, _, m, s := coldProbeAdmissionFixture(t, false)
	queued := submit(t, q, s)
	peak, growth := 4.0, .25
	queued.OOMRetries = 1
	queued.MemoryOOMFloor = &SizeHint{MemoryGB: 6, HostMemoryGB: 4 + minimumHostMemoryGB}
	queued.MemoryAttempts = []AttemptMemoryFacts{{Attempt: 1, LearningReason: "oom", MemoryPeak: &AttemptMemoryPeak{HostGB: &peak, GrowthGB: &growth}, MemoryEnvelope: &JobMemoryEnvelope{HostGB: 10, DeviceGB: 6}}}
	target := m.Devices[0].machine(m)
	target.HostMemoryBudgetGB, target.EffectiveHostMemoryBudgetGB, target.FreeHostMemoryGB = 8, 8, 8
	candidate := q.memoryCandidate(queued, &target)
	if candidate.Spec.Needs.HostMemoryGB != 4.25 || !hostFits(&target, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) {
		t.Fatalf("historical unified vector inflated discrete host demand: %+v", candidate.MemoryEnvelope)
	}
	queued.MemoryAttempts[0].MemoryEnvelope = nil
	if got, _ := measuredOOMRecovery(queued); got != 4.25 {
		t.Fatalf("missing historical ceiling invented measured overshoot: %g", got)
	}
}
