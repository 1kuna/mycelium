package myce

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

const replayHostCeilingGB = 561369088.0 / (1 << 30)
const replayCheckChargeGB = 16.420490264892578

func startReplayCheck(t *testing.T, q *Queue, m Machine, s Spec, device string, running ...*Job) *Job {
	t.Helper()
	s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
	s.Workload = nil
	s.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
	s.AllowedMachines = []string{m.ID}
	s.RequireTarget = true
	s.Needs.GPU = &GPUSelection{Devices: []string{device}}
	s.SizeHint = SizeHint{HostMemoryGB: replayCheckChargeGB}
	pending := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m, running...))
	if run == nil || run.ID != pending.ID || run.MemoryEnvelope.HostGB != replayCheckChargeGB {
		t.Fatalf("check envelope: run=%+v status=%+v", run, job(t, q, pending.ID))
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	return run
}

func completeReplayCheck(t *testing.T, q *Queue, clock *fakeClock, run *Job, peak float64) {
	t.Helper()
	clock.Advance(time.Second)
	r := result(run, true)
	r.MemoryPeak = &AttemptMemoryPeak{CgroupGB: &peak, Events: &MemoryEventCounts{Max: 1}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	facts := job(t, q, run.ID).MemoryAttempts
	if len(facts) != 1 || facts[0].LearningReason != "constrained" {
		t.Fatalf("check supplied accepted memory evidence: %+v", facts)
	}
}

func TestCensoredAcceleratorOOMReplaysMeasuredRecovery(t *testing.T) {
	for _, tc := range []string{"constrained check", "no check peak", "physical insufficiency"} {
		t.Run(tc, func(t *testing.T) {
			q, clock, m, s := gpuHostFixture(t)
			checkPeak, growth := 9.054, .1078
			if tc == "constrained check" {
				completeReplayCheck(t, q, clock, startReplayCheck(t, q, m, s, "0"), checkPeak)
			}
			// Reissue the historical tiny ceiling before introducing the
			// co-tenant. The cold gate now refuses that offer under contention.
			m.HostAvailableMemoryGB = replayHostCeilingGB + minimumHostMemoryGB
			machineCheck(t, q, m)
			work := s
			work.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
			run := assignGPU(t, q, m, submit(t, q, work))
			if run.MemoryEnvelope.HostGB != replayHostCeilingGB {
				t.Fatalf("historical ceiling = %.12f GiB", run.MemoryEnvelope.HostGB)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			m.HostAvailableMemoryGB += replayCheckChargeGB
			holder := startReplayCheck(t, q, m, s, "1", run)
			if tc == "physical insufficiency" {
				profile := *m.CapabilityProfile
				profile.SystemMemoryBytes = uint64(replayHostCeilingGB * (1 << 30))
				m.CapabilityProfile = &profile
				machineCheck(t, q, m, run, holder)
			}
			oom := func(run *Job, peak float64) {
				t.Helper()
				clock.Advance(time.Second)
				r := result(run, false)
				r.OutOfMemory = true
				r.MemoryPeak = &AttemptMemoryPeak{CgroupGB: &peak, GrowthGB: &growth, Events: &MemoryEventCounts{OOM: 1, OOMKill: 3, OOMGroupKill: 1}}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			oom(run, replayHostCeilingGB)
			pending := job(t, q, run.ID)
			if tc == "physical insufficiency" {
				if pending.State != "failed" || !strings.Contains(pending.Reason, "measured OOM lower bound") {
					t.Fatalf("known physical limit did not fail recovery: %s (%s)", pending.State, pending.Reason)
				}
				return
			}
			// With unchanged live availability, more than 16 GiB returns
			// through the actual host ledger only after the check releases.
			if retry := assignedJob(machineCheck(t, q, m, holder)); retry != nil || pending.State != "queued" || pending.OOMRetries != 1 {
				t.Fatalf("retry crossed the occupied host ledger: run=%+v state=%s (%s)", retry, pending.State, pending.Reason)
			}
			if got := job(t, q, holder.ID); got.State != "running" {
				t.Fatalf("recovery interrupted the check: %s (%s)", got.State, got.Reason)
			}
			facts := pending.MemoryAttempts[0]
			inputs := facts.MemoryEnvelope.SizingInputs
			if facts.LearningReason != "oom" || inputs == nil || inputs.LedgerRemainderGB-inputs.DriftGB != replayHostCeilingGB || inputs.AllowanceGB != 24 {
				t.Fatalf("attempt lost original sizing: %+v", inputs)
			}
			if err := q.Complete(result(holder, true)); err != nil {
				t.Fatal(err)
			}
			retry := assignGPU(t, q, m, pending)
			e := retry.MemoryEnvelope
			want, check := m.HostAvailableMemoryGB, 0.0
			if tc == "constrained check" {
				want, check = checkPeak+growth, checkPeak
			}
			if !strings.Contains(e.Source, censoredHostOOMRecoverySource) || e.Evidence != 0 ||
				retry.Spec.Needs.MemoryGB != run.Spec.Needs.MemoryGB || math.Abs(e.HostGB-want) > 1e-9 ||
				e.SizingInputs == nil || e.SizingInputs.ConstrainedCheckHostGB != check {
				t.Fatalf("retry envelope = %+v, want %.6f GiB; sizing = %+v", e, want, e.SizingInputs)
			}
			if err := q.Started(result(retry, false)); err != nil {
				t.Fatal(err)
			}
			oom(retry, e.HostGB)
			if got := job(t, q, run.ID); got.State != "failed" || got.OOMRetries != 1 {
				t.Fatalf("retry budget changed: %s (%s), retries=%d", got.State, got.Reason, got.OOMRetries)
			}
		})
	}
}

func TestColdAcceleratorWaitsForOccupiedHostRAM(t *testing.T) {
	q, _, m, s := gpuHostFixture(t)
	m.HostAvailableMemoryGB = replayCheckChargeGB + replayHostCeilingGB + minimumHostMemoryGB
	holder := startReplayCheck(t, q, m, s, "1")
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	pending := submit(t, q, s)
	if run := assignedJob(machineCheck(t, q, m, holder)); run != nil {
		t.Fatalf("cold job got contention-limited ceiling: %+v", run.MemoryEnvelope)
	}
	if got := job(t, q, pending.ID); !strings.Contains(got.Reason, "waiting for occupied host RAM") {
		t.Fatalf("missing contention hold: %+v (%s)", got.Hold, got.Reason)
	}
	if err := q.Complete(result(holder, true)); err != nil {
		t.Fatal(err)
	}
	run := assignGPU(t, q, m, pending)
	if run.MemoryEnvelope.HostGB != m.HostAvailableMemoryGB-minimumHostMemoryGB {
		t.Fatalf("released RAM did not size the cold offer: %+v", run.MemoryEnvelope)
	}
}

func TestCensoredHostOOMRequiresMatchingCeilingAndAttempt(t *testing.T) {
	ceiling := replayHostCeilingGB
	j := Job{Attempt: 2, Machine: "worker", Session: "session",
		MemoryEnvelope: &JobMemoryEnvelope{HostGB: ceiling, Enforcement: cgroupMemoryEnforcement},
		MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, Machine: "worker", Session: "session",
			MemoryPeak: &AttemptMemoryPeak{CgroupGB: &ceiling, Events: &MemoryEventCounts{OOM: 1}}}}}
	if censoredHostOOMPeak(&j) != nil {
		t.Fatal("stale OOM evidence selected recovery")
	}
	j.MemoryAttempts[0].Attempt = j.Attempt
	peak := j.MemoryAttempts[0].MemoryPeak
	below := ceiling - 2*minimumHostMemoryGB
	peak.CgroupGB = &below
	if censoredHostOOMPeak(&j) != nil {
		t.Fatal("uncensored peak selected ceiling recovery")
	}
	rounded := ceiling - minimumHostMemoryGB/2
	peak.CgroupGB = &rounded
	if censoredHostOOMPeak(&j) != peak {
		t.Fatal("kernel page rounding hid host ceiling evidence")
	}
	above := ceiling + 2*minimumHostMemoryGB
	peak.CgroupGB = &above
	if censoredHostOOMPeak(&j) != peak {
		t.Fatal("overshoot hid host ceiling evidence")
	}
	peak.Events = &MemoryEventCounts{Max: 1}
	if censoredHostOOMPeak(&j) != nil {
		t.Fatal("ceiling pressure without kernel OOM selected recovery")
	}
}

func TestOOMRecoveryUsesDemandNotFreeRAM(t *testing.T) {
	for _, cpu := range []bool{false, true} {
		t.Run(map[bool]string{false: "model check", true: "production CPU"}[cpu], func(t *testing.T) {
			q, clock, m, s := coldProbeAdmissionFixture(t, cpu)
			if cpu {
				s.Priority, s.Class = "production", "production"
				// The legacy Linux route can offer a shell-sized CPU first
				// run only when the worker advertises the adaptive guard.
				m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
			}
			queued := submit(t, q, s)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != queued.ID {
				t.Fatal("not assigned")
			}
			// Replay the frozen pre-fix offer from the real incident.
			run.Spec.Needs.HostMemoryGB, run.MemoryEnvelope.HostGB = 2.582660675048828, 2.582660675048828
			if err := q.update(func() error {
				j := q.touchJob(run.ID)
				j.Spec.Needs.HostMemoryGB = run.Spec.Needs.HostMemoryGB
				j.MemoryEnvelope = run.MemoryEnvelope
				return nil
			}); err != nil {
				t.Fatal(err)
			}
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
			q.mu.Lock()
			defer q.mu.Unlock()
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

func TestRecoveryRefusalKeepsSourceAndDemand(t *testing.T) {
	for _, source := range []string{measuredOOMRecoverySource, "exploratory OOM recovery; measured peak + growth; physical headroom"} {
		t.Run(source, func(t *testing.T) {
			q, clock, m, s := coldProbeAdmissionFixture(t, true)
			submit(t, q, s)
			run := assignedJob(machineCheck(t, q, m))
			run.MemoryEnvelope.Source = source
			run.Spec.Needs.HostMemoryGB, run.MemoryEnvelope.HostGB = 19.49, 19.49
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
			if got := job(t, q, run.ID); got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("prelaunch recovery refusal spent a failure budget: %+v", got)
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
