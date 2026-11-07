package myce

import (
	"strings"
	"testing"
	"time"
)

func cpuDemandFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuBackfillMachine("cpu-demand", 32)
	m.CPUBudgetCores = 4
	m.ExtensionFingerprint, m.CPUExtensionFingerprint = "cpu-runtime", "cpu-runtime"
	machineCheck(t, q, m)
	s := cpuSpec(0, .125)
	s.Needs.Platform = Platform{Required: []string{"cpu"}}
	return q, clock, m, s
}

func cpuDemandStart(t *testing.T, q *Queue, m Machine, s Spec, running ...*Job) *Job {
	t.Helper()
	queued := submit(t, q, s)
	j := assignedJob(machineCheck(t, q, m, running...))
	if j == nil || j.ID != queued.ID {
		t.Fatalf("not assigned: %+v", job(t, q, queued.ID))
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	return j
}

func cpuDemandResult(j *Job, elapsed, used float64) Result {
	r := cleanRepeatedResult(j)
	r.Resources.SampledAt = j.AssignedAt.Add(time.Duration(elapsed * float64(time.Second)))
	r.Resources.ElapsedSeconds = elapsed
	r.Resources.CPU.UsageSeconds = cpuValue(used)
	return r
}

func cpuDemandTeach(t *testing.T, q *Queue, clock *fakeClock, m Machine, s Spec, need float64) *Job {
	t.Helper()
	j := cpuDemandStart(t, q, m, s)
	clock.Advance(time.Second)
	if err := q.Complete(cpuDemandResult(j, 1, need)); err != nil {
		t.Fatal(err)
	}
	return j
}

func TestCPUDemandAcceptedAdmissionAndCleanup(t *testing.T) {
	q, clock, m, s := cpuDemandFixture(t)
	before := preservationSnapshot(t, q)
	first := cpuDemandTeach(t, q, clock, m, s, 1.5)
	after := preservationSnapshot(t, q)
	ledger, err := q.JobLedger(first.ID, 0, -1, 256)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = VerifyCurvePreservation(before, after, []JobLedgerPage{ledger}); err != nil {
		t.Fatal(err)
	}
	a := cpuDemandStart(t, q, m, s)
	b := cpuDemandStart(t, q, m, s, a)
	for _, j := range []*Job{a, b} {
		if j.Spec.Needs.CPUCores != 0 || j.CPUDemand == nil || j.CPUDemand.ChargeCores != 2 || j.CPUDemand.Learned.Samples != 1 || j.CPUDemand.Learned.CoverageSeconds != 1 {
			t.Fatalf("lost demand or caller intent: %+v", j.CPUDemand)
		}
	}
	if q.s.Machines[m.ID].EffectiveCPUCores != 4 || q.s.Machines[m.ID].CPUInUseCores != 4 {
		t.Fatal("physical allowance multiplied")
	}
	samples := q.deviceUsage(cpuMachineView(q.s.Machines[m.ID]))
	for _, sample := range samples {
		if sample.Fair != 2 {
			t.Fatal("fairness undercharged learned cores", sample)
		}
	}
	waiting := submit(t, q, s)
	if next := assignedJob(machineCheck(t, q, m, a, b)); next != nil {
		t.Fatal("overbooked CPU", next.ID)
	}
	clock.Advance(time.Second)
	r := cpuDemandResult(a, 1, 1)
	r.ReleasePending = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if next := assignedJob(machineCheck(t, q, m, b)); next != nil {
		t.Fatal("cleanup released CPU early", next.ID)
	}
	if err := q.Complete(Result{Execution: r.Execution, Machine: r.Machine, Session: r.Session, ReleaseOnly: true}); err != nil {
		t.Fatal(err)
	}
	if next := assignedJob(machineCheck(t, q, m, b)); next == nil || next.ID != waiting.ID {
		t.Fatal("exact cleanup did not release admission")
	}
}

func TestCPUDemandEvidenceQualityAndExplicitAdvice(t *testing.T) {
	q, clock, m, s := cpuDemandFixture(t)
	s.Needs.CPUCores = 4
	first := cpuDemandTeach(t, q, clock, m, s, .5)
	s = first.Spec
	candidate := q.memoryCandidate(&Job{Spec: s, State: "queued"}, cpuMachineView(q.s.Machines[m.ID]))
	cpu := jobResources(candidate)[1]
	if cpu.Used == nil || *cpu.Used != 4 || cpu.Learned == nil || *cpu.Learned != .5 || !strings.Contains(cpu.Note, "owner may lower cpu_cores") {
		t.Fatalf("declaration lowered or advice lost: %+v", cpu)
	}
	// Use another compatible command for uncertain attempts, leaving that queued
	// explicit job out of the execution path.
	s.Run.Command = []string{"echo", "other"}
	for _, kind := range []string{"missing", "throttled", "macos", "stale", "interrupted", "missing timestamp"} {
		t.Run(kind, func(t *testing.T) {
			s.Project = "evidence-" + strings.ReplaceAll(kind, " ", "-")
			j := cpuDemandStart(t, q, m, s)
			clock.Advance(time.Second)
			r := cpuDemandResult(j, 1, 2.5)
			switch kind {
			case "missing timestamp":
				r.Resources.SampledAt = time.Time{}
			case "missing":
				r.Resources = nil
			case "throttled":
				r.Resources.CPU.ThrottledSeconds = cpuValue(.1)
			case "macos":
				r.Resources.CPU.Source = "sampled_process_tree"
				r.Resources.CPU.LowerBound = true
			case "stale":
				r.Resources.AgeSeconds = 2 * attemptMemorySamplePeriod.Seconds()
			case "interrupted":
				r.Success = false
				r.Interrupted = true
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			next := q.cpuDemandFor(&Job{Spec: s}, cpuMachineView(q.s.Machines[m.ID]))
			if next.ChargeCores != 4 {
				t.Fatal("uncertainty lowered explicit floor", next)
			}
			if kind == "throttled" || kind == "macos" {
				if next.Learned == nil || next.Learned.Samples != 0 || next.Learned.LowerBoundSamples != 1 || next.Learned.LowerBoundCores != 2.5 {
					t.Fatalf("uncertain evidence not labelled: %+v", next)
				}
			} else if next.Learned != nil {
				t.Fatal("invalid evidence trained", next)
			}
		})
	}
}

func TestCPUDemandCompatibilityRestartAndOveruse(t *testing.T) {
	q, clock, m, s := cpuDemandFixture(t)
	cpuDemandTeach(t, q, clock, m, s, 1.5)
	cpuDemandTeach(t, q, clock, m, s, 3.2)
	q = memoryFactsRestart(t, q, clock)
	machineCheck(t, q, m)
	next := q.memoryCandidate(submit(t, q, s), cpuMachineView(q.s.Machines[m.ID]))
	if next.CPUDemand == nil || next.CPUDemand.ChargeCores != 4 || next.CPUDemand.Learned.Samples != 2 {
		t.Fatalf("overuse or restart lost: %+v", next.CPUDemand)
	}
	for _, change := range []string{"recipe", "config", "target"} {
		copy := *next
		copy.State = "queued"
		target := cloneMachine(q.s.Machines[m.ID])
		switch change {
		case "recipe":
			copy.Spec.Run.Command = []string{"echo", "changed"}
		case "config":
			copy.Spec.Needs.CPUCores = 1
		case "target":
			target.CPUExtensionFingerprint = "changed"
			target.ExtensionFingerprint = "changed"
			machineCheck(t, q, target)
		}
		demand := q.cpuDemandFor(&copy, cpuMachineView(&target))
		if demand.Learned != nil {
			t.Fatal(change, "inherited unsupported demand", demand)
		}
	}
	// #870's logical-job floor wins even if a compatible learned prior is lower
	// and survives requeue without granting physical capacity.
	next.CPUObservation = &SharedCPUObservation{PeakCores: 5.1}
	shared := m
	shared.SharedHost = &SharedHostProfile{}
	if jobCPUCharge(next, &shared) != 6 || cpuFits(next, &shared) {
		t.Fatal("same-job lower bound or physical budget ignored")
	}
}

func TestCPUDemandTierAndUnknownDoNotReduceCharge(t *testing.T) {
	q, clock, m, s := cpuDemandFixture(t)
	cpuDemandTeach(t, q, clock, m, s, 3.2)
	preemptionPolicy(t, q)
	low := submit(t, q, s)
	urgentSpec := cpuSpec(1, .125)
	urgentSpec.Project = "urgent"
	urgent := submit(t, q, urgentSpec)
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != urgent.ID {
		t.Fatal("CPU learning bypassed higher tier", first)
	}
	if second := assignedJob(machineCheck(t, q, m, first)); second != nil {
		t.Fatal("learned demand created capacity", second.ID)
	}
	finish(t, q, first, true)
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != low.ID || jobCPUCharge(next, &m) != 4 {
		t.Fatal("learned demand lost after higher tier", next)
	}
	if err := q.Started(result(next, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(next, true) // accepted output without counters cannot teach zero.
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	candidate := q.memoryCandidate(&Job{Spec: next.Spec, State: "queued"}, cpuMachineView(q.s.Machines[m.ID]))
	if candidate.CPUDemand.ChargeCores != 4 || candidate.CPUDemand.Learned.Samples != 1 {
		t.Fatal("unknown counters shrank demand", candidate.CPUDemand)
	}
}
