package myce

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Retained API facts give the applied ceilings and peaks, but not the old
// ledger/drift split. Reconstruct a possible sizing state with the exact first
// ceiling; the resulting two-OOM failure must not depend on that split.
func TestExploratoryOOMIncidentMakesProgress(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, true)
	m.HostAvailableMemoryGB = 22.7
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	machineCheck(t, q, m)
	q.s.HostLaunchMemory[hostID(&m)].RefusalGB = 22.7 - .16655705502260787
	queued := submit(t, q, s)
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != queued.ID || first.MemoryEnvelope.HostGB < 2 {
		t.Fatalf("incident first ceiling: %+v", first)
	}
	// Reconcile an already-issued pre-upgrade execution: its frozen tiny
	// envelope still has to recover when the new queue receives its OOM.
	first.Spec.Needs.HostMemoryGB = .16655705502260787
	first.MemoryEnvelope.HostGB = .16655705502260787
	q.s.Jobs[first.ID].Spec.Needs.HostMemoryGB = first.Spec.Needs.HostMemoryGB
	q.s.Jobs[first.ID].MemoryEnvelope.HostGB = first.MemoryEnvelope.HostGB
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	r := result(first, false)
	peak, growth := .16710281372070312, .05551519613167135
	r.OutOfMemory = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak, GrowthGB: &growth, Constrained: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	retry := assignedJob(machineCheck(t, q, m))
	if retry == nil || retry.ID != first.ID || retry.MemoryEnvelope.HostGB < 2 {
		t.Fatalf("fitting job starved after OOM: state=%s floor=%+v retry=%+v", job(t, q, first.ID).State, job(t, q, first.ID).MemoryOOMFloor, retry)
	}
	if err := q.Started(result(retry, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	r = result(retry, true)
	need := 2.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &need}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, first.ID); got.State != "succeeded" || len(got.MemoryAttempts) != 2 || !strings.Contains(got.MemoryAttempts[1].MemoryEnvelope.Source, "physical headroom") {
		t.Fatalf("missing success/applied-envelope explanation: %+v", got)
	}
}

func TestExploratoryOOMColdGuardDoesNotUseResidualAsLimit(t *testing.T) {
	m := cpuMachine("worker", "host", "cpu", 4, 24)
	m.HostMemoryBudgetGB = 24
	j := Job{Spec: cpuSpec(1, 0), MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory", Enforcement: learnedMemoryGuardEnforcement, HostGB: .16655705502260787, ProbeFloorGB: minimumHostMemoryGB}}
	j.Spec.Needs.Memory = "auto"
	j.Spec.Needs.Platform.Required = []string{"cpu"}
	j.Spec.Needs.HostMemoryGB = j.MemoryEnvelope.HostGB
	w := Worker{Machine: m}
	got, err := w.jobCeilingBytes(j)
	if err != nil || got != 24<<30 {
		t.Fatalf("cold probe starved at residual ceiling: %d %v", got, err)
	}
}

// Contention may require several progressively larger exploratory executions.
// A count alone cannot prove that this physically fitting job is impossible.
func TestExploratoryOOMSeveralBoundsThenSuccess(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, true)
	m.HostAvailableMemoryGB = 1
	machineCheck(t, q, m)
	queued := submit(t, q, s)
	for i, room := range []float64{1, 2, 8} {
		m.HostAvailableMemoryGB = room
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.ID != queued.ID {
			t.Fatalf("attempt %d not assigned: %s", i+1, job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		r := result(run, i == 2)
		peak := run.Spec.Needs.HostMemoryGB
		if i == 2 {
			peak = 4
		}
		growth := .125
		r.OutOfMemory = i < 2
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, GrowthGB: &growth, Constrained: i < 2}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if i < 2 && job(t, q, queued.ID).State != "queued" {
			t.Fatalf("exploratory OOM %d exhausted attempts: %s", i+1, job(t, q, queued.ID).Reason)
		}
	}
	if got := job(t, q, queued.ID); got.State != "succeeded" || got.Attempt != 3 {
		t.Fatalf("state=%s attempt=%d", got.State, got.Attempt)
	}
}

func TestExploratoryOOMLeakReachesPhysicalRefusal(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, true)
	m.HostAvailableMemoryGB = 24
	machineCheck(t, q, m)
	queued := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil {
		t.Fatal("not assigned")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(run, false)
	peak := 24.0
	r.OutOfMemory = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, queued.ID)
	if got.State != "failed" || !strings.Contains(got.Reason, "measured OOM lower bound") || got.MemoryOOMFloor.HostMemoryGB <= 24 {
		t.Fatalf("leak not bounded: %s %s floor=%+v", got.State, got.Reason, got.MemoryOOMFloor)
	}
}

func TestExploratoryOOMColdGuardFinalLaunch(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, true)
	m.HostAvailableMemoryGB = 22.7
	m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
	machineCheck(t, q, m)
	submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil {
		t.Fatal("not assigned")
	}
	actor := &attemptSupervisor{job: *run}
	rt := modelRuntime{clock: clock, machine: m.ID, memoryMachine: m, batchJobs: []Job{*run}, batchSupervisors: map[attemptKey]*attemptSupervisor{jobKey(*run): actor}, hostMemorySource: func(context.Context) (float64, error) { return 22.7, nil }}
	if err := rt.checkBatchLaunchMemory(context.Background(), HostLaunch{Machine: m.ID, Session: run.Session, Key: run.ID, Attempt: "1"}); err != nil {
		t.Fatal("launch charged unexecuted cap", err)
	}
	if err := rt.checkHostAdmissionLocked(22.7, true); err == nil {
		t.Fatal("unknown running neighbor released physical cap")
	}
}

func TestExploratoryOOMExtensionCheckWaitsForPhysicalOpportunity(t *testing.T) {
	q, clock, m, s := coldProbeAdmissionFixture(t, false)
	// Another envelope can own most allowance even while its payload is small.
	queued := submit(t, q, s)
	target := *q.s.Machines[m.ID]
	target.FreeHostMemoryGB = 1.0571327209472656
	target.HostAvailableMemoryGB = 19.742050170898438
	candidate := q.memoryCandidate(queued, &target)
	if candidate.MemoryEnvelope.HostGB < 4 || hostFits(&target, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) {
		t.Fatalf("unknown check used tiny residual: %+v", candidate.MemoryEnvelope)
	}
	run := assignedJob(machineCheck(t, q, m))
	if run == nil {
		t.Fatal("check not assigned after capacity")
	}
	// Retained failed-check evidence: a host memcg OOM is not a device OOM.
	run.Spec.Needs.HostMemoryGB = 1.0571327209472656
	run.MemoryEnvelope.HostGB = run.Spec.Needs.HostMemoryGB
	q.s.Jobs[run.ID].Spec.Needs.HostMemoryGB = run.Spec.Needs.HostMemoryGB
	q.s.Jobs[run.ID].MemoryEnvelope.HostGB = run.MemoryEnvelope.HostGB
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(run, false)
	peak, growth := 1.4061470031738281, .12493726001956092
	r.OutOfMemory = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, GrowthGB: &growth, Constrained: true, Events: &MemoryEventCounts{OOM: 1, OOMKill: 6}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	retry := assignedJob(machineCheck(t, q, m))
	if retry == nil || retry.MemoryEnvelope.HostGB < 4 || retry.Spec.Needs.MemoryGB != run.Spec.Needs.MemoryGB {
		t.Fatalf("host OOM retry starved check or changed device: %+v", retry)
	}
	if err := q.Started(result(retry, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r = result(retry, true)
	need := 4.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &need}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, queued.ID); got.State != "succeeded" {
		t.Fatal(got.State, got.Reason)
	}
}
