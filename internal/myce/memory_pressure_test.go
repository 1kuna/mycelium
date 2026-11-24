package myce

import (
	"path/filepath"
	"testing"
	"time"
)

func TestMemoryPoolShortageStopsLowestTierOverrun(t *testing.T) {
	q, clock := fixture(t)
	m := machine("pooled-safety", "cpu")
	m.Devices = []MachineDevice{}
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 24, 24
	machineCheck(t, q, m)
	var active []*Job
	for _, name := range []string{"production", "innocent", "overrun"} {
		s := cpuSpec(1, 3)
		s.Name = name
		if name == "production" {
			s.Priority, s.Class = "production", "production"
		}
		queued := submit(t, q, s)
		run := assignedJob(machineCheck(t, q, m, active...))
		if run == nil || run.ID != queued.ID {
			t.Fatalf("%s was not admitted: %s", name, job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(run, true)); err != nil {
			t.Fatal(err)
		}
		active = append(active, run)
	}
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB = 4
	c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
	for i, j := range active {
		gb := []float64{3, 2, 6}[i]
		c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true,
			CurrentHostMemory: &CurrentHostMemory{SampleID: 1, NonreclaimableGB: gb, FootprintGB: &gb}})
	}
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, active[2].ID); got.State != "stopping" || got.Reason != memoryPressureCause {
		t.Fatalf("overrun was not selected: %s %s", got.State, got.Reason)
	}
	for _, j := range active[:2] {
		if got := job(t, q, j.ID); got.State != "running" {
			t.Fatalf("unrelated %s job stopped: %s", j.Spec.Name, got.Reason)
		}
	}
	r := result(active[2], false)
	r.Interrupted = true
	peak := 6.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, active[2].ID); got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("memory stop spent a retry budget: %s %s", got.State, got.Reason)
	}
	key := pooledEvidenceKey(active[2], &m)
	if got := q.memoryEvidence.estimate(key, 3, initialMemoryConfidence); got.ChargeGB != 12 || got.TypicalGB != 6 || q.memoryEvidence.confidence() <= initialMemoryConfidence {
		t.Fatalf("overrun did not double its floor and tighten the confidence target: %+v", got)
	}
}

func TestMemoryPoolKernelBackstopAndImpossibleRetry(t *testing.T) {
	q, clock := fixture(t)
	m := machine("pooled-cpu", "cpu")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 8, 8
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	machineCheck(t, q, m)
	s := cpuSpec(1, 2)
	queued := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != queued.ID || run.Memory == nil {
		t.Fatal("pooled job was not assigned")
	}
	w := Worker{Clock: clock}
	bytes, err := w.jobCeilingBytes(*run)
	if err != nil {
		t.Fatal(err)
	}
	a, err := fakeCeilings(t).createAttempt("slice-casualty", bytes, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.fs.WriteFile(filepath.Join(a.path, "memory.events"), []byte("oom 0\noom_kill 1\n")); err != nil {
		t.Fatal(err)
	}
	if err := a.failure(); err != errMemoryPoolExhausted {
		t.Fatalf("slice OOM treated as a job ceiling: %v", err)
	}
	r := result(run, false)
	if err := q.Started(r); err != nil {
		t.Fatal(err)
	}
	peak := 5.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Events: &MemoryEventCounts{OOMKill: 1}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, run.ID); got.State != "failed" || got.DiagnosticCode != "memory_capacity" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("impossible retry lacked typed capacity failure: %s %s", got.State, got.Reason)
	}
}
