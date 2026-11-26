package myce

import (
	"path/filepath"
	"testing"
	"time"
)

func TestMemoryPoolPressureRequiresSustainedDeclineWithHeadroom(t *testing.T) {
	q, clock := fixture(t)
	const total, keep, recovered = 31.206165313720703, 4.0, 23.25
	m := cpuMachine("pressure-replay", "pressure-replay", "cpu", 8, recovered-keep)
	m.Devices = []MachineDevice{}
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	m.HostMemoryBudgetGB = 24
	setReading := func(free float64) {
		m.MemoryObservation = &HostMemoryObservation{TotalGB: total, UsedGB: total - free, ReserveGB: keep}
		m.HostAvailableMemoryGB = free - keep
	}
	setReading(recovered)
	queued := submit(t, q, cpuSpec(1, 8.116))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != queued.ID {
		t.Fatal("fixture did not assign the pressure-test attempt")
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	checkRoom := func(free float64) *Job {
		t.Helper()
		clock.Advance(time.Second)
		setReading(free)
		machineCheck(t, q, m, run)
		return job(t, q, run.ID)
	}
	// Replay the five retained physical headroom readings. The preceding
	// readings were not retained, so supply a repeatable recovery between
	// drops to model a series of isolated losses at the recorded levels.
	for _, free := range []float64{18.27295684814453, 20.666767120361328, 17.45999526977539, 19.596416473388672, 21.033782958984375} {
		for _, reading := range []float64{free, recovered} {
			if got := checkRoom(reading); got.State != "running" || got.Attempt != 1 {
				t.Fatalf("isolated drop at %.3f GiB stopped attempt %d: %s %s", reading, got.Attempt, got.State, got.Reason)
			}
		}
	}
	if got := checkRoom(recovered - 2); got.State != "running" {
		t.Fatal("first interval of a high-room decline stopped the attempt", got.Reason)
	}
	if got := checkRoom(recovered - 4); got.State != "stopping" || got.Reason != memoryPressureCause {
		t.Fatalf("sustained decline failed to trigger a protective stop: %s %s", got.State, got.Reason)
	}
}

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

func TestMemoryPoolPressureProjectsPhysicalLoss(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  float64
		lost bool
	}{
		{name: "growth beyond reservation"},
		{name: "old cgroup sample with unrelated physical loss followed by worker loss", age: 7, lost: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertPressureProjectsPhysicalLoss(t, tc.age, tc.lost)
		})
	}
}

func assertPressureProjectsPhysicalLoss(t *testing.T, age float64, lost bool) {
	t.Helper()
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	m.HostMemoryBudgetGB = 121
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	const total, keep = 121.0, 4.0
	setReading := func(free float64) {
		m.MemoryObservation = &HostMemoryObservation{TotalGB: total, UsedGB: total - free, ReserveGB: keep}
		m.HostAvailableMemoryGB = free - keep
	}
	s := sharedSpec("experiment", 35)
	s.Needs.HostMemoryGB = 25
	queued := submit(t, q, s)
	setReading(73.6)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != queued.ID || run.Memory.RAM.ChargeGB != 60 {
		t.Fatal("expected the canary's 60 GiB charge", run)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	// Model initialization takes minutes. During the final loading burst,
	// two samples show 15 GiB of attributed growth per 8 seconds. Physical
	// room is still 26 GiB at the 43.6 GiB peak. Extrapolating unchanged
	// through the 18s stop horizon predicts 33.75 GiB of loss.
	clock.Advance(3 * time.Minute)
	for i, used := range []float64{13.6, 28.6, 43.6} {
		setReading(73.6 - used)
		c := Checkin{Machine: m, period: 8 * time.Second, AttemptProtocol: keyedAttemptProtocol,
			Attempts: []AttemptReport{{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Session: run.Session, Started: true,
				CurrentHostMemory: &CurrentHostMemory{SampleID: int64(i + 1), NonreclaimableGB: used, AgeSeconds: age}}}}
		if _, err := q.Checkin(c); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			requireState(t, q, run.ID, "running")
		}
		clock.Advance(8 * time.Second)
	}
	got := job(t, q, run.ID)
	if got.State != "stopping" {
		t.Fatalf("still running with 26 GiB above reserve and 33.75 GiB projected loss: %s", got.State)
	}
	stop := priorityStop(got)
	if stop == nil || stop.MemoryPool == nil || stop.MemoryPool.Trigger != "stop_grace" ||
		stop.MemoryPool.GraceLossGB < 33.74 || stop.MemoryPool.GraceLossGB > 33.76 {
		t.Fatalf("stop lost the uncapped comparison: %+v", stop)
	}
	if lost {
		clock.Advance(q.machineLossTimeout(m.ID))
		machineCheck(t, q, cpuMachine("neighbor", "neighbor", "cpu", 8, 96))
	} else {
		finishInterrupted(t, q, run)
	}
	got = job(t, q, run.ID)
	if got.State != "queued" {
		t.Fatalf("pressure victim did not requeue: %s %s", got.State, got.Reason)
	}
	if len(got.AttemptHistory) != 1 || got.AttemptHistory[0].PriorityStop == nil ||
		got.AttemptHistory[0].PriorityStop.MemoryPool == nil || *got.AttemptHistory[0].PriorityStop.MemoryPool != *stop.MemoryPool {
		t.Fatalf("attempt history lost the pressure decision: %+v", got.AttemptHistory)
	}
}

func TestMemoryPoolDeviceStopRecordsComparison(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.Devices = m.Devices[:1]
	s := spec("experiment")
	s.Needs.MemoryGB = 4
	j := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != j.ID {
		t.Fatal("expected assigned attempt", run)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	used := 9.0
	c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol,
		Attempts: []AttemptReport{{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Session: run.Session, Started: true,
			CurrentHostMemory: &CurrentHostMemory{DeviceGB: &used}}}}
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, run.ID)
	stop := priorityStop(got)
	if got.State != "stopping" || stop == nil || stop.DevicePool == nil {
		t.Fatalf("missing device pool stop: %s %+v", got.State, stop)
	}
	if d := stop.DevicePool; d.Trigger != "over_budget" || d.BudgetGB != 8 || d.UsedGB != used || d.OverageGB != used-deviceShareGB(run) {
		t.Fatalf("device pool stop lost the decision-time comparison: %+v", d)
	}
}
