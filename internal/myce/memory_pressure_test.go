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
	} else if stop := priorityStop(got); stop == nil || stop.MemoryPool == nil ||
		stop.MemoryPool.Trigger != "next_sample" || stop.MemoryPool.AvailableGB != 4 || stop.MemoryPool.NextLossGB <= 4 {
		t.Fatalf("pressure stop lost the decision-time comparison: %+v", stop)
	}
	for _, j := range active[:2] {
		if got := job(t, q, j.ID); got.State != "running" {
			t.Fatalf("unrelated %s job stopped: %s", j.Spec.Name, got.Reason)
		}
	}
	r := result(active[2], false)
	r.Interrupted = true
	peak := 6.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, active[2].ID); got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("memory stop spent a retry budget: %s %s", got.State, got.Reason)
	}
	key := pooledEvidenceKey(active[2], &m)
	if got := q.memoryEvidence.estimate(key, 3, initialMemoryConfidence); got.ChargeGB != 6 || got.TypicalGB != 6 || q.memoryEvidence.confidence() != initialMemoryConfidence {
		t.Fatalf("overrun did not keep exactly its attributed floor without calibrating: %+v", got)
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
	}{
		{name: "growth beyond reservation"},
		{name: "old cgroup sample with unrelated physical loss", age: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertPressureProjectsPhysicalLoss(t, tc.age)
		})
	}
}

func assertPressureProjectsPhysicalLoss(t *testing.T, age float64) {
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
	if stop := priorityStop(got); stop == nil || stop.MemoryPool == nil ||
		stop.MemoryPool.Trigger != "stop_grace" || stop.MemoryPool.GraceLossGB < 33.74 || stop.MemoryPool.GraceLossGB > 33.76 {
		t.Fatalf("stop lost the uncapped comparison: %+v", stop)
	}

}

func TestMemoryPoolPressureReleaseLearnsExactPeak(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	m.HostMemoryBudgetGB = 96
	s := sharedSpec("experiment", 35)
	s.Needs.HostMemoryGB = 25
	j := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != j.ID || run.Memory.RAM.ChargeGB != 60 {
		t.Fatal("expected assigned attempt", run)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		q.stopForPoolPressure(q.s.Jobs[j.ID], "host/test", nil)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// A completed stop report with unconfirmed cleanup still owns the attempt.
	pending := result(run, false)
	pending.Interrupted, pending.ReleasePending = true, true
	ambiguous := 90.0
	pending.MemoryPeak = &AttemptMemoryPeak{DeviceGB: &ambiguous}
	generation := int64(7)
	pending.FinalCheckpointGeneration = &generation
	if err := q.Complete(pending); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, j.ID, "stopping")
	if got := job(t, q, j.ID).MemoryAttempts; len(got) != 1 || got[0].LearningReason != "pressure_peak_unattributed" {
		t.Fatalf("incomplete attribution missing from status: %+v", got)
	}
	other := m
	other.ID, other.HostID, other.Session = "other", "other-host", "other-session"
	if next := assignedJob(machineCheck(t, q, other)); next != nil {
		t.Fatal("readmitted on another host before exact release", next)
	}

	key := pooledEvidenceKey(run, allocationMachine(&m, run.AllocatedDevices))
	cgroup, want := 69.0, 79.0
	r := result(run, false)
	r.ReleaseOnly, r.Forced, r.MemoryPeak = true, true, &AttemptMemoryPeak{
		CgroupGB: &cgroup, HostGB: &cgroup, DeviceGB: &want,
		DeviceSampling: &DeviceMemorySampling{Complete: true, OwnedPeakGB: &want},
	}
	stale := r
	stale.Session += "-stale"
	if err := q.Complete(stale); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, j.ID, "stopping")
	if got := q.memoryEvidence.estimate(key, 0, initialMemoryConfidence); got.ChargeGB != 0 {
		t.Fatal("stale release added a peak", got)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, j.ID, "queued")
	if got := job(t, q, j.ID).MemoryAttempts; len(got) != 1 || got[0].LearningReason != "pressure_peak" {
		t.Fatalf("exact pressure peak missing from attempt history: %+v", got)
	}
	if got := q.memoryEvidence.estimate(key, 0, initialMemoryConfidence); got.ChargeGB != want {
		t.Fatalf("exact pressure release lost the %.1f GiB measured floor: %+v", want, got)
	}
	// Restore the durable evidence, then exercise ordinary placement. A
	// lower hint for the same exact program cannot erase the retained peak.
	q = reopenQueue(t, q, clock)
	requireState(t, q, j.ID, "queued")
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB = 70
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 128, UsedGB: 58}
	if next := assignedJob(machineCheck(t, q, m)); next != nil {
		t.Fatalf("readmitted at the old charge in 70 GiB of room: %+v", next.Memory)
	}
	requireState(t, q, j.ID, "queued")
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB = 96
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 128, UsedGB: 32}
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != j.ID || next.Attempt != 2 || next.Memory.RAM.ChargeGB != want || next.Memory.RAM.TypicalGB != want {
		t.Fatalf("recovery did not admit attempt 2 at %.0f GiB: %v, %s", want, next, job(t, q, j.ID).Reason)
	}
}

func TestMemoryPoolPressurePeakPreservesLargerEvidenceFloors(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		previousPeak, oom, limit float64
		want                     float64
	}{
		{name: "exact retained peak", previousPeak: 82, want: 82},
		{name: "OOM floor", previousPeak: 82, oom: 50, want: 100},
		{name: "explicit limit", previousPeak: 82, oom: 50, limit: 110, want: 110},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			m := cpuMachine("pressure-floor", "pressure-floor", "cpu", 8, 96)
			m.HostMemoryBudgetGB = 96
			j := Job{Spec: cpuSpec(1, 60)}
			if tc.limit > 0 {
				j.Spec.MemoryLimitGB = &tc.limit
			}
			key := pooledEvidenceKey(&j, &m)
			q.memoryEvidence.exact[key] = memoryEvidenceSeries{Peaks: []float64{tc.previousPeak}, Lower: tc.oom, PressurePeak: 43.6}
			estimate := q.resolvePooledSize(&j, &m).pools.RAM
			if estimate.ChargeGB != tc.want {
				t.Fatalf("got charge %.3f GiB, want %.3f", estimate.ChargeGB, tc.want)
			}
		})
	}
}

func TestMemoryPoolPressureReleaseIgnoresHostWideLoss(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		owned       float64
	}{
		{name: "bystander", state: "queued", owned: 12},
		{name: "genuinely too large", state: "failed", owned: 105},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertMemoryPoolPressureReleaseIgnoresHostWideLoss(t, tc.owned, tc.state)
		})
	}
}

func assertMemoryPoolPressureReleaseIgnoresHostWideLoss(t *testing.T, owned float64, state string) {
	t.Helper()
	q, _ := fixture(t)
	m := cpuMachine("pressure-attribution", "pressure-attribution", "cpu", 8, 96)
	m.Devices = []MachineDevice{}
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	m.HostMemoryBudgetGB = 96
	j := submit(t, q, cpuSpec(1, 60))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != j.ID {
		t.Fatal("expected assigned attempt", run)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		q.stopForPoolPressure(q.s.Jobs[j.ID], "host/test", nil)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	hostDrop := 70.0
	r := result(run, false)
	r.ReleaseOnly, r.Forced = true, true
	r.MemoryPeak = &AttemptMemoryPeak{CgroupGB: &owned, HostGB: &owned, CombinedHostGB: &hostDrop}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, j.ID, state)
	if state == "failed" {
		got := job(t, q, j.ID)
		if got.DiagnosticCode != "memory_capacity" || got.FinishedAt == nil {
			t.Fatalf("missing typed capacity failure: %s %s", got.DiagnosticCode, got.Reason)
		}
	}
	key := pooledEvidenceKey(run, &m)
	if got := q.memoryEvidence.estimate(key, 0, initialMemoryConfidence); got.ChargeGB != owned {
		t.Fatalf("host-wide loss changed the attributed floor: %+v", got)
	}
}
