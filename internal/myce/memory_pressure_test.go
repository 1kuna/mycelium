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

func TestMemoryPoolReservedModelLoadDoesNotRestart(t *testing.T) {
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
				CurrentHostMemory: &CurrentHostMemory{SampleID: int64(i + 1), NonreclaimableGB: used}}}}
		if _, err := q.Checkin(c); err != nil {
			t.Fatal(err)
		}
		if got := job(t, q, run.ID); got.State != "running" || got.Attempt != 1 {
			t.Fatalf("load at %.1f GiB within its 60 GiB charge interrupted attempt %d: %s", used, got.Attempt, got.Reason)
		}
		clock.Advance(8 * time.Second)
	}
}

func TestMemoryPoolPressureReleaseLearnsExactPeak(t *testing.T) {
	for _, name := range []string{"copied generation", "missing final generation"} {
		t.Run(name, func(t *testing.T) {
			var generation *int64
			if name == "copied generation" {
				var copied int64 = 7
				generation = &copied
			}
			assertPoolPressureReleaseLearnsExactPeak(t, generation)
		})
	}
}

func assertPoolPressureReleaseLearnsExactPeak(t *testing.T, generation *int64) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuMachine("pressure-learning", "pressure-learning", "cpu", 8, 96)
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

	// A completed stop report with unconfirmed cleanup still owns the attempt.
	pending := result(run, false)
	pending.Interrupted, pending.ReleasePending = true, true
	pending.FinalCheckpointGeneration = generation
	if err := q.Complete(pending); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, j.ID, "stopping")
	other := cpuMachine("other", "other", "cpu", 8, 96)
	other.Devices = []MachineDevice{}
	if next := assignedJob(machineCheck(t, q, other)); next != nil {
		t.Fatal("readmitted on another host before exact release", next)
	}

	key := pooledEvidenceKey(run, &m)
	want := 43.6
	r := result(run, false)
	r.ReleaseOnly, r.Forced, r.MemoryPeak = true, true, &AttemptMemoryPeak{HostGB: &want}
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
	if got := q.memoryEvidence.estimate(key, 0, initialMemoryConfidence); got.ChargeGB < want {
		t.Fatalf("exact pressure release lost the %.1f GiB measured floor: %+v", want, got)
	}
	// Restore the durable evidence, then exercise ordinary placement. A
	// lower hint for the same exact program cannot erase the retained peak.
	q = reopenQueue(t, q, clock)
	requireState(t, q, j.ID, "queued")
	if got := q.resolvePooledSize(&Job{Spec: cpuSpec(1, 8)}, &m).pools.RAM; got.ChargeGB != want {
		t.Fatalf("next admission did not use the retained peak: %+v", got)
	}
	clock.Advance(time.Second)
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != j.ID || next.Attempt != 2 || next.Memory.RAM.ChargeGB < want {
		t.Fatal("exact release did not permit a new admission with the retained floor", next)
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
