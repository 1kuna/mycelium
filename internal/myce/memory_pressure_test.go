package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMemoryPoolPressureDoesNotCountCopiedCheckpointAsProgress(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "worker", "cpu", 8, 90)
	m.Devices = []MachineDevice{}
	m.HostMemoryBudgetGB = 96
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	s := cpuSpec(1, 60)
	s.Resumable = true
	j := submit(t, q, s)
	if err := os.MkdirAll(j.Checkpoint, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(j.Checkpoint, "save.json"), []byte(`{"myce_checkpoint":{"generation":7}}`), 0600); err != nil {
		t.Fatal(err)
	}
	healthy := func() {
		m.HostAvailableMemoryGB = 90
		m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 31, ReserveGB: 4}
	}
	for attempt := 1; attempt <= 2; attempt++ {
		healthy()
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.Attempt != attempt {
			t.Fatalf("expected attempt %d, got %+v", attempt, run)
		}
		run.Checkpoint = j.Checkpoint // The worker expands the queue's relative offer.
		prepared, err := prepareCheckpointWithGeneration(*run)
		if err != nil {
			t.Fatal(err)
		}
		if generation, err := checkpointProgress(prepared.path); err != nil || generation != 7 || generation != prepared.generation {
			t.Fatalf("expected copied generation 7, got %d, %v", generation, err)
		}
		var start workerStartedRequest
		data, err := json.Marshal(struct {
			Generation int64 `json:"resume_checkpoint_generation"`
		}{prepared.generation})
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &start); err != nil {
			t.Fatal(err)
		}
		start.Result = result(run, true)
		if _, err := q.startedWithCustody(t.Context(), start); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 0, 118
		run.Progress = -1 // Arbitrary NAS writes have no live generation signal.
		machineCheck(t, q, m, run)
		if got := job(t, q, j.ID); got.State != "stopping" || got.Progress != -1 {
			t.Fatalf("expected a pressure stop with unknown live progress, got %s/%d", got.State, got.Progress)
		}
		release := result(run, false)
		release.ReleaseOnly = true
		await(t, func() bool {
			err := q.Complete(release)
			if err != nil && err != errCheckpointIOPending {
				t.Fatal(err)
			}
			return err == nil
		})
		clock.Advance(time.Second)
	}
	healthy()
	if next := assignedJob(machineCheck(t, q, m)); next != nil {
		t.Fatalf("copied checkpoint reopened the same host for attempt %d", next.Attempt)
	}
	if got, err := q.jobSnapshot(j.ID); err != nil || got.Hold == nil || got.Hold.Kind != memoryPressureRetryHold {
		t.Fatalf("expected typed pressure hold after two stops, got %+v, %v", got.Hold, err)
	}
}

func TestMemoryPoolPressureRetryAfterNewCheckpointBeforeStop(t *testing.T) {
	m := cpuMachine("worker", "worker", "cpu", 8, 90)
	stop := &PriorityPreemption{Cause: memoryPressureCause, Demand: "host/" + hostID(&m), PoolPressure: &PoolPressureDecision{Checkpoint: 8}}
	firstResume, secondResume := int64(7), int64(8)
	j := &Job{Spec: Spec{Resumable: true}, State: "queued", Progress: 8, Copies: []Copy{
		{PriorityStop: stop, ResumeCheckpointGeneration: &firstResume},
		{PriorityStop: stop, ResumeCheckpointGeneration: &secondResume},
	}}
	if hold := poolPressureReadmissionHold(j, &m); hold != nil {
		t.Fatalf("first attempt advanced from 7 to 8, one unchanged retry should remain: %+v", hold)
	}
	j.Copies = append(j.Copies, Copy{PriorityStop: stop, ResumeCheckpointGeneration: &secondResume})
	if hold := poolPressureReadmissionHold(j, &m); hold == nil || hold.Kind != memoryPressureRetryHold {
		t.Fatalf("after two stops without advancing past 8, expected pressure hold: %+v", hold)
	}
}

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

func TestMemoryPoolPressureReadmissionAfterExactRelease(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	m.HostMemoryBudgetGB = 96
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	s := sharedSpec("experiment", 35)
	s.Needs.HostMemoryGB = 25
	for i := 1; i <= 2; i++ {
		m.HostAvailableMemoryGB = 90
		m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 31, ReserveGB: 4}
		if i == 1 {
			submit(t, q, s)
		}
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.Attempt != i {
			t.Fatalf("automatic retry %d did not start: %+v", i, run)
		}
		if run.Memory.RAM.ChargeGB != 60 || run.Memory.Card != nil {
			t.Fatalf("unified replay did not carry the same 60 GiB charge: %+v", run.Memory)
		}
		if err := q.Started(result(run, true)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		m.HostAvailableMemoryGB = 0
		m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 118, ReserveGB: 4}
		machineCheck(t, q, m, run)
		got := job(t, q, run.ID)
		if got.State != "stopping" {
			t.Fatalf("pressure did not order a stop: %s", got.State)
		}
		decision := priorityStop(got).PoolPressure
		if decision == nil || decision.Check != "reserve" || decision.FreeGB != 3 || decision.KeepGB != 4 {
			t.Fatalf("stop lost decision-time evidence: %+v", decision)
		}
		release := result(run, false)
		release.ReleaseOnly = true
		if err := q.Complete(release); err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			clock.Advance(time.Second)
			continue
		}
		for _, free := range []float64{94, 116, 95} {
			clock.Advance(time.Second)
			m.HostAvailableMemoryGB = free - 4
			m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 121 - free, ReserveGB: 4}
			if next := assignedJob(machineCheck(t, q, m)); next != nil {
				t.Fatalf("same host started a third attempt: %+v", next)
			}
		}
		got, err := q.jobSnapshot(run.ID)
		if err != nil || got.Hold == nil || got.Hold.Kind != memoryPressureRetryHold || got.Losses != 0 || got.InfrastructureRetries != 0 {
			t.Fatalf("missing typed pressure hold: state=%s hold=%+v err=%v", got.State, got.Hold, err)
		}
		if got.AttemptHistory[1].PriorityStop.PoolPressure.FreeGB != 3 {
			t.Fatal("release-only outcome lost the pressure decision")
		}
		other := sharedUnifiedMachine()
		other.ID, other.HostID, other.Session = "alternate", "alternate", "alternate"
		other.Capabilities = m.Capabilities
		other.MemoryObservation = m.MemoryObservation
		if next := assignedJob(machineCheck(t, q, other)); next == nil || next.Attempt != 3 {
			t.Fatalf("another compatible host did not get the job: %+v", next)
		}
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
