package myce

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// The incident's learned physical charge includes 38.732 host + 22.318 device
// + .125 launch slack. Two smaller co-tenants fit the 96 GiB allowance. A helper
// has already consumed device RAM before main Starts; that RAM is also absent
// from the physical availability read and must not remain wholly pending.
func TestUnifiedLaunchMemoryQueueAndWorker(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.CapabilityProfile.SystemMemoryBytes = 127 << 30
	m.CapabilityProfile.LogicalCPUs = 32
	m.HostAvailableMemoryGB = 102 // 118 raw minus the separate 16 GiB reserve.
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 127, UsedGB: 9}
	machineCheck(t, q, m)
	jobs, reports := []Job{}, []AttemptReport{}
	for i, n := range [][3]float64{{38.73198699951172, 22.31842041015625, 22.28}, {9.114835, 9.7861328125, 9.78}, {6.875, 8, 0}} {
		j := Job{ID: fmt.Sprintf("stage-%d", i), Attempt: 1, Session: m.Session, Machine: m.ID, State: "running", Spec: spec("production")}
		j.Spec.Needs.HostMemoryGB, j.Spec.Needs.MemoryGB = n[0], n[1]
		jobs = append(jobs, j)
		device := n[2]
		reports = append(reports, AttemptReport{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session,
			CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 1.22, DeviceGB: &device}})
		if i < 2 {
			q.s.Jobs[j.ID] = &jobs[i]
		}
	}
	samples := currentHostMemorySamples(reports, clock.Now())
	rt := modelRuntime{clock: clock, machine: m.ID, memoryMachine: m, batchJobs: jobs, batchHostMemorySamples: samples,
		batchSupervisors: map[attemptKey]*attemptSupervisor{}, hostMemorySource: func(context.Context) (float64, error) { return 87, nil }}
	for _, j := range jobs {
		rt.batchSupervisors[jobKey(j)] = &attemptSupervisor{job: j, admissionMu: &rt.admissionMu}
	}
	launch := HostLaunch{Machine: m.ID, Session: m.Session, Key: jobs[2].ID, Attempt: "1"}
	q.hostMemorySamples[m.ID] = samples
	q.refreshHostMemory()
	n := jobs[2].Spec.Needs
	if !hostFits(q.s.Machines[m.ID], n.MemoryGB, n.HostMemoryGB) {
		t.Fatal("queue rejected fitting third stage")
	}
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); err != nil {
		t.Fatal("launch disagreed with fitting queue admission", err)
	}
	// No multiplication of the resident credit: overlapping host/device vectors
	// establish only their maximum. This leaves real future host growth booked.
	s := hostMemorySample{gb: 30, at: clock.Now(), deviceGB: 22, deviceAt: clock.Now()}
	if got := remainingAllocationGrowth(&m, 22, 61, s, clock.Now()); got != 31 {
		t.Fatalf("overlapping occupancy credited twice: growth=%g, want 31", got)
	}
	s.gb, s.source = 1, deviceUnifiedResidentSource
	if got := remainingAllocationGrowth(&m, 22, 61, s, clock.Now()); got != 60 {
		t.Fatalf("RSS estimate earned device credit: growth=%g", got)
	}
	// The same reservations must wait when raw availability actually falls.
	m.MemoryObservation.UsedGB = 57 // 70 raw - 31 floor = 39 usable.
	q.s.Machines[m.ID].MemoryObservation = m.MemoryObservation
	q.refreshHostMemory()
	if hostFits(q.s.Machines[m.ID], n.MemoryGB, n.HostMemoryGB) {
		t.Fatal("queue admitted genuinely unfitting growth")
	}
	rt.hostMemorySource = func(context.Context) (float64, error) { return 39, nil }
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); !errors.Is(err, errWorkerMemory) {
		t.Fatal("launch admitted genuinely unfitting growth", err)
	}
	// Stale samples cannot keep spending earlier occupancy. Exact reservations
	// still bind even when the current free-RAM read is generous.
	clock.Advance(deviceMemorySampleFreshFor)
	rt.hostMemorySource = func(context.Context) (float64, error) { return 87, nil }
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); !errors.Is(err, errWorkerMemory) {
		t.Fatal("stale occupancy admitted three full pending envelopes", err)
	}
	rt.memoryMachine.HostMemoryBudgetGB = 80
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); !errors.Is(err, errWorkerMemory) {
		t.Fatal("physical headroom bypassed actual reservations", err)
	}
}

func TestBatchLaunchMemoryUsesFreshActorBeforeAvailability(t *testing.T) {
	rt, actor, _ := probeMemoryRuntimeFixture(t)
	rt.models = nil
	clock := rt.clock.(*fakeClock)
	clock.Advance(hostMemorySampleFreshFor) // Heartbeat map is now stale.
	footprint := 2.0
	actor.report = AttemptReport{Execution: Execution{ID: actor.job.ID, Attempt: actor.job.Attempt}, Session: actor.job.Session,
		CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 2, FootprintGB: &footprint, GrowthObserved: true, sampledAt: clock.Now()}}
	next := Job{ID: "next", Machine: rt.machine, Session: actor.job.Session, Attempt: 1, State: "running", Spec: cpuSpec(1, 4)}
	if err := normalizeResources(&next.Spec); err != nil {
		t.Fatal(err)
	}
	rt.batchJobs = append(rt.batchJobs, next)
	rt.batchSupervisors[jobKey(next)] = &attemptSupervisor{job: next, admissionMu: &rt.admissionMu}
	launch := HostLaunch{Machine: rt.machine, Session: next.Session, Key: next.ID, Attempt: "1"}
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); err != nil {
		t.Fatal("fresh exact helper snapshot hidden by stale heartbeat", err)
	}
	// Invalidate the fresh publication during the blocking availability read.
	// Its replacement must not grant resident credit against that earlier read.
	actor.report.CurrentHostMemory = nil
	rt.hostMemorySource = func(context.Context) (float64, error) {
		actor.mu.Lock()
		actor.report.CurrentHostMemory = &CurrentHostMemory{NonreclaimableGB: 16, FootprintGB: &footprint, GrowthObserved: true, sampledAt: clock.Now()}
		actor.mu.Unlock()
		return 14, nil
	}
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); !errors.Is(err, errWorkerMemory) {
		t.Fatal("newer occupancy was credited against older availability", err)
	}
}

func TestBatchLaunchMemoryConfirmedReleaseFence(t *testing.T) {
	rt, actor, _ := probeMemoryRuntimeFixture(t)
	rt.models = nil
	next := Job{ID: "next", Machine: rt.machine, Session: actor.job.Session, Attempt: 1, State: "running", Spec: cpuSpec(1, 4)}
	if err := normalizeResources(&next.Spec); err != nil {
		t.Fatal(err)
	}
	rt.batchJobs = append(rt.batchJobs, next)
	rt.batchSupervisors[jobKey(next)] = &attemptSupervisor{job: next, admissionMu: &rt.admissionMu}
	actor.stopAdmission()
	launch := HostLaunch{Machine: rt.machine, Session: next.Session, Key: next.ID, Attempt: "1"}
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); !errors.Is(err, errWorkerMemory) {
		t.Fatal("stop intent released the incumbent reservation", err)
	}
	actor.releaseAdmission()
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); err != nil {
		t.Fatal("confirmed release remained charged in heartbeat snapshot", err)
	}
	launch.Key = actor.job.ID
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); !errors.Is(err, errWorkerMemory) {
		t.Fatal("released attempt reacquired launch custody", err)
	}
}

func TestHelperMemoryHeartbeatKeepsMainUnstarted(t *testing.T) {
	_, clock := fixture(t)
	j := Job{ID: "helper", Attempt: 1, Session: "session"}
	sample := &CurrentHostMemory{NonreclaimableGB: 2, sampledAt: clock.Now()}
	r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, CurrentHostMemory: sample}
	a := &attemptSupervisor{job: j, report: r}
	w := Worker{Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 118, nil }}
	reports := []AttemptReport{{Execution: r.Execution, Session: r.Session}}
	m := Machine{}
	w.sampleHeartbeatMemory(t.Context(), &m, nil, reports, map[attemptKey]*attemptSupervisor{jobKey(j): a})
	if reports[0].Started || reports[0].CurrentHostMemory == nil {
		t.Fatal("helper memory was hidden or promoted to main execution", reports[0])
	}
	if s := currentHostMemorySamples(reports, clock.Now())[jobKey(j)]; s.gb != 2 {
		t.Fatal("helper custody did not reach exact-attempt accounting", s)
	}
	clock.Advance(time.Second)
	if sample.AgeSeconds != 0 {
		t.Fatal("heartbeat mutated actor's publication")
	}
}
