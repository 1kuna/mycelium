package myce

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// The incident's learned physical charge includes 38.732 host + 22.318 device
// + .125 launch slack. Two smaller co-tenants fit the 96 GiB allowance. A helper
// has already consumed uniquely charged host RAM before main Starts. Credit
// that occupancy once; device residency remains unknown even with counters.
func TestUnifiedLaunchMemoryQueueAndWorker(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.CapabilityProfile.SystemMemoryBytes = 121 << 30
	m.CapabilityProfile.LogicalCPUs = 32
	m.HostAvailableMemoryGB = 102 // 118 raw minus the separate 16 GiB reserve.
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 3}
	machineCheck(t, q, m)
	jobs, reports := []Job{}, []AttemptReport{}
	for i, n := range [][3]float64{{38.73198699951172, 22.31842041015625, 22.28}, {9.114835, 9.7861328125, 9.78}, {6.875, 8, 0}} {
		j := Job{ID: fmt.Sprintf("stage-%d", i), Attempt: 1, Session: m.Session, Machine: m.ID, State: "running", Spec: spec("production")}
		j.Spec.Needs.HostMemoryGB, j.Spec.Needs.MemoryGB = n[0], n[1]
		jobs = append(jobs, j)
		device, resident := n[2], 1.22
		if i == 2 {
			resident = 0
		} // The queue has not assigned this candidate yet.
		reports = append(reports, AttemptReport{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session,
			CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: resident, DeviceGB: &device}})
		if i < 2 {
			q.s.Jobs[j.ID] = &jobs[i]
		}
	}
	samples := currentHostMemorySamples(reports, clock.Now())
	rt := modelRuntime{clock: clock, machine: m.ID, memoryMachine: m, batchJobs: jobs, batchHostMemorySamples: samples,
		batchSupervisors: map[attemptKey]*attemptSupervisor{}, hostMemorySource: func(context.Context) (float64, error) { return 93, nil }}
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
	// No multiplication of resident credit: device counters do not prove unique
	// physical allocations. Keep future growth even with a fresh direct counter.
	s := hostMemorySample{gb: 30, at: clock.Now(), deviceGB: 22, deviceAt: clock.Now()}
	if got := remainingAllocationGrowth(&m, 22, 61, s, clock.Now()); got != 31 {
		t.Fatalf("overlapping occupancy credited twice: growth=%g, want 31", got)
	}
	s.gb, s.source = 1, deviceUnifiedResidentSource
	if got := remainingAllocationGrowth(&m, 22, 61, s, clock.Now()); got != 60 {
		t.Fatalf("RSS estimate earned device credit: growth=%g", got)
	}
	// The same reservations must wait when raw availability actually falls.
	m.MemoryObservation.UsedGB = 57 // 64 raw - 25 floor = 39 usable.
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
	rt.hostMemorySource = func(context.Context) (float64, error) { return 93, nil }
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
	actor.report = AttemptReport{Execution: Execution{ID: actor.job.ID, Attempt: actor.job.Attempt}, Session: actor.job.Session, Started: true,
		CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 2, FootprintGB: &footprint, GrowthObserved: true, sampledAt: clock.Now()}}
	next := Job{ID: "next", Machine: rt.machine, Session: actor.job.Session, Attempt: 1, State: "running", Spec: cpuSpec(1, 4)}
	if err := normalizeResources(&next.Spec); err != nil {
		t.Fatal(err)
	}
	rt.batchJobs = append(rt.batchJobs, next)
	rt.batchSupervisors[jobKey(next)] = &attemptSupervisor{job: next, admissionMu: &rt.admissionMu}
	launch := HostLaunch{Machine: rt.machine, Session: next.Session, Key: next.ID, Attempt: "1"}
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); err != nil {
		t.Fatal("fresh exact actor snapshot hidden by stale heartbeat", err)
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
	footprint := 2.0
	sample := &CurrentHostMemory{NonreclaimableGB: 2, GrowthObserved: true, FootprintGB: &footprint, sampledAt: clock.Now()}
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
	j.State, j.Spec, j.MemoryEnvelope = "running", cpuSpec(1, 16), &JobMemoryEnvelope{ProbeFloorGB: .125}
	j.Spec.Needs.Memory = "auto"
	if err := normalizeResources(&j.Spec); err != nil {
		t.Fatal(err)
	}
	// Physical occupancy alone cannot stand in for the as-yet-unexecuted main
	// of a cold probe. Keep its full reservation, while crediting the 2 GiB
	// already absent from physical availability exactly once.
	machine := cpuMachine("cpu", "host", "cpu", 4, 24)
	charge, growth := jobHostMemoryCharge(&j, &machine, 16, currentHostMemorySamples(reports, clock.Now())[jobKey(j)], clock.Now())
	if charge != 16 || growth != 14 {
		t.Fatalf("helper released future main demand: charge=%g growth=%g", charge, growth)
	}
	clock.Advance(time.Second)
	if sample.AgeSeconds != 0 {
		t.Fatal("heartbeat mutated actor's publication")
	}
}

func TestHelperMemoryHeartbeatFreezesDeviceBeforeAvailability(t *testing.T) {
	_, clock := fixture(t)
	var device atomic.Pointer[hostMemorySample]
	device.Store(&hostMemorySample{gb: 0, at: clock.Now()})
	j := Job{ID: "helper", Attempt: 1, Session: "session"}
	r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session,
		CurrentHostMemory: &CurrentHostMemory{sampledAt: clock.Now(), deviceSample: &device}}
	a := &attemptSupervisor{job: j, report: r}
	w := Worker{Clock: clock, HostMemorySource: func(context.Context) (float64, error) {
		// Allocated just after the physical read: these newer 20 GiB must not
		// be combined with that earlier 50 GiB availability in the heartbeat.
		clock.Advance(time.Second)
		device.Store(&hostMemorySample{gb: 20, at: clock.Now()})
		return 50, nil
	}}
	reports := []AttemptReport{r}
	w.sampleHeartbeatMemory(t.Context(), &Machine{}, nil, reports, map[attemptKey]*attemptSupervisor{jobKey(j): a})
	s := reports[0].CurrentHostMemory
	if s == nil || s.DeviceGB == nil || *s.DeviceGB != 0 || s.DeviceAgeSeconds != 1 || s.deviceSample != nil {
		t.Fatalf("newer allocation counter combined with older availability: %+v", s)
	}
	if current := device.Load(); current.gb != 20 {
		t.Fatal("fixture did not publish the intervening allocation")
	}
}

func TestUnifiedLaunchMemoryDoesNotCreditSummedDeviceCounters(t *testing.T) {
	_, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	// Two process rows may map the same 11 GiB allocation. A sum of 22 GiB
	// cannot prove 22 GiB uniquely resident and spend 11 GiB of phantom room.
	s := hostMemorySample{at: clock.Now(), deviceAt: clock.Now(), deviceGB: 22}
	if growth := remainingAllocationGrowth(&m, 22, 61, s, clock.Now()); growth != 61 {
		t.Fatalf("unqualified device sum earned physical residency credit: %g", growth)
	}
}
