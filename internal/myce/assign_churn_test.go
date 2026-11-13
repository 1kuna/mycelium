package myce

import (
	"context"
	"errors"
	"math"
	"net/http/httptest"
	"testing"
	"time"
)

// #1565: the queue has accepted Started while the worker still owns the
// original assignment. Replay the retained envelope/credit and comparison;
// the sensor sequence before that comparison was not retained.
func TestAssignChurnRetainedUnifiedMemory(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	m.MemoryEnforcement = cgroupMemoryEnforcement
	m.MemoryCeilingSlackGB = .125
	m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = 88.79421997070312, 113.85746002197266
	m.Devices[0].ReserveMemoryGB = 4
	m.Devices[0].BudgetGB, m.Devices[0].FreeMemoryGB = 96, 96
	machineCheck(t, q, m)
	co := Job{ID: "co-tenant", Machine: m.ID, Session: m.Session, Attempt: 2, State: "running", AllocatedDevices: []string{"0"}, Spec: spec("production")}
	co.Spec.Needs.Memory, co.Spec.Needs.HostMemoryGB, co.Spec.Needs.MemoryGB = "auto", 38.73198699951172, 44.519424227328635
	co.MemoryEnvelope = &JobMemoryEnvelope{HostGB: 83.37641122684036, DeviceGB: co.Spec.Needs.MemoryGB, Basis: "learned", Learned: &LearnedMemory{Evidence: 184, HostGB: co.Spec.Needs.HostMemoryGB, PeakHostGB: 20.391857147216797, PeakDeviceGB: 22.318359375}}
	other := Job{ID: "other", Machine: m.ID, Session: m.Session, Attempt: 13, State: "running", Spec: cpuSpec(1, 6)}
	if err := normalizeResources(&other.Spec); err != nil {
		t.Fatal(err)
	}
	for _, j := range []*Job{&co, &other} {
		copy := *j
		now := clock.Now()
		copy.StartedAt = &now
		q.s.Jobs[j.ID] = &copy
	}
	device, footprint := 22.318359375, .329437255859375
	reports := []AttemptReport{
		{Execution: Execution{ID: co.ID, Attempt: co.Attempt}, Session: co.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: footprint, FootprintGB: &footprint, GrowthObserved: true, DeviceGB: &device, AgeSeconds: .04776011}},
		{Execution: Execution{ID: other.ID, Attempt: other.Attempt}, Session: other.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: .9172134399414062, AgeSeconds: .075639487}},
	}
	next := submit(t, q, cpuSpec(2, 10))
	reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports})
	if err != nil {
		t.Fatal(err)
	}
	offer := assignedJob(reply)
	if offer == nil || offer.ID != next.ID {
		t.Fatal("fitting consumer was not assigned", job(t, q, next.ID).Reason)
	}
	samples := currentHostMemorySamples(reports, clock.Now())
	// The assignment has no StartedAt. Without the accepted report, its full
	// envelope reproduces the historical refusing worker comparison exactly.
	helper := reports[0]
	helper.Started = false
	unstarted := currentHostMemorySamples([]AttemptReport{helper}, clock.Now())[jobKey(co)]
	_, pending := memoryCharge(&co, allocationMachine(&m, co.AllocatedDevices), co.MemoryEnvelope.HostGB, unstarted, clock.Now())
	if math.Abs(pending-83.04697397098099) > 1e-9 {
		t.Fatalf("retained pending growth changed: %g", pending)
	}
	rt := modelRuntime{clock: clock, machine: m.ID, memoryMachine: m, batchJobs: []Job{co, other, *offer}, batchHostMemorySamples: samples,
		hostMemorySource: func(context.Context) (float64, error) { return m.HostAvailableMemoryGB, nil }}
	rt.batchSupervisors = map[attemptKey]*attemptSupervisor{}
	for _, j := range rt.batchJobs {
		rt.batchSupervisors[jobKey(j)] = &attemptSupervisor{job: j, admissionMu: &rt.admissionMu}
	}
	launch := HostLaunch{Machine: m.ID, Session: offer.Session, Key: offer.ID, Attempt: "1"}
	if err := rt.checkBatchLaunchMemory(t.Context(), launch); err != nil {
		d := admissionDetail(err, *offer)
		if d != nil && (d.Subcheck != "live_host_growth" || math.Abs(d.Required-98.37976053103958) > 1e-9) {
			t.Fatalf("unexpected comparison: %+v", d)
		}
		t.Fatal("placement and prelaunch disagree on identical inputs:", err)
	}
	// Initial local admission and the final exec gate must use the same rule.
	w := Worker{Machine: m, Clock: clock, hostMemorySamples: samples, HostMemorySource: rt.hostMemorySource}
	if err := localAttemptAdmission(w, []Job{co, other}, *offer, nil); err != nil {
		t.Fatal("local admission disagreed:", err)
	}
	charge, growth := memoryCharge(&co, allocationMachine(&m, co.AllocatedDevices), co.MemoryEnvelope.HostGB, samples[jobKey(co)], clock.Now())
	queueCharge, queueGrowth := q.hostJobChargeGrowth(q.s.Jobs[co.ID])
	if charge != queueCharge || growth != queueGrowth {
		t.Fatalf("different co-tenant figures: worker %g/%g queue %g/%g", charge, growth, queueCharge, queueGrowth)
	}
}

// A positive preflight refusal must reach the real queue immediately, without
// waiting for an experiment's start window or a simulated hour to elapse.
func TestAssignChurnPreflightRefusalReturnsPromptly(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 16)
	next := submit(t, q, cpuSpec(1, 4.125))
	offer := assignedJob(machineCheck(t, q, m))
	if offer == nil || offer.ID != next.ID {
		t.Fatal("missing initial assignment")
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := recoveryWorker(t)
	w.Clock, w.Machine, w.Client = clock, m, Client{URL: server.URL, Token: "test"}
	probe := syntheticProfileProbe().Machine
	w.cpuRuntimeProbe, w.control = &probe, &processControl{clock: clock}
	w.HostMemorySource = func(context.Context) (float64, error) { return 3.2438201904296875, nil }
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	root.state.Session = offer.Session
	store, err := newAttemptStore(root, *offer)
	if err != nil {
		t.Fatal(err)
	}
	a := &attemptSupervisor{store: store, job: *offer, worker: w, connected: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go a.run(ctx)
	select {
	case <-a.done:
	case <-time.After(testWaitTimeout):
		cancel()
		<-a.done
		t.Fatal("refused assignment was retained")
	}
	got := job(t, q, next.ID)
	if got.State != "queued" || got.Losses != 0 || !got.Copies[0].ReleaseConfirmed || got.Copies[0].ReleasedAt == nil || !clock.Now().Equal(*got.Copies[0].ReleasedAt) {
		t.Fatalf("refusal did not promptly release and requeue: %+v", got)
	}
	if len(got.LaunchCapacityWait) == 0 || got.Attempt != 1 {
		t.Fatal("refusal lost its typed wait or changed attempt", got.Attempt)
	}
}

// The retained incident has the worker comparison but not its matching queue
// snapshot. Replay both numbers without attributing the difference to a sensor,
// transport delay or changing reservations that the evidence cannot distinguish.
func TestAssignChurnPrelaunchGrowthBoundary(t *testing.T) {
	for _, pair := range [][2]float64{{17.106637954711914, 17.005722045898438}, {16.86988639831543, 16.814495086669922}} {
		q, clock := fixture(t)
		m := cpuMachine("worker", "host", "cpu", 16, 32)
		m.Devices = []MachineDevice{}
		holder := submit(t, q, cpuSpec(4, pair[0]-4))
		running := assignedJob(machineCheck(t, q, m))
		if running == nil || running.ID != holder.ID {
			t.Fatal("missing holder")
		}
		next := submit(t, q, cpuSpec(1, 4))
		m.HostAvailableMemoryGB = pair[0]
		report := AttemptReport{Execution: Execution{ID: running.ID, Attempt: running.Attempt}, Session: running.Session, Started: true}
		r, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}})
		if err != nil || assignedJob(r) == nil || assignedJob(r).ID != next.ID {
			t.Fatalf("boundary assignment missing: %v", err)
		}
		w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return pair[0], nil }}
		if err := localAttemptAdmission(w, []Job{*running}, *assignedJob(r), nil); err != nil {
			t.Fatalf("identical inputs disagree (own charge must occur once): %v", err)
		}
		clock.Advance(2 * time.Second)
		w.HostMemorySource = func(context.Context) (float64, error) { return pair[1], nil }
		err = localAttemptAdmission(w, []Job{*running}, *assignedJob(r), nil)
		var refusal *admissionError
		if !errors.As(err, &refusal) || refusal.detail.Subcheck != "live_host_growth" || refusal.detail.Required != pair[0] || *refusal.detail.Available != pair[1] {
			t.Fatalf("did not replay retained comparison: %v", err)
		}
	}
}

func assignChurnWaveFixture(t *testing.T, ram float64) (*Queue, *workerFakeClock, Machine, Spec) {
	t.Helper()
	q, clock, m, wave := fanOutFixture(t, 1)
	m.Devices = []MachineDevice{}
	m.CPUExtensionFingerprint = m.ExtensionFingerprint
	check(t, q, m, nil)
	wave.Priority, wave.SizeHint.HostMemoryGB = "production", ram
	return q, clock, m, wave
}

func TestAssignChurnPendingProductionWave(t *testing.T) {
	q, clock, m, wave := assignChurnWaveFixture(t, 32)
	h := cpuSpec(2, 16)
	h.Priority = "production"
	holder := submit(t, q, h)
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.ID != holder.ID {
		t.Fatal("missing production holder", running)
	}
	demand := submit(t, q, wave)
	clock.Advance(time.Second)
	lower := submit(t, q, cpuSpec(1, 4))
	if got := assignedJob(machineCheck(t, q, m, running)); got != nil {
		t.Fatalf("started %s while pending wave %s will need its RAM (consumer %s)", got.ID, demand.ID, lower.ID)
	}
	finish(t, q, running, true)
	got := assignedJob(machineCheck(t, q, m))
	if got == nil || got.Batch == nil || got.Batch.Parent != demand.ID {
		t.Fatal("production did not start promptly after capacity release", got)
	}
	if job(t, q, lower.ID).PreemptionCount != 0 {
		t.Fatal("consumer paid an avoidable preemption")
	}
}

func TestAssignChurnProductionAfterRunningWave(t *testing.T) {
	q, clock, m, wave := assignChurnWaveFixture(t, 16)
	parent := submit(t, q, wave)
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.Batch == nil || running.Batch.Parent != parent.ID {
		t.Fatal("missing running wave", running)
	}
	production := cpuSpec(1, 32)
	production.Priority = "production"
	demand := submit(t, q, production)
	clock.Advance(time.Second)
	lower := submit(t, q, cpuSpec(1, 4))
	if got := assignedJob(machineCheck(t, q, m, running)); got != nil {
		t.Fatal("running wave's eventual release was hidden from production admission", got.ID)
	}
	if err := q.Cancel(demand.ID); err != nil {
		t.Fatal(err)
	}
	if got := assignedJob(machineCheck(t, q, m, running)); got == nil || got.ID != lower.ID {
		t.Fatal("withdrawn demand still withheld capacity", got)
	}
}

func TestAssignChurnProductionWaveUsesFreeTarget(t *testing.T) {
	for _, mode := range []string{"serial"} {
		t.Run(mode, func(t *testing.T) {
			q, _, m, wave := assignChurnWaveFixture(t, 32)
			peer := cloneMachine(&m)
			peer.ID, peer.HostID, peer.Session = "free-worker", "free-host", "free-session"
			e := registeredExtension(t, q, wave.Extension)
			report := newExtensionReport(e, &peer, wave.workType())
			report.Setup, report.Qualified, report.CheckID = true, true, "peer-check"
			peer.ExtensionReports = []ExtensionReport{report}
			check(t, q, peer, nil)
			h := cpuSpec(2, 16)
			h.Priority, h.AllowedMachines = "production", []string{m.ID}
			submit(t, q, h)
			running := assignedJob(machineCheck(t, q, m))
			if running == nil {
				t.Fatal("missing occupied target")
			}
			demand := submit(t, q, wave)
			x := cpuSpec(1, 4)
			x.AllowedMachines = []string{m.ID}
			lower := submit(t, q, x)
			if got := assignedJob(machineCheck(t, q, m, running)); got == nil || got.ID != lower.ID {
				t.Fatal("consumer withheld despite an immediately usable production target", got)
			}
			if got := assignedJob(machineCheck(t, q, peer)); got == nil || got.Batch == nil || got.Batch.Parent != demand.ID {
				t.Fatal("production did not use free target", got)
			}
		})
	}
}

func TestAssignChurnExistingConsumerStillYields(t *testing.T) {
	q, _, m, wave := assignChurnWaveFixture(t, 32)
	h := cpuSpec(2, 16)
	h.Priority = "production"
	submit(t, q, h)
	holder := assignedJob(machineCheck(t, q, m))
	submit(t, q, cpuSpec(1, 4))
	consumer := assignedJob(machineCheck(t, q, m, holder))
	if holder == nil || consumer == nil {
		t.Fatal("missing initial executions")
	}
	demand := submit(t, q, wave)
	finish(t, q, holder, true)
	stopped := job(t, q, consumer.ID)
	if stopped.State != "stopping" || priorityStop(stopped) == nil || priorityStop(stopped).Demand != demand.ID {
		t.Fatal("production did not preempt existing lower-tier work", stopped.State)
	}
	finishInterrupted(t, q, consumer)
	if got := assignedJob(machineCheck(t, q, m)); got == nil || got.Batch == nil || got.Batch.Parent != demand.ID {
		t.Fatal("production did not start after exact release", got)
	}
}
