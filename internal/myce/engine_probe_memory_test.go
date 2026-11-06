package myce

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestEngineProbeMemoryQueueAndRuntimeAgree(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 16, 16
	m.CapabilityProfile.SystemMemoryBytes, m.CapabilityProfile.LogicalCPUs = 16<<30, 16
	m.CPUExtensionFingerprint = "cpu-v1"
	model := Model{Name: "shared", Path: "fake", Engine: "fake", MemoryGB: 2, HostMemoryGB: 4, Platforms: []string{"fake"}}
	if err := q.ConfigureModels(ModelConfig{Models: []Model{model}, Idle: time.Minute, Wait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	m.ModelURL, m.ModelConfigHash = "http://worker", modelConfigHash(q.models)
	machineCheck(t, q, m)
	spec := cpuSpec(2, 0)
	spec.Needs.Memory = "auto"
	submit(t, q, spec)
	probe := assignedJob(machineCheck(t, q, m))
	if probe == nil {
		t.Fatal("probe not assigned")
	}
	if probe.MemoryEnvelope == nil || probe.MemoryEnvelope.ProbeFloorGB <= 0 {
		t.Fatal("cold auto run lacks its measured probe floor")
	}
	footprint := 2.0
	report := AttemptReport{Execution: Execution{ID: probe.ID, Attempt: probe.Attempt}, Session: probe.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 2, FootprintGB: &footprint, GrowthObserved: true}}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	if err := q.Pin(ModelPin{Project: "test", Model: model.Name, Machine: m.ID, Device: "0"}); err != nil {
		t.Fatal(err)
	}
	reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.ModelPlan) != 1 {
		t.Fatalf("queue did not offer fitting model: plan=%+v pins=%+v host=%+v", reply.ModelPlan, q.s.Pins, q.s.Machines[m.ID].FreeHostMemoryGB)
	}
	loading := &workerModel{model: model, device: "0", state: "loading"}
	rt := modelRuntime{clock: clock, memoryMachine: m, memoryMeasured: true,
		batchJobs: []Job{*probe}, batchHostMemorySamples: currentHostMemorySamples([]AttemptReport{report}, clock.Now()),
		models:           map[string]*workerModel{"shared": loading},
		hostMemorySource: func(context.Context) (float64, error) { return 14, nil }, memorySource: func(context.Context) (float64, error) { return 8, nil }}
	if err := rt.checkLoadMemory(context.Background(), loading); err != nil {
		t.Fatal("runtime rejected queue's measured charge", err)
	}
	clock.Advance(hostMemorySampleFreshFor)
	if err := rt.checkLoadMemory(context.Background(), loading); err == nil {
		t.Fatal("runtime credited expired probe telemetry")
	}
}

func TestEngineProbeMemoryStopDuringPreparation(t *testing.T) {
	for _, source := range []string{"directive", "event"} {
		t.Run(source, func(t *testing.T) {
			_, clock := fixture(t)
			machine := m3Machine()
			machine.HostMemoryBudgetGB = 16
			machine.CapabilityProfile.LogicalCPUs = 16
			probe := Job{ID: "probe", Attempt: 1, Session: "session", State: "running", Spec: cpuSpec(2, 16), MemoryEnvelope: &JobMemoryEnvelope{ProbeFloorGB: 0.125}}
			probe.Spec.Needs.Memory = "auto"
			if err := normalizeResources(&probe.Spec); err != nil {
				t.Fatal(err)
			}
			actor := &attemptSupervisor{job: probe, worker: Worker{Clock: clock}, wake: make(chan struct{}, 1)}
			footprint := 2.0
			report := AttemptReport{Execution: Execution{ID: probe.ID, Attempt: probe.Attempt}, Session: probe.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 2, FootprintGB: &footprint, GrowthObserved: true}}
			engine := &FakeEngine{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			loading := &workerModel{model: Model{Name: "shared", Engine: "fake", MemoryGB: 2, HostMemoryGB: 4}, state: "loading", engine: engine, cancel: cancel}
			rt := modelRuntime{clock: clock, memoryMachine: machine, memoryMeasured: true,
				batchJobs: []Job{probe}, batchSupervisors: map[attemptKey]*attemptSupervisor{jobKey(probe): actor}, batchHostMemorySamples: currentHostMemorySamples([]AttemptReport{report}, clock.Now()),
				models: map[string]*workerModel{"shared": loading}, hostMemorySource: func(context.Context) (float64, error) { return 14, nil }, memorySource: func(context.Context) (float64, error) { return 8, nil }}
			if err := rt.checkLoadMemory(ctx, loading); err != nil {
				t.Fatal("running probe should permit the model", err)
			}
			preparing, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			ctx = withLaunchGate(ctx, func() error {
				select {
				case <-preparing:
				default:
					close(preparing)
				}
				<-resume
				return nil
			})
			go func() { defer close(done); rt.load(ctx, loading) }()
			select {
			case <-preparing:
			case <-time.After(testWaitTimeout):
				close(resume)
				t.Fatal("load did not reach preparation")
			}
			if source == "directive" {
				actor.update(actor.worker, AttemptDirective{Stop: true, UserCancel: true}, true, false)
				// A reconnect must not restore running-state admission credit.
				actor.update(actor.worker, AttemptDirective{}, false, false)
			} else {
				actor.mu.Lock()
				actor.worker.control = &processControl{pendingEvents: map[attemptKey]WorkerEvent{jobKey(probe): {Execution: Execution{ID: probe.ID, Attempt: probe.Attempt}, Session: probe.Session}}}
				actor.mu.Unlock()
			}
			close(resume)
			select {
			case <-done:
			case <-time.After(testWaitTimeout):
				t.Fatal("load did not finish")
			}
			if loading.state != "failed" || !strings.Contains(loading.err, "aggregate engine CPU/RAM allowance") || engine.Model != "" {
				t.Fatalf("stopping probe did not restore full charge before model launch: state=%s error=%s loaded=%s", loading.state, loading.err, engine.Model)
			}
			if actor.job.State != "running" || rt.batchJobs[0].State != "running" {
				t.Fatal("admission changed the original execution or heartbeat snapshot")
			}
		})
	}
}
