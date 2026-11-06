package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
			rt, actor, loading := probeMemoryRuntimeFixture(t)
			ctx := context.Background()
			engine := loading.engine.(*FakeEngine)
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
				actor.worker.control = &processControl{pendingEvents: map[attemptKey]WorkerEvent{jobKey(actor.job): {Execution: Execution{ID: actor.job.ID, Attempt: actor.job.Attempt}, Session: actor.job.Session}}}
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

func probeMemoryRuntimeFixture(t *testing.T) (*modelRuntime, *attemptSupervisor, *workerModel) {
	t.Helper()
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
	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	loading := &workerModel{model: Model{Name: "shared", Engine: "fake", MemoryGB: 2, HostMemoryGB: 4}, state: "loading", engine: engine, cancel: cancel}
	rt := &modelRuntime{clock: clock, machine: machine.ID, memoryMachine: machine, memoryMeasured: true,
		batchJobs: []Job{probe}, batchSupervisors: map[attemptKey]*attemptSupervisor{jobKey(probe): actor}, batchHostMemorySamples: currentHostMemorySamples([]AttemptReport{report}, clock.Now()),
		models: map[string]*workerModel{"shared": loading}, hostMemorySource: func(context.Context) (float64, error) { return 14, nil }, memorySource: func(context.Context) (float64, error) { return 8, nil }}
	return rt, actor, loading
}

func TestEngineProbeMemoryStopDuringFinalDeviceQuery(t *testing.T) {
	rt, actor, loading := probeMemoryRuntimeFixture(t)
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	calls := 0
	rt.memorySource = func(context.Context) (float64, error) {
		calls++
		if calls == 3 { // Initial check, pre-preparation gate, then final payload gate.
			close(entered)
			<-resume
		}
		return 8, nil
	}
	go func() { defer close(done); rt.load(context.Background(), loading) }()
	select {
	case <-entered:
	case <-time.After(testWaitTimeout):
		close(resume)
		t.Fatal("final device query not reached")
	}
	actor.update(actor.worker, AttemptDirective{Stop: true}, true, false)
	close(resume)
	select {
	case <-done:
	case <-time.After(testWaitTimeout):
		t.Fatal("load did not finish")
	}
	if loading.state != "failed" || !strings.Contains(loading.err, "aggregate engine CPU/RAM allowance") || loading.engine.(*FakeEngine).Model != "" {
		t.Fatalf("stale admission survived final device query: state=%s error=%s", loading.state, loading.err)
	}
}

func TestBatchProbeMemoryStopDuringAuthorization(t *testing.T) {
	for _, boundary := range []string{"launch", "started"} {
		t.Run(boundary, func(t *testing.T) {
			rt, probe, _ := probeMemoryRuntimeFixture(t)
			rt.models = map[string]*workerModel{}
			next := Job{ID: "next", Attempt: 1, Session: probe.job.Session, Machine: rt.machine, State: "running", Spec: cpuSpec(1, 4)}
			if err := normalizeResources(&next.Spec); err != nil {
				t.Fatal(err)
			}
			actor := &attemptSupervisor{job: next}
			rt.batchJobs = append(rt.batchJobs, next)
			rt.batchSupervisors[jobKey(next)] = actor
			launch := HostLaunch{Machine: rt.machine, Session: next.Session, Kind: "batch", Key: next.ID, Attempt: fmt.Sprint(next.Attempt)}
			if err := rt.checkBatchLaunchMemory(context.Background(), launch); err != nil {
				t.Fatal("initial batch admission failed", err)
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/workers/"+boundary {
					t.Errorf("unexpected authorization %s", r.URL.Path)
				}
				close(entered)
				<-resume
				fmt.Fprintf(w, `{"ok":true,"start_gate":%q}`, startGateProtocol)
			}))
			defer server.Close()
			worker := Worker{Machine: rt.memoryMachine, Clock: rt.clock, Client: Client{URL: server.URL}, batchAdmission: rt.checkBatchLaunchMemory}
			done := make(chan error, 1)
			go func() {
				if boundary == "launch" {
					done <- worker.authorizeLaunch(launch)
				} else {
					done <- worker.authorizeStart(next, next.Session)
				}
			}()
			select {
			case <-entered:
			case <-time.After(testWaitTimeout):
				close(resume)
				t.Fatal("authorization not reached")
			}
			probe.update(probe.worker, AttemptDirective{Stop: true}, true, false)
			close(resume)
			select {
			case err := <-done:
				if !errors.Is(err, errWorkerMemory) {
					t.Fatalf("batch payload gate retained stale admission: %v", err)
				}
			case <-time.After(testWaitTimeout):
				t.Fatal("authorization did not finish")
			}
		})
	}
}
