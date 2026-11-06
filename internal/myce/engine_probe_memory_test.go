package myce

import (
	"context"
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
