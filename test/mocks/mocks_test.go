package mocks

import (
	"context"
	"errors"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
)

func TestBackendAdapterRecordsCallsAndFailures(t *testing.T) {
	backend := NewBackendAdapter()
	if backend.Name() != "mock" {
		t.Fatalf("name = %s", backend.Name())
	}
	handle, err := backend.Launch(context.Background(), fixtures.MakePreset(), "127.0.0.1:1")
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	backend.ReadyAfter = 1
	if err := backend.WaitReady(context.Background(), handle.Addr); err == nil {
		t.Fatal("first WaitReady should fail")
	}
	if err := backend.WaitReady(context.Background(), handle.Addr); err != nil {
		t.Fatalf("second WaitReady: %v", err)
	}
	if err := backend.Stop(context.Background(), handle); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(backend.Calls) != 4 {
		t.Fatalf("calls = %+v", backend.Calls)
	}

	launchErr := errors.New("launch")
	backend = &BackendAdapter{LaunchErr: launchErr}
	if _, err := backend.Launch(context.Background(), fixtures.MakePreset(), "addr"); !errors.Is(err, launchErr) {
		t.Fatalf("launch error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backend.WaitReady(ctx, "addr"); err == nil {
		t.Fatal("WaitReady should return context error")
	}
	stopErr := errors.New("stop")
	backend.StopErr = stopErr
	if err := backend.Stop(context.Background(), ports.Handle{}); !errors.Is(err, stopErr) {
		t.Fatalf("stop error = %v", err)
	}
}

func TestNodeAgentRecordsLoadUnloadAndFailures(t *testing.T) {
	agent := NewNodeAgent(fixtures.MakeNode())
	if _, err := agent.Snapshot(context.Background()); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	inst, err := agent.Load(context.Background(), fixtures.MakePreset())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if inst.ID == "" || len(agent.Instances) != 1 {
		t.Fatalf("load state = %+v", agent)
	}
	if err := agent.Unload(context.Background(), inst.ID); err != nil {
		t.Fatalf("Unload: %v", err)
	}
	if len(agent.Instances) != 0 {
		t.Fatalf("instances = %+v", agent.Instances)
	}

	loadErr := errors.New("load")
	agent.LoadErr = loadErr
	if _, err := agent.Load(context.Background(), fixtures.MakePreset()); !errors.Is(err, loadErr) {
		t.Fatalf("load error = %v", err)
	}
	unloadErr := errors.New("unload")
	agent.UnloadErr = unloadErr
	if err := agent.Unload(context.Background(), "missing"); !errors.Is(err, unloadErr) {
		t.Fatalf("unload error = %v", err)
	}
}

func TestResourceEstimatorAllocatorTelemetryAndClock(t *testing.T) {
	estimator := &ResourceEstimator{Claim: fixtures.MakeClaim(1, 2)}
	claim, err := estimator.Estimate(context.Background(), fixtures.MakePreset(), 10, 3)
	if err != nil || claim != fixtures.MakeClaim(1, 2) || len(estimator.Calls) != 1 {
		t.Fatalf("estimate = %+v %v calls=%+v", claim, err, estimator.Calls)
	}
	estErr := errors.New("estimate")
	estimator.Err = estErr
	if _, err := estimator.Estimate(context.Background(), fixtures.MakePreset(), 10, 1); !errors.Is(err, estErr) {
		t.Fatalf("estimate error = %v", err)
	}

	allocator := &Allocator{FitsVal: true, CanStackLoadVal: true}
	if !allocator.Fits(fixtures.MakeNode(), []int{0}, nil, fixtures.MakeClaim(1, 1)) {
		t.Fatal("Fits should return configured value")
	}
	if !allocator.CanStackLoad(fixtures.MakeNode(), []int{0}, nil) {
		t.Fatal("CanStackLoad should return configured value")
	}
	if allocator.FitsCalls != 1 || allocator.CanStackCalls != 1 {
		t.Fatalf("allocator calls = %d/%d", allocator.FitsCalls, allocator.CanStackCalls)
	}

	sink := &TelemetrySink{}
	metric := domain.RunMetric{JobID: "job"}
	if err := sink.Record(context.Background(), metric); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if len(sink.Metrics) != 1 {
		t.Fatalf("metrics = %+v", sink.Metrics)
	}
	recordErr := errors.New("record")
	sink.Err = recordErr
	if err := sink.Record(context.Background(), metric); !errors.Is(err, recordErr) {
		t.Fatalf("record error = %v", err)
	}

	clock := NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	timer := clock.NewTimer(time.Second)
	if !timer.Stop() {
		t.Fatal("fresh timer should stop")
	}
	if timer.Stop() {
		t.Fatal("stopped timer should not stop twice")
	}
	clock.Advance(time.Second)
	if !clock.Now().Equal(time.Date(2026, 5, 29, 12, 0, 1, 0, time.UTC)) {
		t.Fatalf("now = %s", clock.Now())
	}
}
