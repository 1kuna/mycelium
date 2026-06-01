package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestPlacerSatisfiesPort(t *testing.T) {
	var _ ports.Placer = NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{}, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
}

func TestPlaceUsesWarmInstanceForThroughput(t *testing.T) {
	preset := fixtures.MakePreset()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst_warm"), fixtures.WithInstancePreset(preset.ID))
	fleet := domain.FleetSnapshot{
		Nodes:     []domain.Node{fixtures.MakeNode()},
		Instances: []domain.ModelInstance{inst},
	}
	placer := NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(1, 1)}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(), fleet)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionWarmInstance || decision.InstanceID != "inst_warm" {
		t.Fatalf("decision = %+v", decision)
	}
	assertTraceContains(t, decision.Trace, "admit", "warm")
}

func TestPlaceLoadsBestColdCandidate(t *testing.T) {
	preset := fixtures.MakePreset()
	fleet := domain.FleetSnapshot{
		Nodes: []domain.Node{
			fixtures.Make4090Node(),
			fixtures.MakeSparkNode(),
		},
	}
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(fixtures.Latency), fleet)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionDedicatedUnit || decision.NodeID != "node_spark" {
		t.Fatalf("decision = %+v", decision)
	}
	assertTraceContains(t, decision.Trace, "score", "node_spark")
}

func TestPlaceQueuesWhenNoFitAndSoftPreemption(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(1000), fixtures.WithKVPerToken(0))
	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.5))
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{node}})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionQueued {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestPlaceFailsLoudOnUnknownPresetAndEstimateError(t *testing.T) {
	placer := NewPlacer(&mocks.ResourceEstimator{}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	_, err := placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{})
	if err == nil || !strings.Contains(err.Error(), "unknown model") {
		t.Fatalf("unknown model err = %v", err)
	}

	preset := fixtures.MakePreset()
	placer = NewPlacer(&mocks.ResourceEstimator{Err: errors.New("boom")}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	_, err = placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("estimate err = %v", err)
	}
}

func TestPlaceFiltersByTaskCapabilityAndNodeSelector(t *testing.T) {
	preset := fixtures.MakePreset()
	wrongTask := preset
	wrongTask.Capabilities = []domain.Capability{domain.CapabilityEmbedding}
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), wrongTask)
	if _, err := placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}}); err == nil || !strings.Contains(err.Error(), "does not support") {
		t.Fatalf("capability err = %v", err)
	}

	emptyCaps := preset
	emptyCaps.Capabilities = nil
	placer = NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), emptyCaps)
	if _, err := placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode()}}); err == nil || !strings.Contains(err.Error(), "no schedulable capabilities") {
		t.Fatalf("empty capabilities err = %v", err)
	}

	nvidia := fixtures.MakeNode(fixtures.WithNodeID("node-nvidia"))
	nvidia.Labels = map[string]string{"gpu.vendor": "nvidia"}
	intel := fixtures.MakeNode(fixtures.WithNodeID("node-intel"))
	intel.Labels = map[string]string{"gpu.vendor": "intel"}
	intel.SpeedClass.TokensPerSecRef = 1000
	placer = NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	job := fixtures.MakeJob(func(j *domain.Job) {
		j.NodeSelector = map[string]string{"gpu.vendor": "nvidia"}
	})
	decision, err := placer.Place(context.Background(), job, domain.FleetSnapshot{Nodes: []domain.Node{intel, nvidia}})
	if err != nil {
		t.Fatalf("Place selector: %v", err)
	}
	if decision.NodeID != "node-nvidia" {
		t.Fatalf("decision = %+v", decision)
	}
	filter := traceStep(decision.Trace, "filter")
	dropped := filter.Data["dropped"].(map[string]string)
	if dropped["node-intel"] != "label.gpu.vendor" {
		t.Fatalf("dropped = %+v", dropped)
	}
}

func TestPlaceRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	placer := NewPlacer(&mocks.ResourceEstimator{}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), fixtures.MakePreset())
	_, err := placer.Place(ctx, fixtures.MakeJob(), domain.FleetSnapshot{})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}

func traceStep(trace []domain.TraceStep, step string) domain.TraceStep {
	for _, got := range trace {
		if got.Step == step {
			return got
		}
	}
	return domain.TraceStep{}
}

func assertTraceContains(t *testing.T, trace []domain.TraceStep, step string, text string) {
	t.Helper()
	for _, got := range trace {
		if got.Step == step && strings.Contains(got.Result, text) {
			return
		}
	}
	t.Fatalf("trace missing %s containing %q: %+v", step, text, trace)
}
