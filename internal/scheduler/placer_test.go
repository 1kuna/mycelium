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

func TestPlaceSkipsWarmInstanceWhenIncrementalKVDoesNotFit(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(900), fixtures.WithKVPerToken(0))
	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	warm := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst_warm"),
		fixtures.WithInstancePreset(preset.ID),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(900, 0)),
	)
	fleet := domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{warm}}
	placer := NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(900, 200)}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(), fleet)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionQueued {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestPlaceUsesWarmInstanceDuringUnrelatedCatastrophicLoad(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(100), fixtures.WithKVPerToken(0))
	node := fixtures.MakeSparkNode(fixtures.WithNodeID("spark"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	warm := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst_warm"),
		fixtures.WithInstancePreset(preset.ID),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(100, 0)),
	)
	loading := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst_loading"),
		fixtures.WithInstancePreset("other"),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(100, 0)),
		func(i *domain.ModelInstance) {
			i.State = domain.InstLoading
			i.Loading = true
		},
	)
	placer := NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(100, 10)}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{warm, loading},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionWarmInstance || decision.InstanceID != warm.ID || decision.Claim != (domain.Claim{KVReservedMB: 10}) {
		t.Fatalf("decision = %+v", decision)
	}
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

func TestPlacePassesExpectedConcurrencyToEstimator(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithContextLength(4096))
	node := fixtures.MakeNode()
	estimator := &mocks.ResourceEstimator{Claim: fixtures.MakeClaim(10, 2)}
	placer := NewPlacer(estimator, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	_, err := placer.Place(context.Background(), fixtures.MakeJob(fixtures.WithContext(2048), fixtures.WithConcurrency(3)), domain.FleetSnapshot{Nodes: []domain.Node{node}})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if len(estimator.Calls) == 0 {
		t.Fatal("expected estimator calls")
	}
	for _, call := range estimator.Calls {
		if call.ContextLen != 2048 || call.Concurrency != 3 {
			t.Fatalf("estimator call = %+v", call)
		}
	}
}

func TestPlaceCarriesEffectiveContextInLaunchPreset(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithContextLength(262144))
	node := fixtures.MakeNode(fixtures.WithVRAM(32768), fixtures.WithMaxUtil(0.85))
	estimator := &mocks.ResourceEstimator{Claim: fixtures.MakeClaim(16039, 5120)}
	placer := NewPlacer(estimator, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID), fixtures.WithContext(81920)), domain.FleetSnapshot{Nodes: []domain.Node{node}})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action == domain.ActionQueued {
		t.Fatalf("decision queued unexpectedly: %+v", decision)
	}
	if decision.Preset.ContextLength != 81920 {
		t.Fatalf("launch preset context = %d, want 81920", decision.Preset.ContextLength)
	}
	if len(estimator.Calls) == 0 || estimator.Calls[0].ContextLen != 81920 {
		t.Fatalf("estimator calls = %+v", estimator.Calls)
	}
}

func TestPlaceAutoScoresColdCandidatesInsteadOfBlindWarmMatch(t *testing.T) {
	preset := fixtures.MakePreset()
	slowWarm := fixtures.MakeNode(fixtures.WithNodeID("node-slow"))
	slowWarm.SpeedClass.TokensPerSecRef = 5
	fastCold := fixtures.MakeNode(fixtures.WithNodeID("node-fast"))
	fastCold.SpeedClass.TokensPerSecRef = 100
	warm := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst-slow"),
		fixtures.WithInstancePreset(preset.ID),
		fixtures.OnNode(slowWarm.ID),
		fixtures.WithClaim(fixtures.MakeClaim(10, 1)),
	)
	placer := NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(10, 1)}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(fixtures.Auto), domain.FleetSnapshot{
		Nodes:     []domain.Node{slowWarm, fastCold},
		Instances: []domain.ModelInstance{warm},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action == domain.ActionWarmInstance || decision.NodeID != fastCold.ID {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestPlaceAutoUsesWarmInstanceWhenColdPlacementCannotFit(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(900), fixtures.WithKVPerToken(0))
	node := fixtures.MakeNode(fixtures.WithNodeID("spark"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	warm := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst-spark-122b"),
		fixtures.WithInstancePreset(preset.ID),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(900, 0)),
	)
	placer := NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(900, 10)}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(fixtures.Auto), domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{warm},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionWarmInstance || decision.InstanceID != warm.ID {
		t.Fatalf("decision = %+v", decision)
	}
	assertTraceContains(t, decision.Trace, "filter", "after cold no-fit")
}

func TestPlaceAutoWarmAfterColdNoFitPropagatesEstimateError(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(900), fixtures.WithKVPerToken(0))
	node := fixtures.MakeNode(fixtures.WithNodeID("spark"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	warm := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst-spark-122b"),
		fixtures.WithInstancePreset(preset.ID),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(900, 0)),
	)
	estimator := &countingUnitEstimator{claim: fixtures.MakeClaim(900, 10), errOn: 2}
	placer := NewPlacer(estimator, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(fixtures.Auto), domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{warm},
	})
	if !errors.Is(err, domain.ErrUnsupported) {
		t.Fatalf("Place err = %v decision=%+v", err, decision)
	}
	if estimator.calls != 2 {
		t.Fatalf("estimator calls = %d", estimator.calls)
	}
	assertTraceContains(t, decision.Trace, "estimate", domain.ErrUnsupported.Error())
}

func TestPlaceFiltersNodeWhenModelDownloadWouldCrossDiskFloor(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(10), fixtures.WithArtifactSize(30))
	tightDisk := fixtures.MakeNode(fixtures.WithNodeID("node-tight"), fixtures.WithVRAM(1000), fixtures.WithDisk(1000, 270))
	healthyDisk := fixtures.MakeNode(fixtures.WithNodeID("node-healthy"), fixtures.WithVRAM(1000), fixtures.WithDisk(1000, 500))
	placer := NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(10, 1)}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(fixtures.Latency), domain.FleetSnapshot{
		Nodes: []domain.Node{tightDisk, healthyDisk},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.NodeID != healthyDisk.ID {
		t.Fatalf("decision = %+v", decision)
	}
	dropped := traceStep(decision.Trace, "filter").Data["dropped"].(map[string]string)
	if dropped[tightDisk.ID] != "disk.free_after_model" {
		t.Fatalf("dropped = %+v", dropped)
	}
}

func TestPlaceTreatsPresetNodeAsHardLocality(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetNode("b70"))
	b70 := fixtures.MakeNode(fixtures.WithNodeID("b70"), fixtures.WithVRAM(1000))
	spark := fixtures.MakeSparkNode(fixtures.WithNodeID("spark"), fixtures.WithVRAM(100000))
	spark.SpeedClass.TokensPerSecRef = 1000
	placer := NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(10, 1)}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{spark, b70}})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.NodeID != "b70" {
		t.Fatalf("decision = %+v", decision)
	}
	dropped := traceStep(decision.Trace, "filter").Data["dropped"].(map[string]string)
	if dropped["spark"] != "preset.node_id" {
		t.Fatalf("dropped = %+v", dropped)
	}
}

func TestPlaceFiltersBackendLabelMismatch(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.Backend = domain.BackendVLLM
		p.LaunchArgs = []string{"--gpu-memory-utilization", "0.80"}
	})
	llamaNode := fixtures.MakeNode(fixtures.WithNodeID("llama"), fixtures.WithVRAM(100000), func(n *domain.Node) {
		n.Labels = map[string]string{domain.LabelPeerBackend: string(domain.BackendLlamaCpp)}
		n.SpeedClass.TokensPerSecRef = 1000
	})
	vllmNode := fixtures.MakeNode(fixtures.WithNodeID("vllm"), fixtures.WithVRAM(100000), func(n *domain.Node) {
		n.Labels = map[string]string{domain.LabelPeerBackend: string(domain.BackendVLLM)}
	})
	placer := NewPlacer(estimate.NewBackendAware(estimate.NewInMemory(), estimate.NewInMemory()), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{llamaNode, vllmNode}})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.NodeID != "vllm" {
		t.Fatalf("decision = %+v", decision)
	}
	dropped := traceStep(decision.Trace, "filter").Data["dropped"].(map[string]string)
	if dropped["llama"] != "label."+domain.LabelPeerBackend {
		t.Fatalf("dropped = %+v", dropped)
	}
}

func TestPlaceAcceptsNodeWithMultipleBackendRuntimes(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.Backend = domain.BackendLlamaCpp
	})
	multi := fixtures.MakeNode(fixtures.WithNodeID("multi"), fixtures.WithVRAM(100000), func(n *domain.Node) {
		n.Labels = map[string]string{domain.LabelPeerBackends: "llamacpp,vllm"}
		n.SpeedClass.TokensPerSecRef = 1000
	})
	vllmOnly := fixtures.MakeNode(fixtures.WithNodeID("vllm-only"), fixtures.WithVRAM(100000), func(n *domain.Node) {
		n.Labels = map[string]string{domain.LabelPeerBackends: "vllm"}
		n.SpeedClass.TokensPerSecRef = 100
	})
	placer := NewPlacer(estimate.NewBackendAware(estimate.NewInMemory(), estimate.NewInMemory()), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{vllmOnly, multi}})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.NodeID != "multi" {
		t.Fatalf("decision = %+v", decision)
	}
	dropped := traceStep(decision.Trace, "filter").Data["dropped"].(map[string]string)
	if dropped["vllm-only"] != "label."+domain.LabelPeerBackends {
		t.Fatalf("dropped = %+v", dropped)
	}
}

func TestPlaceSkipsWarmInstanceWhenNodeIsAtDiskFloor(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(10), fixtures.WithArtifactSize(100))
	fullNode := fixtures.MakeNode(fixtures.WithDisk(1000, 250))
	warm := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst-warm"),
		fixtures.WithInstancePreset(preset.ID),
		fixtures.OnNode(fullNode.ID),
		fixtures.WithClaim(fixtures.MakeClaim(10, 1)),
	)
	placer := NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(10, 1)}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{
		Nodes:     []domain.Node{fullNode},
		Instances: []domain.ModelInstance{warm},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionQueued {
		t.Fatalf("decision = %+v", decision)
	}
	dropped := traceStep(decision.Trace, "filter").Data["dropped"].(map[string]string)
	if dropped[fullNode.ID] != "disk.free" {
		t.Fatalf("dropped = %+v", dropped)
	}
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
