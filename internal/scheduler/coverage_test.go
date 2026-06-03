package scheduler

import (
	"context"
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

func TestResolvePresetBranches(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset_a"), fixtures.WithModelRef("model_a"), fixtures.WithAliases("alias_a"))
	blankModel := fixtures.MakePreset(fixtures.WithPresetID("preset_blank"), fixtures.WithModelRef(""))
	placer := NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{}, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset, blankModel)

	got, err := placer.resolvePreset(fixtures.MakeJob(fixtures.WithPreset("preset_a")))
	if err != nil || got.ID != "preset_a" {
		t.Fatalf("resolve by preset = %+v %v", got, err)
	}
	got, err = placer.resolvePreset(fixtures.MakeJob(fixtures.WithModel("alias_a")))
	if err != nil || got.ID != "preset_a" {
		t.Fatalf("resolve by alias = %+v %v", got, err)
	}
	_, err = placer.resolvePreset(domain.Job{ID: "missing"})
	if err == nil || !strings.Contains(err.Error(), "no model") {
		t.Fatalf("missing model err = %v", err)
	}
	_, err = placer.resolvePreset(fixtures.MakeJob(fixtures.WithPreset("missing")))
	if err == nil || !strings.Contains(err.Error(), "unknown preset") {
		t.Fatalf("unknown preset err = %v", err)
	}
}

func TestSelectWarmInstanceChoosesLeastBusyThenID(t *testing.T) {
	preset := fixtures.MakePreset()
	placer := NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{FitsVal: true, CanStackLoadVal: true}, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	busy := fixtures.MakeInstance(fixtures.WithInstanceID("a_busy"), fixtures.WithInstancePreset(preset.ID))
	busy.InFlight = 2
	later := fixtures.MakeInstance(fixtures.WithInstanceID("z_idle"), fixtures.WithInstancePreset(preset.ID))
	later.InFlight = 0
	earlier := fixtures.MakeInstance(fixtures.WithInstanceID("a_idle"), fixtures.WithInstancePreset(preset.ID))
	earlier.InFlight = 0

	got, ok, err := placer.selectWarmInstance(context.Background(), fixtures.MakeJob(), preset, 100, 1, domain.FleetSnapshot{
		Nodes:     []domain.Node{fixtures.MakeNode()},
		Instances: []domain.ModelInstance{busy, later, earlier},
	})
	if err != nil {
		t.Fatalf("selectWarmInstance: %v", err)
	}
	if !ok || got.ID != "a_idle" {
		t.Fatalf("warm = %+v ok=%v", got, ok)
	}
	if _, ok, err := placer.selectWarmInstance(context.Background(), fixtures.MakeJob(fixtures.Latency), preset, 100, 1, domain.FleetSnapshot{}); err != nil || ok {
		t.Fatal("latency job should skip warm batching")
	}
	if _, ok, err := placer.selectWarmInstance(context.Background(), fixtures.MakeJob(), preset, 100, 1, domain.FleetSnapshot{}); err != nil || ok {
		t.Fatal("empty fleet should not return warm instance")
	}
	if _, ok, err := placer.selectWarmInstance(context.Background(), fixtures.MakeJob(func(j *domain.Job) {
		j.NodeSelector = map[string]string{"gpu.vendor": "amd"}
	}), preset, 100, 1, domain.FleetSnapshot{
		Nodes:     []domain.Node{fixtures.MakeNode()},
		Instances: []domain.ModelInstance{earlier},
	}); err != nil || ok {
		t.Fatal("warm instance on selector-mismatched node should not be selected")
	}

	errPlacer := NewPlacer(unitEstimator{err: domain.ErrUnsupported}, &mocks.Allocator{FitsVal: true, CanStackLoadVal: true}, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	_, ok, err = errPlacer.selectWarmInstance(context.Background(), fixtures.MakeJob(), preset, 100, 1, domain.FleetSnapshot{
		Nodes:     []domain.Node{fixtures.MakeNode()},
		Instances: []domain.ModelInstance{earlier},
	})
	if err == nil || ok || !strings.Contains(err.Error(), domain.ErrUnsupported.Error()) {
		t.Fatalf("warm estimate err = %v ok=%v", err, ok)
	}
}

func TestFilterCandidatesDropsStatusLoadAndFit(t *testing.T) {
	claim := fixtures.MakeClaim(1, 1)
	down := fixtures.MakeNode(fixtures.WithNodeID("down"), fixtures.Maintenance)
	noFit := fixtures.MakeNode(fixtures.WithNodeID("nofit"))
	noStack := fixtures.MakeSparkNode(fixtures.WithNodeID("nostack"))
	fit := fixtures.MakeNode(fixtures.WithNodeID("fit"))

	placer := NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{FitsVal: false, CanStackLoadVal: true}, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	candidates, trace := placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{down, noFit}}, claim)
	if len(candidates) != 0 {
		t.Fatalf("candidates = %+v", candidates)
	}
	dropped := trace.Data["dropped"].(map[string]string)
	if dropped["down"] != string(domain.NodeMaintenance) || dropped["nofit"] != "fit" {
		t.Fatalf("dropped = %+v", dropped)
	}

	placer = NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{FitsVal: true, CanStackLoadVal: false}, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	_, trace = placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{noStack}}, claim)
	dropped = trace.Data["dropped"].(map[string]string)
	if dropped["nostack"] != "load_in_flight" {
		t.Fatalf("dropped = %+v", dropped)
	}

	placer = NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{FitsVal: true, CanStackLoadVal: true}, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	candidates, _ = placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{fit}}, claim)
	if len(candidates) != 1 || candidates[0].node.ID != "fit" {
		t.Fatalf("fit candidates = %+v", candidates)
	}
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("a"))
	nodeB := fixtures.MakeNode(fixtures.WithNodeID("b"))
	candidates, _ = placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{nodeB, nodeA}}, claim)
	if len(candidates) != 2 || candidates[0].node.ID != "a" {
		t.Fatalf("node sort = %+v", candidates)
	}

	multiAcc := fixtures.MakeNode(fixtures.WithNodeID("multi"), func(n *domain.Node) {
		n.Accelerators = []domain.Accelerator{{Index: 1, VRAMTotalMB: 1000}, {Index: 0, VRAMTotalMB: 1000}}
	})
	candidates, _ = placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{multiAcc}}, claim)
	if len(candidates) != 3 || candidates[0].acc[0] != 0 || len(candidates[2].acc) != 2 {
		t.Fatalf("accelerator sort = %+v", candidates)
	}
	if acceleratorSetLess([]int{0, 1}, []int{0, 1}) {
		t.Fatal("equal accelerator sets should not sort less")
	}
	dedicatedInst := fixtures.MakeInstance(fixtures.WithInstanceID("busy"), fixtures.OnNode(fit.ID), func(i *domain.ModelInstance) { i.AcceleratorSet = []int{0} })
	candidates, trace = placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{fit}, Instances: []domain.ModelInstance{dedicatedInst}}, claim, true)
	if len(candidates) != 0 || trace.Data["dropped"].(map[string]string)["fit"] != "dedicated_unit" {
		t.Fatalf("dedicated candidates=%+v trace=%+v", candidates, trace)
	}
	otherNodeInst := fixtures.MakeInstance(fixtures.WithInstanceID("other"), fixtures.OnNode("other"), func(i *domain.ModelInstance) { i.AcceleratorSet = []int{0} })
	if hasOverlappingInstance(fit.ID, []int{0}, []domain.ModelInstance{otherNodeInst}) {
		t.Fatal("instance on another node should not overlap")
	}
}

func TestFilterPlacementCandidatesCoversUnitEstimatorBranches(t *testing.T) {
	preset := fixtures.MakePreset()
	claim := fixtures.MakeClaim(1, 1)
	fit := fixtures.MakeNode(fixtures.WithNodeID("fit"))
	mismatch := fixtures.MakeNode(fixtures.WithNodeID("mismatch"))
	mismatch.Labels = map[string]string{"gpu.vendor": "amd"}
	down := fixtures.MakeNode(fixtures.WithNodeID("down"), fixtures.Maintenance)
	loading := fixtures.MakeInstance(fixtures.WithInstanceID("loading"), fixtures.OnNode(fit.ID), func(i *domain.ModelInstance) {
		i.State = domain.InstLoading
		i.Loading = true
	})
	placer := NewPlacer(unitEstimator{claim: claim}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	job := fixtures.MakeJob(func(j *domain.Job) {
		j.NodeSelector = map[string]string{"gpu.vendor": "nvidia"}
	})

	candidates, trace, err := placer.filterPlacementCandidates(context.Background(), job, preset, 100, 1, domain.FleetSnapshot{
		Nodes:     []domain.Node{down, mismatch, fit},
		Instances: []domain.ModelInstance{loading},
	}, true)
	if err != nil {
		t.Fatalf("filterPlacementCandidates: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %+v", candidates)
	}
	dropped := trace.Data["dropped"].(map[string]string)
	if dropped["down"] != string(domain.NodeMaintenance) || dropped["mismatch"] != "label.gpu.vendor" || dropped["fit"] != "dedicated_unit" {
		t.Fatalf("dropped = %+v", dropped)
	}

	placer = NewPlacer(unitEstimator{claim: claim}, &mocks.Allocator{FitsVal: true, CanStackLoadVal: false}, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	_, trace, err = placer.filterPlacementCandidates(context.Background(), fixtures.MakeJob(), preset, 100, 1, domain.FleetSnapshot{Nodes: []domain.Node{fit}})
	if err != nil {
		t.Fatalf("filter load-in-flight: %v", err)
	}
	if trace.Data["dropped"].(map[string]string)["fit"] != "load_in_flight" {
		t.Fatalf("load-in-flight dropped = %+v", trace.Data["dropped"])
	}

	multiAcc := fixtures.MakeNode(fixtures.WithNodeID("multi-placement"), func(n *domain.Node) {
		n.Accelerators = []domain.Accelerator{{Index: 1, VRAMTotalMB: 1000}, {Index: 0, VRAMTotalMB: 1000}}
	})
	placer = NewPlacer(unitEstimator{claim: claim}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	candidates, _, err = placer.filterPlacementCandidates(context.Background(), fixtures.MakeJob(), preset, 100, 1, domain.FleetSnapshot{Nodes: []domain.Node{multiAcc}})
	if err != nil {
		t.Fatalf("filter placement multi-acc: %v", err)
	}
	if len(candidates) != 3 || candidates[0].acc[0] != 0 || len(candidates[2].acc) != 2 {
		t.Fatalf("placement accelerator sort = %+v", candidates)
	}

	_, _, err = NewPlacer(unitEstimator{err: domain.ErrUnsupported}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset).
		filterPlacementCandidates(context.Background(), fixtures.MakeJob(), preset, 100, 1, domain.FleetSnapshot{Nodes: []domain.Node{fit}})
	if err == nil || !strings.Contains(err.Error(), domain.ErrUnsupported.Error()) {
		t.Fatalf("unit estimator err = %v", err)
	}
}

func TestScoreCandidatesTieBreaksDeterministically(t *testing.T) {
	node := fixtures.MakeNode(func(n *domain.Node) {
		n.Accelerators = []domain.Accelerator{
			{Index: 1, VRAMTotalMB: 1024},
			{Index: 0, VRAMTotalMB: 1024},
		}
		n.SpeedClass.TokensPerSecRef = 10
	})
	placer := NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{}, mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))

	scored := placer.scoreCandidates(fixtures.MakeJob(), fixtures.MakePreset(), []candidate{
		{node: fixtures.MakeNode(fixtures.WithNodeID("b")), acc: []int{0}},
		{node: fixtures.MakeNode(fixtures.WithNodeID("a")), acc: []int{0}},
	})
	if scored[0].candidate.node.ID != "a" {
		t.Fatalf("node id tie-break failed: %+v", scored)
	}

	scored = placer.scoreCandidates(fixtures.MakeJob(), fixtures.MakePreset(), []candidate{
		{node: node, acc: []int{1}},
		{node: node, acc: []int{0}},
	})
	if scored[0].candidate.acc[0] != 0 {
		t.Fatalf("accelerator tie-break failed: %+v", scored)
	}

	localPreset := fixtures.MakePreset(fixtures.WithPresetNode("local"))
	scored = placer.scoreCandidates(fixtures.MakeJob(), localPreset, []candidate{
		{node: fixtures.MakeNode(fixtures.WithNodeID("remote")), acc: []int{0}},
		{node: fixtures.MakeNode(fixtures.WithNodeID("local")), acc: []int{0}},
	})
	if scored[0].candidate.node.ID != "local" || scored[0].parts["model_locality"] != true {
		t.Fatalf("locality score failed: %+v", scored)
	}
}

func TestEffectiveSpeedAndActionDefaults(t *testing.T) {
	if effectiveSpeed("") != domain.SpeedThroughput {
		t.Fatal("empty speed should default to throughput")
	}
	if actionForSpeed(domain.SpeedThroughput) != domain.ActionLoadedNew {
		t.Fatal("throughput should load normally")
	}
	if actionForSpeed(domain.SpeedLatency) != domain.ActionDedicatedUnit {
		t.Fatal("latency should dedicate")
	}
}

func TestValidatePresetForJobAllowsEmptyTaskType(t *testing.T) {
	if err := validatePresetForJob(domain.Job{ID: "job"}, fixtures.MakePreset()); err != nil {
		t.Fatalf("empty task validation = %v", err)
	}
}

func TestPreemptHardReplacesVictimWhenOtherNodeFits(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(800), fixtures.WithKVPerToken(0))
	target := fixtures.Make4090Node(fixtures.WithNodeID("target"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.8))
	other := fixtures.Make4090Node(fixtures.WithNodeID("other"), fixtures.WithVRAM(200), fixtures.WithMaxUtil(0.8))
	victim := fixtures.MakeInstance(
		fixtures.WithInstanceID("victim"),
		fixtures.WithInstancePreset("other_preset"),
		fixtures.OnNode(target.ID),
		fixtures.WithClaim(fixtures.MakeClaim(100, 0)),
		fixtures.WithInstancePriority(domain.PriorityBackground),
	)
	victim.InFlight = 1
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(),
		fixtures.MakeJob(fixtures.Hard, fixtures.WithPreset(preset.ID)),
		domain.FleetSnapshot{Nodes: []domain.Node{target, other}, Instances: []domain.ModelInstance{victim}})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if len(decision.Requeued) != 0 {
		t.Fatalf("victim should replace, requeued = %+v", decision.Requeued)
	}
	if len(decision.Replacements) != 1 || decision.Replacements[0].InstanceID != victim.ID || decision.Replacements[0].NodeID != other.ID {
		t.Fatalf("replacement = %+v", decision.Replacements)
	}
	assertTraceContains(t, decision.Trace, "replace", "replaced")
}

func TestPreemptOnlyRequeuesActiveVictimsWithoutReplacement(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(800), fixtures.WithKVPerToken(0))
	node := fixtures.Make4090Node(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.8))
	victim := fixtures.MakeInstance(
		fixtures.WithInstanceID("victim"),
		fixtures.WithInstancePreset("other_preset"),
		fixtures.OnNode(node.ID),
		fixtures.WithClaim(fixtures.MakeClaim(100, 0)),
		fixtures.WithInstancePriority(domain.PriorityBackground),
	)
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	job := fixtures.MakeJob(fixtures.Hard, fixtures.WithPreset(preset.ID))

	decision, err := placer.Place(context.Background(), job, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}})
	if err != nil {
		t.Fatalf("Place idle: %v", err)
	}
	if decision.Action != domain.ActionHardPreempted || len(decision.Requeued) != 0 {
		t.Fatalf("idle victim should be evicted without requeue: %+v", decision)
	}

	victim.InFlight = 1
	decision, err = placer.Place(context.Background(), job, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}})
	if err != nil {
		t.Fatalf("Place active: %v", err)
	}
	if len(decision.Requeued) != 1 || decision.Requeued[0] != victim.ID {
		t.Fatalf("active victim should be requeued: %+v", decision)
	}
}

func TestPreemptSkipsUnusableCandidatesAndReturnsNoResult(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(1000), fixtures.WithKVPerToken(0))
	maintenance := fixtures.Make4090Node(fixtures.WithNodeID("maintenance"), fixtures.Maintenance)
	full := fixtures.Make4090Node(fixtures.WithNodeID("full"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.5))
	victim := fixtures.MakeInstance(
		fixtures.WithInstanceID("victim"),
		fixtures.WithInstancePreset("other"),
		fixtures.OnNode(full.ID),
		fixtures.WithClaim(fixtures.MakeClaim(100, 0)),
		fixtures.WithInstancePriority(domain.PriorityBackground),
	)
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	result, ok := placer.tryPreempt(
		fixtures.MakeJob(fixtures.Interactive, fixtures.HardForInteractive, fixtures.WithPreset(preset.ID)),
		domain.FleetSnapshot{Nodes: []domain.Node{maintenance, full}, Instances: []domain.ModelInstance{victim}},
		fixtures.MakeClaim(1000, 0))
	if ok || len(result.trace) != 0 {
		t.Fatalf("preempt = %+v ok=%v", result, ok)
	}
}

func TestTryPreemptSortsMultipleResults(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(800), fixtures.WithKVPerToken(0))
	nodeA := fixtures.Make4090Node(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.8))
	nodeB := fixtures.Make4090Node(fixtures.WithNodeID("node-b"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.8))
	instances := []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.WithInstanceID("victim-z"), fixtures.WithInstancePreset("other-a"), fixtures.OnNode(nodeA.ID), fixtures.WithClaim(fixtures.MakeClaim(100, 0)), fixtures.WithInstancePriority(domain.PriorityBackground)),
		fixtures.MakeInstance(fixtures.WithInstanceID("victim-a"), fixtures.WithInstancePreset("other-b"), fixtures.OnNode(nodeB.ID), fixtures.WithClaim(fixtures.MakeClaim(100, 0)), fixtures.WithInstancePriority(domain.PriorityBackground)),
	}
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	result, ok := placer.tryPreempt(
		fixtures.MakeJob(fixtures.Interactive, fixtures.HardForInteractive, fixtures.WithPreset(preset.ID)),
		domain.FleetSnapshot{Nodes: []domain.Node{nodeA, nodeB}, Instances: instances},
		fixtures.MakeClaim(800, 0))
	if !ok || result.victim.ID != "victim-a" {
		t.Fatalf("preempt = %+v ok=%v", result, ok)
	}
}

func TestTryPreemptForPresetPropagatesEstimateError(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.Make4090Node(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.8))
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim"), fixtures.WithInstancePreset("other"), fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(100, 0)), fixtures.WithInstancePriority(domain.PriorityBackground))
	placer := NewPlacer(unitEstimator{err: domain.ErrUnsupported}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	_, _, err := placer.tryPreemptForPreset(context.Background(), fixtures.MakeJob(fixtures.Interactive, fixtures.HardForInteractive, fixtures.WithPreset(preset.ID)), preset, 100, 1, domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{victim},
	})
	if !strings.Contains(err.Error(), domain.ErrUnsupported.Error()) {
		t.Fatalf("preempt estimate err = %v", err)
	}
}

func TestPlacePropagatesUnitEstimateErrors(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim"), fixtures.WithInstancePreset("other"), fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 0)), fixtures.WithInstancePriority(domain.PriorityBackground))
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))

	_, err := NewPlacer(unitEstimator{err: domain.ErrUnsupported}, lease.NewAllocator(), clock, preset).
		Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{node}})
	if err == nil || !strings.Contains(err.Error(), domain.ErrUnsupported.Error()) {
		t.Fatalf("filter estimate err = %v", err)
	}

	warm := fixtures.MakeInstance(fixtures.WithInstancePreset(preset.ID), fixtures.OnNode(node.ID))
	_, err = NewPlacer(unitEstimator{err: domain.ErrUnsupported}, &mocks.Allocator{FitsVal: true, CanStackLoadVal: true}, clock, preset).
		Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{warm}})
	if err == nil || !strings.Contains(err.Error(), domain.ErrUnsupported.Error()) {
		t.Fatalf("warm estimate err = %v", err)
	}

	estimator := &countingUnitEstimator{claim: fixtures.MakeClaim(1, 1), errOn: 1}
	_, err = NewPlacer(estimator, &mocks.Allocator{FitsVal: false, CanStackLoadVal: false}, clock, preset).
		Place(context.Background(), fixtures.MakeJob(fixtures.Hard), domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}})
	if err == nil || !strings.Contains(err.Error(), domain.ErrUnsupported.Error()) {
		t.Fatalf("preempt estimate err = %v", err)
	}
}

func TestPreemptSelectorMismatchSkipsCandidate(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	node.Labels = map[string]string{"gpu.vendor": "intel"}
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim"), fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 0)), fixtures.WithInstancePriority(domain.PriorityBackground))
	placer := NewPlacer(unitEstimator{claim: fixtures.MakeClaim(1, 1)}, lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)
	_, ok, err := placer.tryPreemptForPreset(context.Background(), fixtures.MakeJob(fixtures.Interactive, fixtures.HardForInteractive, func(j *domain.Job) {
		j.NodeSelector = map[string]string{"gpu.vendor": "nvidia"}
	}), preset, 100, 1, domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: []domain.ModelInstance{victim}})
	if err != nil || ok {
		t.Fatalf("preempt selector ok=%v err=%v", ok, err)
	}
}

func TestPresetBackendMismatchHelperBranches(t *testing.T) {
	node := fixtures.MakeNode()
	if reason, drop := presetBackendMismatch(domain.Preset{}, node); drop || reason != "" {
		t.Fatalf("empty backend mismatch = %q %v", reason, drop)
	}
	preset := fixtures.MakePreset()
	node.Labels = nil
	if reason, drop := presetBackendMismatch(preset, node); drop || reason != "" {
		t.Fatalf("missing label mismatch = %q %v", reason, drop)
	}
}

func TestWarmSelectionSkipsPresetLocalityAndBackendMismatch(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))

	localPreset := fixtures.MakePreset(fixtures.WithPresetNode("right-node"))
	wrongNode := fixtures.MakeNode(fixtures.WithNodeID("wrong-node"))
	wrongWarm := fixtures.MakeInstance(fixtures.WithInstanceID("wrong-warm"), fixtures.WithInstancePreset(localPreset.ID), fixtures.OnNode(wrongNode.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 0)))
	decision, err := NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(1, 0)}, lease.NewAllocator(), clock, localPreset).
		Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{wrongNode}, Instances: []domain.ModelInstance{wrongWarm}})
	if err != nil {
		t.Fatalf("locality Place: %v", err)
	}
	if decision.Action == domain.ActionWarmInstance {
		t.Fatalf("warm locality mismatch accepted: %+v", decision)
	}

	backendPreset := fixtures.MakePreset(func(p *domain.Preset) { p.Backend = domain.BackendVLLM })
	backendNode := fixtures.MakeNode(func(n *domain.Node) {
		n.Labels = map[string]string{domain.LabelPeerBackend: string(domain.BackendLlamaCpp)}
	})
	backendWarm := fixtures.MakeInstance(fixtures.WithInstanceID("backend-warm"), fixtures.WithInstancePreset(backendPreset.ID), fixtures.OnNode(backendNode.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 0)))
	decision, err = NewPlacer(&mocks.ResourceEstimator{Claim: fixtures.MakeClaim(1, 0)}, lease.NewAllocator(), clock, backendPreset).
		Place(context.Background(), fixtures.MakeJob(), domain.FleetSnapshot{Nodes: []domain.Node{backendNode}, Instances: []domain.ModelInstance{backendWarm}})
	if err != nil {
		t.Fatalf("backend Place: %v", err)
	}
	if decision.Action == domain.ActionWarmInstance {
		t.Fatalf("warm backend mismatch accepted: %+v", decision)
	}
}

func TestPreemptSkipsPresetLocalityAndBackendMismatch(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	victim := func(nodeID string) domain.ModelInstance {
		return fixtures.MakeInstance(fixtures.WithInstanceID("victim"), fixtures.OnNode(nodeID), fixtures.WithClaim(fixtures.MakeClaim(100, 0)), fixtures.WithInstancePriority(domain.PriorityBackground))
	}

	localPreset := fixtures.MakePreset(fixtures.WithPresetNode("right-node"))
	wrongNode := fixtures.MakeNode(fixtures.WithNodeID("wrong-node"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	placer := NewPlacer(unitEstimator{claim: fixtures.MakeClaim(900, 0)}, lease.NewAllocator(), clock, localPreset)
	_, ok, err := placer.tryPreemptForPreset(context.Background(), fixtures.MakeJob(fixtures.Interactive, fixtures.HardForInteractive), localPreset, 100, 1, domain.FleetSnapshot{
		Nodes:     []domain.Node{wrongNode},
		Instances: []domain.ModelInstance{victim(wrongNode.ID)},
	})
	if err != nil || ok {
		t.Fatalf("preempt locality ok=%v err=%v", ok, err)
	}

	backendPreset := fixtures.MakePreset(func(p *domain.Preset) { p.Backend = domain.BackendVLLM })
	backendNode := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1), func(n *domain.Node) {
		n.Labels = map[string]string{domain.LabelPeerBackend: string(domain.BackendLlamaCpp)}
	})
	placer = NewPlacer(unitEstimator{claim: fixtures.MakeClaim(900, 0)}, lease.NewAllocator(), clock, backendPreset)
	_, ok, err = placer.tryPreemptForPreset(context.Background(), fixtures.MakeJob(fixtures.Interactive, fixtures.HardForInteractive), backendPreset, 100, 1, domain.FleetSnapshot{
		Nodes:     []domain.Node{backendNode},
		Instances: []domain.ModelInstance{victim(backendNode.ID)},
	})
	if err != nil || ok {
		t.Fatalf("preempt backend ok=%v err=%v", ok, err)
	}
}

func TestCanReplaceVictimHonorsPresetLocalityAndBackend(t *testing.T) {
	victimPreset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"), fixtures.WithPresetNode("home"))
	victim := fixtures.MakeInstance(fixtures.WithInstanceID("victim"), fixtures.WithInstancePreset(victimPreset.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 0)))
	other := fixtures.MakeNode(fixtures.WithNodeID("other"))
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), victimPreset)
	if _, ok := placer.replacementForVictim(victim, "original", domain.FleetSnapshot{Nodes: []domain.Node{other}}, nil); ok {
		t.Fatal("victim replaced onto wrong preset node")
	}

	backendPreset := fixtures.MakePreset(fixtures.WithPresetID("backend-preset"), func(p *domain.Preset) { p.Backend = domain.BackendVLLM })
	victim = fixtures.MakeInstance(fixtures.WithInstanceID("backend-victim"), fixtures.WithInstancePreset(backendPreset.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 0)))
	llamaNode := fixtures.MakeNode(func(n *domain.Node) {
		n.Labels = map[string]string{domain.LabelPeerBackend: string(domain.BackendLlamaCpp)}
	})
	placer = NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), backendPreset)
	if _, ok := placer.replacementForVictim(victim, "original", domain.FleetSnapshot{Nodes: []domain.Node{llamaNode}}, nil); ok {
		t.Fatal("victim replaced onto wrong backend")
	}
}

func TestEligibleVictimsAndPriorityHelpers(t *testing.T) {
	node := fixtures.MakeNode()
	victims := eligibleVictims(fixtures.MakeJob(fixtures.Interactive), node, []int{0}, []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.WithInstanceID("other-node"), fixtures.OnNode("other"), fixtures.WithInstancePriority(domain.PriorityBackground)),
		fixtures.MakeInstance(fixtures.WithInstanceID("other-acc"), fixtures.OnNode(node.ID), func(i *domain.ModelInstance) { i.AcceleratorSet = []int{1} }, fixtures.WithInstancePriority(domain.PriorityBackground)),
		fixtures.MakeInstance(fixtures.WithInstanceID("pinned"), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityBackground), func(i *domain.ModelInstance) { i.Pinned = true }),
		fixtures.MakeInstance(fixtures.WithInstanceID("same-priority"), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityInteractive)),
		fixtures.MakeInstance(fixtures.WithInstanceID("victim"), fixtures.OnNode(node.ID), fixtures.WithInstancePriority(domain.PriorityBackground)),
	})
	if len(victims) != 1 || victims[0].ID != "victim" {
		t.Fatalf("victims = %+v", victims)
	}
	if priorityRank("") != priorityRank(domain.PriorityNormal) {
		t.Fatal("unknown priority should rank as normal")
	}
	if overlaps(nil, []int{0}) {
		t.Fatal("empty accelerator set should not overlap")
	}
	if overlaps([]int{0}, []int{1}) {
		t.Fatal("different accelerator sets should not overlap")
	}
}

func TestResolveInstanceFillsMissingLoadedClaim(t *testing.T) {
	preset := fixtures.MakePreset()
	node := fixtures.MakeNode()
	service := &Service{
		Nodes:   zeroClaimResolver{node: node},
		Presets: map[string]domain.Preset{preset.ID: preset},
	}
	inst, err := service.resolveInstance(context.Background(), fixtures.MakeJob(fixtures.WithPreset(preset.ID)), domain.PlacementDecision{
		NodeID:         node.ID,
		AcceleratorSet: []int{0},
		Claim:          fixtures.MakeClaim(7, 8),
	}, domain.FleetSnapshot{Nodes: []domain.Node{node}})
	if err != nil {
		t.Fatalf("resolveInstance: %v", err)
	}
	if inst.Claim != (fixtures.MakeClaim(7, 8)) {
		t.Fatalf("instance claim = %+v", inst.Claim)
	}
}

type unitEstimator struct {
	claim domain.Claim
	err   error
}

type countingUnitEstimator struct {
	claim domain.Claim
	calls int
	errOn int
}

type zeroClaimResolver struct {
	node domain.Node
}

func (r zeroClaimResolver) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	if nodeID != r.node.ID {
		return nil, domain.ErrUnreachable
	}
	return zeroClaimAgent{node: r.node}, nil
}

type zeroClaimAgent struct {
	node domain.Node
}

func (a zeroClaimAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{Node: a.node}, nil
}

func (a zeroClaimAgent) Load(context.Context, domain.LoadRequest) (domain.ModelInstance, error) {
	return domain.ModelInstance{ID: "inst-zero", PresetID: "preset_test", NodeID: a.node.ID, AcceleratorSet: []int{0}, State: domain.InstReady}, nil
}

func (a zeroClaimAgent) Unload(context.Context, string) error { return nil }

func (a zeroClaimAgent) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, domain.ErrUnsupported
}

func (a zeroClaimAgent) BeginRequest(context.Context, string) error { return nil }

func (a zeroClaimAgent) EndRequest(context.Context, string) error { return nil }

func (e *countingUnitEstimator) Estimate(context.Context, domain.Preset, int, int) (domain.Claim, error) {
	return e.claim, nil
}

func (e *countingUnitEstimator) EstimateForUnit(context.Context, domain.Preset, int, int, domain.Node, []int) (domain.Claim, error) {
	e.calls++
	if e.calls == e.errOn {
		return domain.Claim{}, domain.ErrUnsupported
	}
	return e.claim, nil
}

func (e unitEstimator) Estimate(context.Context, domain.Preset, int, int) (domain.Claim, error) {
	if e.err != nil {
		return domain.Claim{}, e.err
	}
	return e.claim, nil
}

func (e unitEstimator) EstimateForUnit(context.Context, domain.Preset, int, int, domain.Node, []int) (domain.Claim, error) {
	if e.err != nil {
		return domain.Claim{}, e.err
	}
	return e.claim, nil
}
