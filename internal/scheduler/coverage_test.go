package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestResolvePresetBranches(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset_a"), fixtures.WithModelRef("model_a"), fixtures.WithAliases("alias_a"))
	blankModel := fixtures.MakePreset(fixtures.WithPresetID("preset_blank"), fixtures.WithModelRef(""))
	placer := NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{}, mocks.NewFakeClock(time.Now()), preset, blankModel)

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
	placer := NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{}, mocks.NewFakeClock(time.Now()), preset)
	busy := fixtures.MakeInstance(fixtures.WithInstanceID("a_busy"), fixtures.WithInstancePreset(preset.ID))
	busy.InFlight = 2
	later := fixtures.MakeInstance(fixtures.WithInstanceID("z_idle"), fixtures.WithInstancePreset(preset.ID))
	later.InFlight = 0
	earlier := fixtures.MakeInstance(fixtures.WithInstanceID("a_idle"), fixtures.WithInstancePreset(preset.ID))
	earlier.InFlight = 0

	got, ok := placer.selectWarmInstance(fixtures.MakeJob(), preset, domain.FleetSnapshot{
		Nodes:     []domain.Node{fixtures.MakeNode()},
		Instances: []domain.ModelInstance{busy, later, earlier},
	})
	if !ok || got.ID != "a_idle" {
		t.Fatalf("warm = %+v ok=%v", got, ok)
	}
	if _, ok := placer.selectWarmInstance(fixtures.MakeJob(fixtures.Latency), preset, domain.FleetSnapshot{}); ok {
		t.Fatal("latency job should skip warm batching")
	}
	if _, ok := placer.selectWarmInstance(fixtures.MakeJob(), preset, domain.FleetSnapshot{}); ok {
		t.Fatal("empty fleet should not return warm instance")
	}
}

func TestFilterCandidatesDropsStatusLoadAndFit(t *testing.T) {
	claim := fixtures.MakeClaim(1, 1)
	down := fixtures.MakeNode(fixtures.WithNodeID("down"), fixtures.Maintenance)
	noFit := fixtures.MakeNode(fixtures.WithNodeID("nofit"))
	noStack := fixtures.MakeSparkNode(fixtures.WithNodeID("nostack"))
	fit := fixtures.MakeNode(fixtures.WithNodeID("fit"))

	placer := NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{FitsVal: false, CanStackLoadVal: true}, mocks.NewFakeClock(time.Now()))
	candidates, trace := placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{down, noFit}}, claim)
	if len(candidates) != 0 {
		t.Fatalf("candidates = %+v", candidates)
	}
	dropped := trace.Data["dropped"].(map[string]string)
	if dropped["down"] != string(domain.NodeMaintenance) || dropped["nofit"] != "fit" {
		t.Fatalf("dropped = %+v", dropped)
	}

	placer = NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{FitsVal: true, CanStackLoadVal: false}, mocks.NewFakeClock(time.Now()))
	_, trace = placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{noStack}}, claim)
	dropped = trace.Data["dropped"].(map[string]string)
	if dropped["nostack"] != "load_in_flight" {
		t.Fatalf("dropped = %+v", dropped)
	}

	placer = NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{FitsVal: true, CanStackLoadVal: true}, mocks.NewFakeClock(time.Now()))
	candidates, _ = placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{fit}}, claim)
	if len(candidates) != 1 || candidates[0].node.ID != "fit" {
		t.Fatalf("fit candidates = %+v", candidates)
	}

	multiAcc := fixtures.MakeNode(fixtures.WithNodeID("multi"), func(n *domain.Node) {
		n.Accelerators = []domain.Accelerator{{Index: 1, VRAMTotalMB: 1000}, {Index: 0, VRAMTotalMB: 1000}}
	})
	candidates, _ = placer.filterCandidates(domain.FleetSnapshot{Nodes: []domain.Node{multiAcc}}, claim)
	if len(candidates) != 2 || candidates[0].acc[0] != 0 {
		t.Fatalf("accelerator sort = %+v", candidates)
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
	placer := NewPlacer(&mocks.ResourceEstimator{}, &mocks.Allocator{}, mocks.NewFakeClock(time.Now()))

	scored := placer.scoreCandidates(fixtures.MakeJob(), []candidate{
		{node: fixtures.MakeNode(fixtures.WithNodeID("b")), acc: []int{0}},
		{node: fixtures.MakeNode(fixtures.WithNodeID("a")), acc: []int{0}},
	})
	if scored[0].candidate.node.ID != "a" {
		t.Fatalf("node id tie-break failed: %+v", scored)
	}

	scored = placer.scoreCandidates(fixtures.MakeJob(), []candidate{
		{node: node, acc: []int{1}},
		{node: node, acc: []int{0}},
	})
	if scored[0].candidate.acc[0] != 0 {
		t.Fatalf("accelerator tie-break failed: %+v", scored)
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
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Now()), preset)

	decision, err := placer.Place(context.Background(),
		fixtures.MakeJob(fixtures.Hard, fixtures.WithPreset(preset.ID)),
		domain.FleetSnapshot{Nodes: []domain.Node{target, other}, Instances: []domain.ModelInstance{victim}})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if len(decision.Requeued) != 0 {
		t.Fatalf("victim should replace, requeued = %+v", decision.Requeued)
	}
	assertTraceContains(t, decision.Trace, "replace", "replaced")
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
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Now()), preset)

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
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Now()), preset)

	result, ok := placer.tryPreempt(
		fixtures.MakeJob(fixtures.Interactive, fixtures.HardForInteractive, fixtures.WithPreset(preset.ID)),
		domain.FleetSnapshot{Nodes: []domain.Node{nodeA, nodeB}, Instances: instances},
		fixtures.MakeClaim(800, 0))
	if !ok || result.victim.ID != "victim-a" {
		t.Fatalf("preempt = %+v ok=%v", result, ok)
	}
}

func TestEligibleVictimsAndPriorityHelpers(t *testing.T) {
	node := fixtures.MakeNode()
	victims := eligibleVictims(fixtures.MakeJob(fixtures.Interactive), node, []int{0}, []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.WithInstanceID("other-node"), fixtures.OnNode("other"), fixtures.WithInstancePriority(domain.PriorityBackground)),
		fixtures.MakeInstance(fixtures.WithInstanceID("other-acc"), fixtures.OnNode(node.ID), func(i *domain.ModelInstance) { i.AcceleratorSet = []int{1} }, fixtures.WithInstancePriority(domain.PriorityBackground)),
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
