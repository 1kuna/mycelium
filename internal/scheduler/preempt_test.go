package scheduler

import (
	"context"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestPlaceHardPreemptsLowestPriorityVictim(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset_120b"), fixtures.WithWeights(800), fixtures.WithKVPerToken(0))
	spark := fixtures.MakeSparkNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.90))
	victim := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst_background"),
		fixtures.OnNode(spark.ID),
		fixtures.WithClaim(fixtures.MakeClaim(100, 0)),
		fixtures.WithInstancePriority(domain.PriorityBackground),
	)
	victim.InFlight = 1
	normal := fixtures.MakeInstance(
		fixtures.WithInstanceID("inst_normal"),
		fixtures.OnNode(spark.ID),
		fixtures.WithClaim(fixtures.MakeClaim(1, 0)),
		fixtures.WithInstancePriority(domain.PriorityNormal),
	)
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(),
		fixtures.MakeJob(fixtures.Interactive, fixtures.HardForInteractive, fixtures.WithPreset(preset.ID)),
		domain.FleetSnapshot{Nodes: []domain.Node{spark}, Instances: []domain.ModelInstance{normal, victim}})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionHardPreempted {
		t.Fatalf("action = %s", decision.Action)
	}
	if len(decision.Preempted) != 1 || decision.Preempted[0] != "inst_background" {
		t.Fatalf("preempted = %+v", decision.Preempted)
	}
	if len(decision.Requeued) != 1 || decision.Requeued[0] != "inst_background" {
		t.Fatalf("requeued = %+v", decision.Requeued)
	}
}

func TestHardForInteractiveDoesNotPreemptForNormalJob(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(800), fixtures.WithKVPerToken(0))
	node := fixtures.Make4090Node(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.80))
	existing := []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.OnNode(node.ID), fixtures.WithInstancePreset("other"), fixtures.WithClaim(fixtures.MakeClaim(100, 0)), fixtures.WithInstancePriority(domain.PriorityBackground)),
	}
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(fixtures.HardForInteractive), domain.FleetSnapshot{Nodes: []domain.Node{node}, Instances: existing})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionQueued {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestPreemptSkipsDiskUnsafeTarget(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithWeights(800), fixtures.WithKVPerToken(0), fixtures.WithArtifactSize(100))
	node := fixtures.Make4090Node(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.90), fixtures.WithDisk(1000, 250))
	victim := fixtures.MakeInstance(
		fixtures.OnNode(node.ID),
		fixtures.WithInstancePreset("other"),
		fixtures.WithClaim(fixtures.MakeClaim(100, 0)),
		fixtures.WithInstancePriority(domain.PriorityBackground),
	)
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(fixtures.Interactive, fixtures.HardForInteractive, fixtures.WithPreset(preset.ID)), domain.FleetSnapshot{
		Nodes:     []domain.Node{node},
		Instances: []domain.ModelInstance{victim},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if decision.Action != domain.ActionQueued {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestCanReplaceVictimSkipsDiskUnsafeReplacement(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"), fixtures.WithArtifactSize(100))
	fullDisk := fixtures.MakeNode(fixtures.WithNodeID("full-disk"), fixtures.WithDisk(1000, 260), fixtures.WithVRAM(1000))
	victim := fixtures.MakeInstance(
		fixtures.WithInstancePreset(preset.ID),
		fixtures.WithClaim(fixtures.MakeClaim(10, 1)),
	)
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	if _, ok := placer.replacementForVictim(victim, "original", domain.FleetSnapshot{Nodes: []domain.Node{fullDisk}}, nil); ok {
		t.Fatal("disk-unsafe replacement accepted")
	}
	if !preemptNodeAllowed(fullDisk, []func(domain.Node) bool{func(domain.Node) bool { return true }}) {
		t.Fatal("true guard rejected")
	}
	if preemptNodeAllowed(fullDisk, []func(domain.Node) bool{nil, func(domain.Node) bool { return false }}) {
		t.Fatal("false guard accepted")
	}
}

func TestReplacementForVictimSkipsCatastrophicLoadingTarget(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("victim-preset"))
	spark := fixtures.MakeSparkNode(fixtures.WithNodeID("spark"), fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.90))
	victim := fixtures.MakeInstance(
		fixtures.WithInstanceID("victim"),
		fixtures.WithInstancePreset(preset.ID),
		fixtures.WithClaim(fixtures.MakeClaim(10, 1)),
	)
	loading := fixtures.MakeInstance(
		fixtures.WithInstanceID("loading"),
		fixtures.OnNode(spark.ID),
		fixtures.WithClaim(fixtures.MakeClaim(10, 0)),
		func(i *domain.ModelInstance) {
			i.State = domain.InstLoading
			i.Loading = true
		},
	)
	placer := NewPlacer(estimate.NewInMemory(), lease.NewAllocator(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), preset)

	if _, ok := placer.replacementForVictim(victim, "original", domain.FleetSnapshot{Nodes: []domain.Node{spark}}, []domain.ModelInstance{loading}); ok {
		t.Fatal("replacement stacked onto catastrophic loading target")
	}
}

func TestVictimTieBreaksByInFlightThenID(t *testing.T) {
	left := fixtures.MakeInstance(fixtures.WithInstanceID("b"), fixtures.WithInstancePriority(domain.PriorityBackground))
	right := fixtures.MakeInstance(fixtures.WithInstanceID("a"), fixtures.WithInstancePriority(domain.PriorityBackground))
	left.InFlight = 0
	right.InFlight = 0
	if !victimLess(right, left) {
		t.Fatal("expected id tie-break")
	}
	left.InFlight = 1
	if !victimLess(right, left) {
		t.Fatal("expected lower in-flight to win")
	}
}
