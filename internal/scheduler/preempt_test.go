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
