package sharding

import (
	"context"
	"errors"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/test/fixtures"
)

func TestPlannerBuildsCrossNodeShardPlan(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("mlx-large"), fixtures.WithWeights(12000), fixtures.WithKVPerToken(0.5), fixtures.WithContextLength(4000))
	nodeA := fixtures.MakeNode(fixtures.WithNodeID("node-a"), fixtures.WithVRAM(10000))
	nodeA.SpeedClass.TokensPerSecRef = 40
	nodeB := fixtures.MakeNode(fixtures.WithNodeID("node-b"), fixtures.WithVRAM(10000))
	nodeB.SpeedClass.TokensPerSecRef = 80
	nodeC := fixtures.MakeNode(fixtures.WithNodeID("node-c"), fixtures.WithVRAM(10000))
	nodeC.SpeedClass.TokensPerSecRef = 60
	planner := Planner{Estimator: estimate.NewInMemory(), Allocator: lease.NewAllocator()}

	plan, err := planner.Plan(context.Background(), preset, domain.FleetSnapshot{Nodes: []domain.Node{nodeA, nodeB, nodeC}}, 2, 0)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.ID != "shard-mlx-large-2" || len(plan.Assignments) != 2 {
		t.Fatalf("plan = %+v", plan)
	}
	if plan.Assignments[0].NodeID != "node-b" || plan.Assignments[1].NodeID != "node-c" {
		t.Fatalf("assignments = %+v", plan.Assignments)
	}
	if plan.Assignments[0].Claim.WeightsMB != 6000 || plan.Assignments[0].Claim.KVReservedMB != 1000 {
		t.Fatalf("claim = %+v", plan.Assignments[0].Claim)
	}
}

func TestPlannerRejectsInvalidAndNoFitPlans(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("too-big"), fixtures.WithWeights(12000))
	planner := Planner{Estimator: estimate.NewInMemory(), Allocator: lease.NewAllocator()}
	if _, err := (Planner{}).Plan(context.Background(), preset, domain.FleetSnapshot{}, 2, 0); err == nil {
		t.Fatal("unconfigured planner accepted")
	}
	if _, err := planner.Plan(context.Background(), domain.Preset{}, domain.FleetSnapshot{}, 2, 0); err == nil {
		t.Fatal("missing preset id accepted")
	}
	if _, err := planner.Plan(context.Background(), preset, domain.FleetSnapshot{}, 1, 0); err == nil {
		t.Fatal("single shard accepted")
	}
	if _, err := planner.Plan(context.Background(), preset, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode(fixtures.WithVRAM(1000))}}, 2, 0); !errors.Is(err, domain.ErrNoFit) {
		t.Fatalf("no-fit err = %v", err)
	}
}

func TestSplitClaimRoundsUp(t *testing.T) {
	got := splitClaim(domain.Claim{WeightsMB: 5, KVReservedMB: 7}, 2)
	if got.WeightsMB != 3 || got.KVReservedMB != 4 {
		t.Fatalf("claim = %+v", got)
	}
}
