package e2e

import (
	"context"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/internal/scheduler"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestLargeModelBalancingKeepsSparkUnderCap(t *testing.T) {
	spark := fixtures.MakeSparkNode(fixtures.WithVRAM(122000), fixtures.WithMaxUtil(0.90))
	b70 := fixtures.MakeNode(fixtures.WithNodeID("b70"), fixtures.WithVRAM(32768), fixtures.WithMaxUtil(0.85))
	b70.Labels = map[string]string{"gpu.vendor": "intel", "gpu.kind": "arc-pro-b70"}
	b70.SpeedClass.TokensPerSecRef = 70
	macMini := fixtures.MakeNode(fixtures.WithNodeID("mac-mini"), fixtures.WithVRAM(64000), fixtures.WithMaxUtil(0.80))
	macMini.Labels = map[string]string{"gpu.vendor": "apple", "memory.class": "unified"}
	macMini.SpeedClass.TokensPerSecRef = 45

	preset122B := fixtures.MakePreset(
		fixtures.WithPresetID("qwen35-122b-q4"),
		fixtures.WithModelRef("qwen35-122b"),
		fixtures.WithWeights(76000),
		fixtures.WithArtifactSize(76000),
		fixtures.WithKVPerToken(0),
	)
	instances := []domain.ModelInstance{
		instance("inst_9b_a", spark.ID, fixtures.MakeClaim(7000, 500), domain.PriorityInteractive),
		instance("inst_9b_b", spark.ID, fixtures.MakeClaim(7000, 500), domain.PriorityInteractive),
		instance("inst_9b_c", spark.ID, fixtures.MakeClaim(7000, 500), domain.PriorityInteractive),
		instance("inst_27b_background", spark.ID, fixtures.MakeClaim(18000, 2000), domain.PriorityBackground),
	}
	fleet := domain.FleetSnapshot{Nodes: []domain.Node{spark, b70, macMini}, Instances: instances}
	placer := scheduler.NewPlacer(
		estimate.NewInMemory(),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Date(2026, 6, 2, 9, 0, 0, 0, time.UTC)),
		preset122B,
	)

	decision, err := placer.Place(context.Background(), fixtures.MakeJob(
		fixtures.WithJobID("job_122b"),
		fixtures.WithPreset(preset122B.ID),
		fixtures.Interactive,
		fixtures.HardForInteractive,
	), fleet)
	if err != nil {
		t.Fatalf("Place 122B: %v", err)
	}
	if decision.Action != domain.ActionHardPreempted || decision.NodeID != spark.ID {
		t.Fatalf("decision = %+v", decision)
	}
	if len(decision.Preempted) != 1 || decision.Preempted[0] != "inst_27b_background" || len(decision.Requeued) != 0 {
		t.Fatalf("preempted=%+v requeued=%+v", decision.Preempted, decision.Requeued)
	}
	allocator := lease.NewAllocator()
	if !allocator.Fits(spark, []int{0}, remove(instances, "inst_27b_background"), decision.Claim) {
		t.Fatal("122B placement would exceed Spark capped usable memory")
	}
	if allocator.Fits(spark, []int{0}, remove(instances, "inst_27b_background"), fixtures.MakeClaim(decision.Claim.WeightsMB+6000, 0)) {
		t.Fatal("Spark accepted a claim above its capped usable memory")
	}
	if !allocator.Fits(b70, []int{0}, nil, fixtures.MakeClaim(18000, 2000)) {
		t.Fatal("27B victim should fit on B70 replacement capacity")
	}
}

func TestSparkVLLMReservationCannotReachNinetyPercent(t *testing.T) {
	spark := fixtures.MakeSparkNode(fixtures.WithVRAM(122000), fixtures.WithMaxUtil(0.90))
	estimator := estimate.NewBackendAware(nil, estimate.NewInMemory())
	allocator := lease.NewAllocator()
	preset := fixtures.MakePreset(fixtures.WithPresetID("spark-vllm"), fixtures.WithModelRef("qwen35-122b"))
	preset.Backend = domain.BackendVLLM
	preset.LaunchArgs = []string{"serve", "{model}", "--gpu-memory-utilization", "0.90"}

	claim, err := estimator.EstimateForUnit(context.Background(), preset, 4096, 1, spark, []int{0})
	if err != nil {
		t.Fatalf("EstimateForUnit 0.90: %v", err)
	}
	if allocator.Fits(spark, []int{0}, nil, claim) {
		t.Fatalf("Spark accepted 90%% vLLM reservation: %+v", claim)
	}

	preset.LaunchArgs = []string{"serve", "{model}", "--gpu-memory-utilization", "0.85"}
	claim, err = estimator.EstimateForUnit(context.Background(), preset, 4096, 1, spark, []int{0})
	if err != nil {
		t.Fatalf("EstimateForUnit 0.85: %v", err)
	}
	if !allocator.Fits(spark, []int{0}, nil, claim) {
		t.Fatalf("Spark rejected capped vLLM reservation: %+v", claim)
	}
}
