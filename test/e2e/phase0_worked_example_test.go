package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/internal/scheduler"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestPhase0WorkedExampleHardPreemptsBackground27B(t *testing.T) {
	spark := fixtures.MakeSparkNode(fixtures.WithVRAM(120000), fixtures.WithMaxUtil(0.90))
	box4090 := fixtures.Make4090Node(fixtures.WithVRAM(24000), fixtures.WithMaxUtil(0.90))
	preset120B := fixtures.MakePreset(
		fixtures.WithPresetID("preset_120b_ctx8k_q4"),
		fixtures.WithModelRef("qwen-120b"),
		fixtures.WithWeights(76000),
		fixtures.WithKVPerToken(0),
	)

	instances := []domain.ModelInstance{
		instance("inst_9b_a", spark.ID, fixtures.MakeClaim(7000, 500), domain.PriorityInteractive),
		instance("inst_9b_b", spark.ID, fixtures.MakeClaim(7000, 500), domain.PriorityInteractive),
		instance("inst_9b_c", spark.ID, fixtures.MakeClaim(7000, 500), domain.PriorityInteractive),
		instance("inst_1b_asr", spark.ID, fixtures.MakeClaim(1200, 200), domain.PriorityInteractive),
		instance("inst_27b_background", spark.ID, fixtures.MakeClaim(18000, 2000), domain.PriorityBackground),
	}
	fleet := domain.FleetSnapshot{
		Nodes:     []domain.Node{spark, box4090},
		Instances: instances,
	}
	placer := scheduler.NewPlacer(
		estimate.NewInMemory(),
		lease.NewAllocator(),
		mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		preset120B,
	)

	decision, err := placer.Place(context.Background(),
		fixtures.MakeJob(
			fixtures.WithJobID("job_120b"),
			fixtures.WithPreset(preset120B.ID),
			fixtures.Interactive,
			fixtures.HardForInteractive,
		),
		fleet)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if decision.Action != domain.ActionHardPreempted {
		t.Fatalf("action = %s", decision.Action)
	}
	if decision.NodeID != spark.ID {
		t.Fatalf("node = %s", decision.NodeID)
	}
	if len(decision.Preempted) != 1 || decision.Preempted[0] != "inst_27b_background" {
		t.Fatalf("preempted = %+v", decision.Preempted)
	}
	if len(decision.Requeued) != 0 {
		t.Fatalf("background 27B should fit on 4090, requeued = %+v", decision.Requeued)
	}
	for _, step := range []string{"estimate", "filter", "preempt", "replace"} {
		if !traceHasStep(decision.Trace, step) {
			t.Fatalf("trace missing %s: %+v", step, decision.Trace)
		}
	}
	if !traceContains(decision.Trace, "preempt", "inst_27b_background") {
		t.Fatalf("preempt trace missing victim: %+v", decision.Trace)
	}
	if !lease.NewAllocator().Fits(spark, []int{0}, remove(instances, "inst_27b_background"), decision.Claim) {
		t.Fatal("resulting Spark placement exceeds max_util")
	}
	if !lease.NewAllocator().Fits(box4090, []int{0}, nil, fixtures.MakeClaim(18000, 2000)) {
		t.Fatal("test fixture is wrong: background 27B must fit on 4090 replacement target")
	}
}

func instance(id, nodeID string, claim domain.Claim, priority domain.Priority) domain.ModelInstance {
	return fixtures.MakeInstance(
		fixtures.WithInstanceID(id),
		fixtures.WithInstancePreset(id+"_preset"),
		fixtures.OnNode(nodeID),
		fixtures.WithClaim(claim),
		fixtures.WithInstancePriority(priority),
	)
}

func remove(instances []domain.ModelInstance, id string) []domain.ModelInstance {
	out := make([]domain.ModelInstance, 0, len(instances))
	for _, inst := range instances {
		if inst.ID != id {
			out = append(out, inst)
		}
	}
	return out
}

func traceHasStep(trace []domain.TraceStep, step string) bool {
	for _, got := range trace {
		if got.Step == step {
			return true
		}
	}
	return false
}

func traceContains(trace []domain.TraceStep, step, text string) bool {
	for _, got := range trace {
		if got.Step == step && strings.Contains(got.Result, text) {
			return true
		}
	}
	return false
}
