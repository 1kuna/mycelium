package pool

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

// Claim math with the in-memory estimator and the default fixture preset:
// weights 5600MB + ceil(8000 tokens * 1 conc * 0.18 MB/token) = 7040MB/replica.
// 4090 usable = 24576*0.90 = 22118 -> 3 replicas.
// Spark usable = 131072*0.90 - 131072*0.05 (catastrophic margin) = 111411 -> 15.
func planFixtures() ([]domain.Preset, domain.FleetSnapshot) {
	preset := fixtures.MakePreset()
	fleet := domain.FleetSnapshot{Nodes: []domain.Node{
		fixtures.Make4090Node(),
		fixtures.MakeSparkNode(),
	}}
	return []domain.Preset{preset}, fleet
}

func buildTestPlan(t *testing.T, presets []domain.Preset, fleet domain.FleetSnapshot, opts Options) Plan {
	t.Helper()
	plan, err := BuildPlan(context.Background(), estimate.NewInMemory(), lease.NewAllocator(), "qwen2.5-9b-instruct", presets, fleet, opts)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	return plan
}

func buildBackendAwareTestPlan(t *testing.T, presets []domain.Preset, fleet domain.FleetSnapshot, opts Options) Plan {
	t.Helper()
	plan, err := BuildPlan(context.Background(), estimate.NewBackendAware(nil, estimate.NewInMemory()), lease.NewAllocator(), "vision", presets, fleet, opts)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	return plan
}

func nodePlan(t *testing.T, plan Plan, nodeID string) NodePlan {
	t.Helper()
	for _, node := range plan.Nodes {
		if node.NodeID == nodeID {
			return node
		}
	}
	t.Fatalf("node %q missing from plan: %+v", nodeID, plan.Nodes)
	return NodePlan{}
}

func TestBuildPlanCountsSafeReplicasPerNode(t *testing.T) {
	presets, fleet := planFixtures()
	plan := buildTestPlan(t, presets, fleet, Options{})

	gpu := nodePlan(t, plan, "node_4090a")
	if gpu.Additional != 3 {
		t.Fatalf("4090 additional = %d, want 3 (claim %+v)", gpu.Additional, gpu.ClaimPerReplica)
	}
	if gpu.SerialColdLoads {
		t.Fatalf("soft-OOM node must not require serial cold loads")
	}
	spark := nodePlan(t, plan, "node_spark")
	if spark.Additional != 15 {
		t.Fatalf("spark additional = %d, want 15", spark.Additional)
	}
	if !spark.SerialColdLoads {
		t.Fatalf("catastrophic node must flag serial cold loads")
	}
	if plan.TotalAdditional != 18 {
		t.Fatalf("total additional = %d, want 18", plan.TotalAdditional)
	}
	if plan.Concurrency != 1 {
		t.Fatalf("default concurrency = %d, want 1", plan.Concurrency)
	}
}

func TestBuildPlanHonorsPerNodeAndTotalCaps(t *testing.T) {
	presets, fleet := planFixtures()

	perNode := buildTestPlan(t, presets, fleet, Options{MaxPerNode: 2})
	if got := nodePlan(t, perNode, "node_spark").Additional; got != 2 {
		t.Fatalf("spark additional under MaxPerNode=2 -> %d", got)
	}
	if perNode.TotalAdditional != 4 {
		t.Fatalf("total under MaxPerNode=2 -> %d, want 4", perNode.TotalAdditional)
	}

	// Nodes are planned in ID order (node_4090a first): 3 fit there, leaving 2.
	total := buildTestPlan(t, presets, fleet, Options{MaxTotal: 5})
	if got := nodePlan(t, total, "node_4090a").Additional; got != 3 {
		t.Fatalf("4090 additional under MaxTotal=5 -> %d", got)
	}
	if got := nodePlan(t, total, "node_spark").Additional; got != 2 {
		t.Fatalf("spark additional under MaxTotal=5 -> %d", got)
	}
	if total.TotalAdditional != 5 {
		t.Fatalf("total additional = %d, want 5", total.TotalAdditional)
	}
}

func TestBuildPlanRefusesNotReadyNode(t *testing.T) {
	presets, fleet := planFixtures()
	fleet.Nodes[0] = fixtures.Make4090Node(fixtures.Maintenance)
	plan := buildTestPlan(t, presets, fleet, Options{})
	gpu := nodePlan(t, plan, "node_4090a")
	if gpu.Additional != 0 || !strings.Contains(gpu.Refusal, "maintenance") {
		t.Fatalf("maintenance node plan = %+v", gpu)
	}
	if gpu.SetupGap == "" {
		t.Fatalf("refusal must carry a setup gap")
	}
}

func TestBuildPlanRefusesBackendMismatch(t *testing.T) {
	presets, fleet := planFixtures()
	fleet.Nodes[0] = fixtures.Make4090Node(func(n *domain.Node) {
		n.Labels[domain.LabelPeerBackends] = "mlx, vllm"
	})
	fleet.Nodes[1] = fixtures.MakeSparkNode(func(n *domain.Node) {
		n.Labels[domain.LabelPeerBackend] = "vllm"
	})
	plan := buildTestPlan(t, presets, fleet, Options{})
	for _, id := range []string{"node_4090a", "node_spark"} {
		node := nodePlan(t, plan, id)
		if node.Additional != 0 || node.Refusal != "no compatible backend runtime" {
			t.Fatalf("%s plan = %+v", id, node)
		}
	}
}

func TestBuildPlanRefusesRemoteArtifactAndUsesPerNodePresets(t *testing.T) {
	sparkPreset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "preset_spark"
		p.NodeID = "node_spark"
	})
	_, fleet := planFixtures()

	plan := buildTestPlan(t, []domain.Preset{sparkPreset}, fleet, Options{MaxPerNode: 1})
	gpu := nodePlan(t, plan, "node_4090a")
	if gpu.Additional != 0 || gpu.Refusal != "model artifact not local" {
		t.Fatalf("4090 plan = %+v", gpu)
	}
	if !strings.Contains(gpu.SetupGap, "stage the model artifact") {
		t.Fatalf("setup gap should point at staging, got %q", gpu.SetupGap)
	}
	spark := nodePlan(t, plan, "node_spark")
	if spark.Additional != 1 || spark.PresetID != "preset_spark" {
		t.Fatalf("spark plan = %+v", spark)
	}

	// A second per-node preset makes both nodes plannable.
	gpuPreset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "preset_4090"
		p.NodeID = "node_4090a"
	})
	both := buildTestPlan(t, []domain.Preset{sparkPreset, gpuPreset}, fleet, Options{MaxPerNode: 1})
	if got := nodePlan(t, both, "node_4090a").PresetID; got != "preset_4090" {
		t.Fatalf("4090 preset = %q", got)
	}
	if got := nodePlan(t, both, "node_spark").PresetID; got != "preset_spark" {
		t.Fatalf("spark preset = %q", got)
	}
}

func TestBuildPlanCountsExistingReplicasAndReducedFit(t *testing.T) {
	presets, fleet := planFixtures()
	claim := domain.Claim{WeightsMB: 5600, KVReservedMB: 1440}
	fleet.Instances = []domain.ModelInstance{
		{ID: "inst_ready", PresetID: presets[0].ID, NodeID: "node_4090a", AcceleratorSet: []int{0}, Claim: claim, State: domain.InstReady},
		{ID: "inst_loading", PresetID: presets[0].ID, NodeID: "node_4090a", AcceleratorSet: []int{0}, Claim: claim, State: domain.InstLoading},
		{ID: "inst_stopping", PresetID: presets[0].ID, NodeID: "node_4090a", AcceleratorSet: []int{0}, Claim: claim, State: domain.InstStopping},
		{ID: "inst_other_preset", PresetID: "preset_other", NodeID: "node_4090a", AcceleratorSet: []int{0}, Claim: claim, State: domain.InstReady},
	}
	plan := buildTestPlan(t, presets, fleet, Options{})
	gpu := nodePlan(t, plan, "node_4090a")
	if gpu.Existing != 2 {
		t.Fatalf("existing replicas = %d, want 2 (ready+loading of matching preset)", gpu.Existing)
	}
	if gpu.Additional != 0 || gpu.Refusal != "warm instance concurrency envelope is full" {
		t.Fatalf("full node plan = %+v", gpu)
	}
	if plan.TotalExisting != 2 {
		t.Fatalf("total existing = %d, want 2", plan.TotalExisting)
	}
}

func TestBuildPlanIgnoresErrorInstancesForCapacity(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision"
		p.Aliases = []string{"vision"}
		p.NodeID = "node_spark"
		p.ContextLength = 49152
		p.EstWeightsMB = 1000
		p.KVPerTokenMB = 0
	})
	fleet := domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeSparkNode()}}
	staleClaim := domain.Claim{WeightsMB: 56000}
	fleet.Instances = []domain.ModelInstance{
		{ID: "inst_error_a", PresetID: preset.ID, NodeID: "node_spark", AcceleratorSet: []int{0}, Claim: staleClaim, State: domain.InstError},
		{ID: "inst_error_b", PresetID: preset.ID, NodeID: "node_spark", AcceleratorSet: []int{0}, Claim: staleClaim, State: domain.InstError},
	}

	plan := buildTestPlan(t, []domain.Preset{preset}, fleet, Options{ContextLength: 49152, MaxTotal: 1})
	spark := nodePlan(t, plan, "node_spark")
	if spark.Existing != 0 {
		t.Fatalf("error instances counted as existing replicas: %+v", spark)
	}
	if spark.Additional != 1 {
		t.Fatalf("error instances consumed capacity: %+v", spark)
	}
}

func TestBuildPlanUsesLeaseAwareWarmHeadroom(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision"
		p.Aliases = []string{"vision"}
		p.NodeID = "node_spark"
		p.ContextLength = 49152
		p.EstWeightsMB = 1000
		p.KVPerTokenMB = 0
	})
	fleet := domain.FleetSnapshot{
		Nodes: []domain.Node{fixtures.MakeSparkNode()},
		Instances: []domain.ModelInstance{{
			ID:             "inst_ready",
			PresetID:       preset.ID,
			NodeID:         "node_spark",
			AcceleratorSet: []int{0},
			Claim:          domain.Claim{WeightsMB: 56000},
			State:          domain.InstReady,
		}},
	}

	plan := buildTestPlan(t, []domain.Preset{preset}, fleet, Options{ContextLength: 49152, MaxTotal: 3, MaxPerNode: 2})
	spark := nodePlan(t, plan, "node_spark")
	if spark.Existing != 1 {
		t.Fatalf("existing = %d, want warm instance counted", spark.Existing)
	}
	if spark.Additional != 1 {
		t.Fatalf("warm headroom additional = %d, want 1: %+v", spark.Additional, spark)
	}
	if plan.TotalAdditional != 1 {
		t.Fatalf("total additional = %d, want 1", plan.TotalAdditional)
	}

	fleet.Instances[0].InFlight = 1
	full := buildTestPlan(t, []domain.Preset{preset}, fleet, Options{ContextLength: 49152, MaxTotal: 3, MaxPerNode: 2})
	fullSpark := nodePlan(t, full, "node_spark")
	if fullSpark.Additional != 0 || fullSpark.Refusal != "warm instance concurrency envelope is full" {
		t.Fatalf("full warm envelope plan = %+v", fullSpark)
	}

	fleet.Instances[0].InFlight = 0
	fleet.Instances[0].LeaseID = "lease-active"
	leased := buildTestPlan(t, []domain.Preset{preset}, fleet, Options{ContextLength: 49152, MaxTotal: 3, MaxPerNode: 2})
	leasedSpark := nodePlan(t, leased, "node_spark")
	if leasedSpark.Additional != 0 || leasedSpark.Refusal != "warm instance concurrency envelope is full" {
		t.Fatalf("leased warm envelope plan = %+v", leasedSpark)
	}
}

func TestBuildPlanUsesVLLMUnitReservationForColdReplicas(t *testing.T) {
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.ID = "vision-vllm"
		p.Aliases = []string{"vision"}
		p.Backend = domain.BackendVLLM
		p.NodeID = "node_spark"
		p.ContextLength = 49152
		p.EstWeightsMB = 1000
		p.KVPerTokenMB = 0
		p.LaunchArgs = []string{"--gpu-memory-utilization", "0.45"}
	})
	fleet := domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeSparkNode(func(n *domain.Node) {
		n.Accelerators[0].VRAMTotalMB = 124610
		n.Accelerators[0].VRAMUsedMB = 3285
	})}}

	plan := buildBackendAwareTestPlan(t, []domain.Preset{preset}, fleet, Options{ContextLength: 49152, MaxTotal: 3, MaxPerNode: 2})
	spark := nodePlan(t, plan, "node_spark")
	if spark.Additional != 1 {
		t.Fatalf("vLLM reservation additional = %d, want 1: %+v", spark.Additional, spark)
	}
	if spark.ClaimPerReplica.WeightsMB != 56075 {
		t.Fatalf("vLLM reservation claim = %+v, want weights 56075", spark.ClaimPerReplica)
	}
	if plan.TotalAdditional != 1 {
		t.Fatalf("total additional = %d, want 1", plan.TotalAdditional)
	}
}

func TestBuildPlanUsesContextAndConcurrencyOverrides(t *testing.T) {
	_, fleet := planFixtures()
	// The preset must actually support the requested context (see the
	// undersized-context refusal test); use a 32k preset with a 16k override.
	presets := []domain.Preset{fixtures.MakePreset(func(p *domain.Preset) {
		p.ContextLength = 32768
	})}
	// 16k context at concurrency 2: 5600 + ceil(16384*2*0.18) = 11499MB.
	plan := buildTestPlan(t, presets, fleet, Options{ContextLength: 16384, Concurrency: 2})
	gpu := nodePlan(t, plan, "node_4090a")
	if gpu.ClaimPerReplica.KVReservedMB != 5899 {
		t.Fatalf("kv reservation = %d, want 5899", gpu.ClaimPerReplica.KVReservedMB)
	}
	if gpu.Additional != 1 {
		t.Fatalf("4090 additional at 16k/x2 = %d, want 1", gpu.Additional)
	}
	if gpu.ContextLength != 16384 || plan.Concurrency != 2 {
		t.Fatalf("plan did not record overrides: %+v", gpu)
	}
}

func TestBuildPlanInputValidation(t *testing.T) {
	presets, fleet := planFixtures()
	alloc := lease.NewAllocator()
	est := estimate.NewInMemory()
	cases := []struct {
		name string
		run  func() error
	}{
		{"nil estimator", func() error {
			_, err := BuildPlan(context.Background(), nil, alloc, "m", presets, fleet, Options{})
			return err
		}},
		{"nil allocator", func() error {
			_, err := BuildPlan(context.Background(), est, nil, "m", presets, fleet, Options{})
			return err
		}},
		{"empty model", func() error {
			_, err := BuildPlan(context.Background(), est, alloc, "  ", presets, fleet, Options{})
			return err
		}},
		{"no presets", func() error {
			_, err := BuildPlan(context.Background(), est, alloc, "m", nil, fleet, Options{})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.run() == nil {
				t.Fatalf("expected error")
			}
		})
	}
}

func TestBuildPlanPropagatesEstimatorErrors(t *testing.T) {
	presets, fleet := planFixtures()
	broken := &mocks.ResourceEstimator{Err: fmt.Errorf("boom")}
	_, err := BuildPlan(context.Background(), broken, lease.NewAllocator(), "m", presets, fleet, Options{})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("estimator error not propagated: %v", err)
	}
}

func TestBuildPlanRefusesUndersizedContextPreset(t *testing.T) {
	presets, fleet := planFixtures() // preset context is 8000
	plan := buildTestPlan(t, presets, fleet, Options{ContextLength: 49152})
	for _, id := range []string{"node_4090a", "node_spark"} {
		node := nodePlan(t, plan, id)
		if node.Additional != 0 || !strings.Contains(node.Refusal, "context cap 8000 below requested 49152") {
			t.Fatalf("%s plan = %+v", id, node)
		}
		if !strings.Contains(node.SetupGap, "larger-context preset") {
			t.Fatalf("%s setup gap = %q", id, node.SetupGap)
		}
	}
	if plan.TotalAdditional != 0 {
		t.Fatalf("total additional = %d, want 0", plan.TotalAdditional)
	}
}
