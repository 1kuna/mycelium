package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestTuneLaunchForPlacementAddsLlamaArgs(t *testing.T) {
	node := tuningNode()
	preset := fixtures.MakePreset(fixtures.WithPresetID("llama"), fixtures.WithLaunchArgs("--flash-attn"))
	decision := domain.PlacementDecision{AcceleratorSet: []int{0, 1}}

	got, err := tuneLaunchForPlacement(preset, decision, node)
	if err != nil {
		t.Fatalf("tuneLaunchForPlacement: %v", err)
	}
	if strings.Join(got.LaunchArgs, " ") != "--flash-attn --n-gpu-layers 999 --tensor-split 100,200" {
		t.Fatalf("launch args = %+v", got.LaunchArgs)
	}
}

func TestTuneLaunchForPlacementPreservesExplicitAndNonLlamaArgs(t *testing.T) {
	node := tuningNode()
	decision := domain.PlacementDecision{AcceleratorSet: []int{0, 1}}
	mlx := fixtures.MakePreset(fixtures.WithPresetID("mlx"), fixtures.WithLaunchArgs("--ctx", "4096"))
	mlx.Backend = domain.BackendMLX
	if got, err := tuneLaunchForPlacement(mlx, decision, node); err != nil || strings.Join(got.LaunchArgs, " ") != "--ctx 4096" {
		t.Fatalf("mlx tune = %+v %v", got.LaunchArgs, err)
	}

	preset := fixtures.MakePreset(fixtures.WithLaunchArgs("--n-gpu-layers=12", "-ts", "1,1"))
	got, err := tuneLaunchForPlacement(preset, decision, node)
	if err != nil {
		t.Fatalf("explicit tune: %v", err)
	}
	if strings.Join(got.LaunchArgs, " ") != "--n-gpu-layers=12 -ts 1,1" {
		t.Fatalf("explicit args = %+v", got.LaunchArgs)
	}
}

func TestTuneLaunchForPlacementFailsOnBadTensorSplitInputs(t *testing.T) {
	node := tuningNode()
	if _, err := tuneLaunchForPlacement(fixtures.MakePreset(), domain.PlacementDecision{AcceleratorSet: []int{0, 2}}, node); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing accelerator err = %v", err)
	}
	node.Accelerators[1].VRAMTotalMB = 0
	if _, err := tuneLaunchForPlacement(fixtures.MakePreset(), domain.PlacementDecision{AcceleratorSet: []int{0, 1}}, node); err == nil || !strings.Contains(err.Error(), "invalid vram") {
		t.Fatalf("invalid vram err = %v", err)
	}
}

func TestServicePassesLaunchTuningToColdLoad(t *testing.T) {
	clock := mocks.NewFakeClock(time.Unix(1, 0).UTC())
	node := tuningNode()
	agent := mocks.NewNodeAgent(node)
	admission := &mocks.AdmissionController{}
	preset := fixtures.MakePreset(fixtures.WithPresetID("llama"))
	service := &Service{
		Placer: fakePlacer{decision: domain.PlacementDecision{
			JobID:          "job-a",
			NodeID:         node.ID,
			AcceleratorSet: []int{0, 1},
			Claim:          fixtures.MakeClaim(1, 1),
			Action:         domain.ActionLoadedNew,
		}},
		Fleet:  staticFleet{fleet: domain.FleetSnapshot{Nodes: []domain.Node{node}}},
		Nodes:  staticNodes{agents: map[string]*mocks.NodeAgent{node.ID: agent}, admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Owners: staticNodes{admissions: map[string]ports.AdmissionController{node.ID: admission}},
		Queue:  NewQueue(clock),
		Store:  &runtimeStore{},
		Clock:  clock,
		Presets: map[string]domain.Preset{
			preset.ID: preset,
		},
	}

	if _, err := service.Submit(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a"), fixtures.WithPreset(preset.ID))); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if len(agent.Loaded) != 1 || strings.Join(agent.Loaded[0].Preset.LaunchArgs, " ") != "--n-gpu-layers 999 --tensor-split 100,200" {
		t.Fatalf("loaded presets = %+v", agent.Loaded)
	}
	if !sameIntSet(agent.Loaded[0].AcceleratorSet, []int{0, 1}) || agent.Loaded[0].Claim != (fixtures.MakeClaim(1, 1)) {
		t.Fatalf("loaded request = %+v", agent.Loaded[0])
	}
}

func tuningNode() domain.Node {
	node := fixtures.MakeNode(fixtures.WithNodeID("node-tune"))
	node.Accelerators = []domain.Accelerator{
		{Index: 0, Vendor: "apple", Kind: "unified", VRAMTotalMB: 100, UnifiedMemory: true},
		{Index: 1, Vendor: "apple", Kind: "unified", VRAMTotalMB: 200, UnifiedMemory: true},
	}
	return node
}

func sameIntSet(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
