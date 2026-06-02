package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"mycelium/internal/bench"
	"mycelium/internal/domain"
	"mycelium/test/mocks"
)

func TestFleetBenchmarkConservativeSimulationAllocatesAcrossSystems(t *testing.T) {
	cfg := e2eFleetBenchmarkConfig()
	preflight, err := bench.SimulateFleet(context.Background(), cfg, bench.FleetProfileConservative, mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatalf("SimulateFleet: %v", err)
	}
	for _, want := range []string{"cold placement", "warm reuse", "hard preemption", "disk-headroom", "submitted-from-one-peer"} {
		if !e2eHasProof(preflight.Proofs, want) {
			t.Fatalf("proofs missing %q: %+v", want, preflight.Proofs)
		}
	}
	seenNodes := map[string]bool{}
	for _, plan := range preflight.Plans {
		if plan.Decision.NodeID != "" {
			seenNodes[plan.Decision.NodeID] = true
		}
		if plan.Decision.NodeID == "spark" && plan.Decision.Claim.WeightsMB+plan.Decision.Claim.KVReservedMB > 103700 {
			t.Fatalf("Spark decision exceeds capped usable memory: %+v", plan.Decision)
		}
	}
	if !seenNodes["b70"] || !seenNodes["spark"] {
		t.Fatalf("expected B70 and Spark placement, saw %+v", seenNodes)
	}
}

func TestFleetBenchmarkProfilesAreDeterministicPreflightInputs(t *testing.T) {
	cfg := e2eFleetBenchmarkConfig()
	cfg.Waves = nil
	clock := mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))
	for _, profile := range []string{bench.FleetProfileSaturation, bench.FleetProfileSoak} {
		preflight, err := bench.SimulateFleet(context.Background(), cfg, profile, clock)
		if err != nil {
			t.Fatalf("%s SimulateFleet: %v", profile, err)
		}
		if len(preflight.Plans) == 0 || len(preflight.Proofs) == 0 {
			t.Fatalf("%s preflight = %+v", profile, preflight)
		}
	}
}

func e2eHasProof(proofs []string, want string) bool {
	for _, proof := range proofs {
		if strings.Contains(proof, want) {
			return true
		}
	}
	return false
}

func e2eFleetBenchmarkConfig() bench.FleetBenchmarkConfig {
	return bench.FleetBenchmarkConfig{
		ID:       "fleet-e2e",
		Project:  "project-a",
		RPCToken: "rpc-secret",
		Gateways: []bench.FleetGateway{
			{ID: "macbook-gw", URL: "http://macbook.test", NodeID: "macbook"},
			{ID: "macmini-gw", URL: "http://macmini.test", NodeID: "mac-mini"},
		},
		Peers:   []bench.FleetPeer{{ID: "spark", URL: "http://spark.test", RPCToken: "rpc-secret"}},
		Prompts: []bench.FleetPrompt{{ID: "default", Text: "answer briefly"}},
		Models: []bench.FleetModel{
			{ID: "qwen9b", RequestModel: "qwen9b", PresetID: "preset-9b", PromptID: "default", Priority: domain.PriorityInteractive, SpeedPref: domain.SpeedThroughput, Preemption: domain.PreemptSoft, MaxTokens: 8},
			{ID: "qwen122b", RequestModel: "qwen122b", PresetID: "preset-122b", PromptID: "default", Priority: domain.PriorityInteractive, SpeedPref: domain.SpeedThroughput, Preemption: domain.PreemptHardForInteractive, MaxTokens: 8},
		},
		Waves: []bench.FleetWave{
			{ID: "cold-9b", Jobs: []bench.FleetWaveJob{{ModelID: "qwen9b", GatewayID: "macbook-gw"}}},
			{ID: "warm-9b", Jobs: []bench.FleetWaveJob{{ModelID: "qwen9b", GatewayID: "macmini-gw"}}},
			{ID: "fit-forced-122b", Jobs: []bench.FleetWaveJob{{ModelID: "qwen122b", GatewayID: "macbook-gw"}}},
		},
		Simulation: bench.FleetSimulationConfig{
			Nodes: []domain.Node{
				{
					ID:               "spark",
					Name:             "dgx-spark",
					MaxUtil:          0.90,
					DiskTotalMB:      1_000_000,
					DiskFreeMB:       900_000,
					DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
					OOMSeverity:      domain.OOMCatastrophic,
					Status:           domain.NodeReady,
					Labels:           map[string]string{"gpu.kind": "gb10"},
					Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 122000}},
					SpeedClass:       domain.SpeedClass{TokensPerSecRef: 145},
				},
				{
					ID:               "b70",
					Name:             "arc-b70",
					MaxUtil:          0.85,
					DiskTotalMB:      1_000_000,
					DiskFreeMB:       700_000,
					DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
					OOMSeverity:      domain.OOMSoft,
					Status:           domain.NodeReady,
					Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 32768}},
					SpeedClass:       domain.SpeedClass{TokensPerSecRef: 70},
				},
				{
					ID:               "disk-full",
					Name:             "disk-full",
					MaxUtil:          0.90,
					DiskTotalMB:      1000,
					DiskFreeMB:       250,
					DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
					OOMSeverity:      domain.OOMSoft,
					Status:           domain.NodeReady,
					Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 200000}},
					SpeedClass:       domain.SpeedClass{TokensPerSecRef: 999},
				},
			},
			Presets: []domain.Preset{
				{ID: "preset-9b", ModelRef: "qwen9b", Backend: domain.BackendLlamaCpp, ContextLength: 8000, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 7000, ArtifactSizeMB: 7000, KVPerTokenMB: 0.05},
				{ID: "preset-27b", ModelRef: "qwen27b", Backend: domain.BackendLlamaCpp, ContextLength: 8000, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 30000, ArtifactSizeMB: 30000, KVPerTokenMB: 0.25},
				{ID: "preset-122b", ModelRef: "qwen122b", Backend: domain.BackendLlamaCpp, ContextLength: 8000, Capabilities: []domain.Capability{domain.CapabilityChat}, EstWeightsMB: 76000, ArtifactSizeMB: 76000, KVPerTokenMB: 0},
			},
			Instances: []domain.ModelInstance{{
				ID:             "inst-27b-background",
				PresetID:       "preset-27b",
				NodeID:         "spark",
				AcceleratorSet: []int{0},
				Claim:          domain.Claim{WeightsMB: 30000, KVReservedMB: 2000},
				State:          domain.InstReady,
				Priority:       domain.PriorityBackground,
			}},
		},
		Safety: bench.FleetBenchmarkSafety{MinDiskFreeRatio: domain.DefaultDiskMinFreeRatio, MaxSparkGPUMemoryUtil: 0.85},
	}
}
