package bench

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/pkg/api"
	"mycelium/test/mocks"
)

func TestFleetBenchmarkConfigValidationRejectsBadInputs(t *testing.T) {
	cfg := fleetTestConfig()
	cfg.Models = append(cfg.Models, cfg.Models[0])
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "duplicate model") {
		t.Fatalf("duplicate model err = %v", err)
	}

	cfg = fleetTestConfig()
	if err := ValidateFleetConfig(cfg, "reckless", true); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("profile err = %v", err)
	}

	cfg = fleetTestConfig()
	cfg.Simulation.Nodes[0].DiskMinFreeRatio = 1
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "disk_min_free_ratio") {
		t.Fatalf("disk err = %v", err)
	}

	cfg = fleetTestConfig()
	cfg.Simulation.Presets = append(cfg.Simulation.Presets, domain.Preset{
		ID:            "unsafe-vllm",
		ModelRef:      "unsafe",
		Backend:       domain.BackendVLLM,
		ContextLength: 4096,
		Capabilities:  []domain.Capability{domain.CapabilityChat},
		EstWeightsMB:  1,
		LaunchArgs:    []string{"serve", "{model}", "--gpu-memory-utilization", "0.90"},
	})
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "Spark safety cap") {
		t.Fatalf("spark cap err = %v", err)
	}

	cfg = fleetTestConfig()
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, false); err != nil {
		t.Fatalf("real config with peers should validate: %v", err)
	}
	cfg.Peers = nil
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, false); err == nil || !strings.Contains(err.Error(), "telemetry") {
		t.Fatalf("telemetry err = %v", err)
	}
}

func TestFleetBenchmarkSimulationProvesConservativeScenarioAndWritesArtifacts(t *testing.T) {
	cfg := fleetTestConfig()
	clock := mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))

	result, err := RunFleet(context.Background(), cfg, FleetRunOptions{
		Profile:    FleetProfileConservative,
		Simulate:   true,
		OutputRoot: t.TempDir(),
		Clock:      clock,
	})
	if err != nil {
		t.Fatalf("RunFleet simulate: %v", err)
	}
	if !result.Preflight.Passed || len(result.Preflight.Plans) != 3 {
		t.Fatalf("preflight = %+v", result.Preflight)
	}
	for _, want := range []string{"cold placement", "warm reuse", "hard preemption", "disk-headroom", "submitted-from-one-peer"} {
		if !containsProof(result.Preflight.Proofs, want) {
			t.Fatalf("proofs missing %q: %+v", want, result.Preflight.Proofs)
		}
	}
	for _, name := range []string{"manifest.json", "results.json", "events.jsonl", "snapshots.jsonl", "failures.json", "report.html"} {
		if _, err := os.Stat(filepath.Join(result.OutputDir, name)); err != nil {
			t.Fatalf("artifact %s missing: %v", name, err)
		}
	}
	report, err := os.ReadFile(filepath.Join(result.OutputDir, "report.html"))
	if err != nil || !strings.Contains(string(report), "Mycelium Fleet Benchmark") {
		t.Fatalf("report = %q %v", report, err)
	}
}

func TestFleetBenchmarkLiveRunnerCapturesHeadersMetricsAndOutputs(t *testing.T) {
	cfg := fleetTestConfig()
	clock := mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))
	client := directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshot":
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: cfg.Simulation.Nodes[0]})
		case "/telemetry/metrics":
			if r.Header.Get("Authorization") != "Bearer rpc-secret" {
				http.Error(w, "rpc token required", http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode([]domain.RunMetric{{
				JobID:        "metric-a",
				NodeID:       "b70",
				Project:      cfg.Project,
				TokensPerSec: 11,
				At:           clock.Now(),
			}})
		case "/v1/chat/completions":
			clock.Advance(100 * time.Millisecond)
			var req api.OpenAIChatRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			w.Header().Set(fleetHeaderDecision, string(domain.ActionLoadedNew))
			w.Header().Set(fleetHeaderNode, "b70")
			w.Header().Set(fleetHeaderInstance, "inst-live")
			w.Header().Set(fleetHeaderBackend, string(domain.BackendLlamaCpp))
			w.Header().Set(fleetHeaderAttempts, "1")
			w.Header().Set(fleetHeaderTrace, `[{"step":"score","result":"winner=b70"}]`)
			_ = json.NewEncoder(w).Encode(api.OpenAIChatResponse{
				Model: req.Model,
				Choices: []api.OpenAIChatChoice{{
					Message: api.OpenAIMessage{Role: "assistant", Content: "answer for " + req.Model},
				}},
				Usage: api.OpenAIUsage{PromptTokens: 3, CompletionTokens: 7, TotalTokens: 10},
			})
		default:
			http.NotFound(w, r)
		}
	}))

	result, err := RunFleet(context.Background(), cfg, FleetRunOptions{
		Profile:    FleetProfileConservative,
		OutputRoot: t.TempDir(),
		Client:     client,
		Clock:      clock,
	})
	if err != nil {
		t.Fatalf("RunFleet live: %v", err)
	}
	if len(result.Results) != 3 || result.Results[0].NodeID != "b70" || result.Results[0].TokensPerSec <= 0 || len(result.Results[0].Trace) != 1 {
		t.Fatalf("results = %+v", result.Results)
	}
	if len(result.Metrics) != 4 || result.Metrics[0].TokensPerSec != 11 {
		t.Fatalf("metrics = %+v", result.Metrics)
	}
	if _, err := os.Stat(result.Results[0].OutputPath); err != nil {
		t.Fatalf("output missing: %v", err)
	}
}

func TestLoadFleetConfigAndDefaultProfiles(t *testing.T) {
	cfg := fleetTestConfig()
	cfg.Waves = nil
	path := filepath.Join(t.TempDir(), "fleet.json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	loaded, err := LoadFleetConfig(path)
	if err != nil {
		t.Fatalf("LoadFleetConfig: %v", err)
	}
	if loaded.ID != cfg.ID || len(defaultWaves(loaded, FleetProfileSaturation)) == 0 || len(defaultWaves(loaded, FleetProfileSoak)) != 5 {
		t.Fatalf("loaded/defaults = %+v", loaded)
	}
	if _, err := LoadFleetConfig(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("missing config accepted")
	}
	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte(`{`), 0644); err != nil {
		t.Fatalf("write bad: %v", err)
	}
	if _, err := LoadFleetConfig(badPath); err == nil {
		t.Fatal("bad config accepted")
	}
}

func TestFleetBenchmarkFailurePathsWriteArtifacts(t *testing.T) {
	cfg := fleetTestConfig()
	cfg.Waves = []FleetWave{{ID: "weak", Jobs: []FleetWaveJob{{ModelID: "qwen9b"}}}}
	outDir := t.TempDir()
	result, err := RunFleet(context.Background(), cfg, FleetRunOptions{
		Profile:    FleetProfileConservative,
		Simulate:   true,
		OutputRoot: outDir,
		Clock:      mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)),
	})
	if err == nil || !strings.Contains(err.Error(), "did not prove") {
		t.Fatalf("weak preflight err = %v", err)
	}
	if len(result.Failures) != 1 {
		t.Fatalf("failures = %+v", result.Failures)
	}
	if _, statErr := os.Stat(filepath.Join(result.OutputDir, "failures.json")); statErr != nil {
		t.Fatalf("failure artifact missing: %v", statErr)
	}
	if _, err := RunFleet(context.Background(), fleetTestConfig(), FleetRunOptions{OutputRoot: ""}); err == nil {
		t.Fatal("missing output root accepted")
	}
}

func TestFleetBenchmarkLiveRunnerRecordsHTTPFailures(t *testing.T) {
	cfg := fleetTestConfig()
	client := directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshot":
			http.Error(w, "snapshot unavailable", http.StatusServiceUnavailable)
		case "/telemetry/metrics":
			http.Error(w, "no metrics", http.StatusUnauthorized)
		case "/v1/chat/completions":
			http.Error(w, "backend saturated", http.StatusTooManyRequests)
		default:
			http.NotFound(w, r)
		}
	}))
	result, err := RunFleet(context.Background(), cfg, FleetRunOptions{
		Profile:    FleetProfileConservative,
		OutputRoot: t.TempDir(),
		Client:     client,
		Clock:      mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)),
	})
	if err == nil || !strings.Contains(err.Error(), "failures") {
		t.Fatalf("live failure err = %v", err)
	}
	if len(result.Failures) != 3 || !result.Failures[0].RetryAllowed {
		t.Fatalf("failures = %+v", result.Failures)
	}
	if len(result.Snapshots) == 0 || result.Snapshots[0].Error == "" {
		t.Fatalf("snapshots = %+v", result.Snapshots)
	}
}

func containsProof(proofs []string, want string) bool {
	for _, proof := range proofs {
		if strings.Contains(proof, want) {
			return true
		}
	}
	return false
}

func directFleetHTTPClient(handler http.Handler) *http.Client {
	return &http.Client{Transport: fleetRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Result(), nil
	})}
}

type fleetRoundTripFunc func(*http.Request) (*http.Response, error)

func (f fleetRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func fleetTestConfig() FleetBenchmarkConfig {
	spark := domain.Node{
		ID:               "spark",
		Name:             "dgx-spark",
		MaxUtil:          0.90,
		DiskTotalMB:      1_000_000,
		DiskFreeMB:       900_000,
		DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
		OOMSeverity:      domain.OOMCatastrophic,
		Status:           domain.NodeReady,
		Labels:           map[string]string{"gpu.kind": "gb10", "gpu.vendor": "nvidia"},
		Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 122000}},
		SpeedClass:       domain.SpeedClass{TokensPerSecRef: 145},
	}
	b70 := domain.Node{
		ID:               "b70",
		Name:             "arc-b70",
		MaxUtil:          0.85,
		DiskTotalMB:      1_000_000,
		DiskFreeMB:       700_000,
		DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
		OOMSeverity:      domain.OOMSoft,
		Status:           domain.NodeReady,
		Labels:           map[string]string{"gpu.kind": "arc-pro-b70", "gpu.vendor": "intel"},
		Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 32768}},
		SpeedClass:       domain.SpeedClass{TokensPerSecRef: 70},
	}
	macMini := domain.Node{
		ID:               "mac-mini",
		Name:             "mac-mini",
		MaxUtil:          0.80,
		DiskTotalMB:      1_000_000,
		DiskFreeMB:       600_000,
		DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
		OOMSeverity:      domain.OOMSoft,
		Status:           domain.NodeReady,
		Labels:           map[string]string{"gpu.vendor": "apple", "memory.class": "unified"},
		Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 64000, UnifiedMemory: true}},
		SpeedClass:       domain.SpeedClass{TokensPerSecRef: 45},
	}
	diskFull := domain.Node{
		ID:               "disk-full",
		Name:             "disk-full",
		MaxUtil:          0.90,
		DiskTotalMB:      1000,
		DiskFreeMB:       250,
		DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
		OOMSeverity:      domain.OOMSoft,
		Status:           domain.NodeReady,
		Labels:           map[string]string{"gpu.vendor": "nvidia"},
		Accelerators:     []domain.Accelerator{{Index: 0, VRAMTotalMB: 200000}},
		SpeedClass:       domain.SpeedClass{TokensPerSecRef: 999},
	}
	return FleetBenchmarkConfig{
		ID:       "fleet-test",
		Project:  "project-a",
		RPCToken: "rpc-secret",
		Gateways: []FleetGateway{
			{ID: "macbook-gw", URL: "http://macbook.test", NodeID: "macbook"},
			{ID: "macmini-gw", URL: "http://macmini.test", NodeID: "mac-mini"},
		},
		Peers: []FleetPeer{
			{ID: "spark", URL: "http://spark.test", RPCToken: "rpc-secret"},
			{ID: "b70", URL: "http://b70.test", RPCToken: "rpc-secret"},
		},
		Prompts: []FleetPrompt{{ID: "default", Text: "answer briefly"}},
		Models: []FleetModel{
			{ID: "qwen9b", RequestModel: "qwen9b", PresetID: "preset-9b", PromptID: "default", Priority: domain.PriorityInteractive, SpeedPref: domain.SpeedThroughput, Preemption: domain.PreemptSoft, MaxTokens: 8},
			{ID: "qwen122b", RequestModel: "qwen122b", PresetID: "preset-122b", PromptID: "default", Priority: domain.PriorityInteractive, SpeedPref: domain.SpeedThroughput, Preemption: domain.PreemptHardForInteractive, MaxTokens: 8},
		},
		Waves: []FleetWave{
			{ID: "cold-9b", Jobs: []FleetWaveJob{{ModelID: "qwen9b", GatewayID: "macbook-gw"}}},
			{ID: "warm-9b", Jobs: []FleetWaveJob{{ModelID: "qwen9b", GatewayID: "macmini-gw"}}},
			{ID: "fit-forced-122b", Jobs: []FleetWaveJob{{ModelID: "qwen122b", GatewayID: "macbook-gw"}}},
		},
		Simulation: FleetSimulationConfig{
			Nodes: []domain.Node{spark, b70, macMini, diskFull},
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
		Safety: FleetBenchmarkSafety{MinDiskFreeRatio: domain.DefaultDiskMinFreeRatio, MaxSparkGPUMemoryUtil: 0.85},
	}
}
