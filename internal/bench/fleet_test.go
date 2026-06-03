package bench

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
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

	cfg = fleetTestConfig()
	cfg.Waves[0].Jobs[0].DelayMS = -1
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "delay_ms") {
		t.Fatalf("delay err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Waves[0].Jobs[0].ExpectedStatus = 700
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "expected_status") {
		t.Fatalf("expected status err = %v", err)
	}

	cfg = fleetTestConfig()
	cfg.Peers = append(cfg.Peers, cfg.Peers[0])
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, false); err == nil || !strings.Contains(err.Error(), "duplicate peer") {
		t.Fatalf("duplicate peer err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Models[0].PromptID = "missing"
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "unknown prompt") {
		t.Fatalf("model prompt err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Waves[0].Jobs[0].GatewayID = "missing"
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "unknown gateway") {
		t.Fatalf("wave gateway err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Waves[0].Jobs[0].ModelID = "missing"
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "unknown model") {
		t.Fatalf("wave model err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Simulation.Nodes = append(cfg.Simulation.Nodes, cfg.Simulation.Nodes[0])
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "duplicate simulation node") {
		t.Fatalf("duplicate node err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Simulation.Nodes[0].MaxUtil = 2
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "max_util") {
		t.Fatalf("max util err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Simulation.Nodes[0].DiskFreeMB = cfg.Simulation.Nodes[0].DiskTotalMB + 1
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "disk_free_mb") {
		t.Fatalf("disk free err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Simulation.Nodes[0].Accelerators[0].VRAMTotalMB = 0
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "vram_total_mb") {
		t.Fatalf("vram err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Simulation.Presets = append(cfg.Simulation.Presets, cfg.Simulation.Presets[0])
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "duplicate simulation preset") {
		t.Fatalf("duplicate preset err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Models[0].Priority = "urgent"
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "priority") {
		t.Fatalf("priority enum err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Models[0].SpeedPref = "cheap"
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "speed_pref") {
		t.Fatalf("speed enum err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Models[0].Preemption = "always"
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "preemption") {
		t.Fatalf("preemption enum err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Simulation.Nodes[0].Status = "busy"
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "status") {
		t.Fatalf("status enum err = %v", err)
	}
	cfg = fleetTestConfig()
	cfg.Simulation.Nodes[0].OOMSeverity = "meltdown"
	if err := ValidateFleetConfig(cfg, FleetProfileConservative, true); err == nil || !strings.Contains(err.Error(), "oom_severity") {
		t.Fatalf("oom enum err = %v", err)
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
	for _, name := range []string{"config.json", "manifest.json", "results.json", "events.jsonl", "snapshots.jsonl", "resources.jsonl", "metrics.json", "failures.json", "report.html"} {
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
	liveCalls := 0
	client := directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshot":
			node := cfg.Simulation.Nodes[0]
			instances := []domain.ModelInstance{{ID: "inst-spark", NodeID: node.ID, PresetID: "preset-122b", AcceleratorSet: []int{0}, Claim: domain.Claim{WeightsMB: 76000}, State: domain.InstReady}}
			if strings.Contains(r.Host, "b70") {
				node = cfg.Simulation.Nodes[1]
				instances = []domain.ModelInstance{{ID: "inst-live", NodeID: node.ID, PresetID: "preset-9b", State: domain.InstReady}}
			}
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: node, Instances: instances})
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
			liveCalls++
			switch liveCalls {
			case 1:
				w.Header().Set(fleetHeaderDecision, string(domain.ActionLoadedNew))
				w.Header().Set(fleetHeaderNode, "b70")
				w.Header().Set(fleetHeaderInstance, "inst-live")
				w.Header().Set(fleetHeaderBackend, string(domain.BackendLlamaCpp))
			case 2:
				w.Header().Set(fleetHeaderDecision, string(domain.ActionWarmInstance))
				w.Header().Set(fleetHeaderNode, "b70")
				w.Header().Set(fleetHeaderInstance, "inst-live")
				w.Header().Set(fleetHeaderBackend, string(domain.BackendLlamaCpp))
			default:
				w.Header().Set(fleetHeaderDecision, string(domain.ActionHardPreempted))
				w.Header().Set(fleetHeaderNode, "spark")
				w.Header().Set(fleetHeaderInstance, "inst-spark")
				w.Header().Set(fleetHeaderBackend, string(domain.BackendLlamaCpp))
			}
			w.Header().Set(fleetHeaderAttempts, "1")
			w.Header().Set(fleetHeaderTrace, `[{"step":"score","result":"winner"}]`)
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
	if len(result.Results) != 3 || result.Results[0].NodeID != "b70" || result.Results[1].Decision != string(domain.ActionWarmInstance) || result.Results[2].NodeID != "spark" || !result.Results[2].LiveMatchesPreflight || result.Results[0].TokensPerSec <= 0 || len(result.Results[0].Trace) != 1 {
		t.Fatalf("results = %+v", result.Results)
	}
	if len(result.Metrics) != 4 || result.Metrics[0].TokensPerSec != 11 {
		t.Fatalf("metrics = %+v", result.Metrics)
	}
	if len(result.Resources) != 8 || result.Resources[0].NodeID == "" {
		t.Fatalf("resources = %+v", result.Resources)
	}
	if _, err := os.Stat(result.Results[0].OutputPath); err != nil {
		t.Fatalf("output missing: %v", err)
	}
}

func TestFleetBenchmarkCleanStartUnloadsBenchmarkInstances(t *testing.T) {
	cfg := fleetTestConfig()
	cfg.Safety.ResetBenchmarkInstances = true
	clock := mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))
	instances := map[string][]domain.ModelInstance{
		"b70": {
			{ID: "inst-benchmark", NodeID: "b70", PresetID: "preset-9b", State: domain.InstReady},
			{ID: "inst-user", NodeID: "b70", PresetID: "user-preset", State: domain.InstReady},
		},
	}
	liveCalls := 0
	client := directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshot":
			node := cfg.Simulation.Nodes[0]
			if strings.Contains(r.Host, "b70") {
				node = cfg.Simulation.Nodes[1]
			}
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: node, Instances: append([]domain.ModelInstance(nil), instances[node.ID]...)})
		case "/unload":
			if r.Header.Get("Authorization") != "Bearer rpc-secret" {
				http.Error(w, "rpc token required", http.StatusUnauthorized)
				return
			}
			var req struct {
				InstanceID string `json:"instance_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode unload: %v", err)
			}
			for nodeID, nodeInstances := range instances {
				var kept []domain.ModelInstance
				for _, inst := range nodeInstances {
					if inst.ID != req.InstanceID {
						kept = append(kept, inst)
					}
				}
				instances[nodeID] = kept
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/telemetry/metrics":
			_ = json.NewEncoder(w).Encode([]domain.RunMetric{})
		case "/v1/chat/completions":
			liveCalls++
			nodeID := "b70"
			instanceID := "inst-new"
			decision := domain.ActionLoadedNew
			if liveCalls == 2 {
				decision = domain.ActionWarmInstance
			}
			if liveCalls >= 3 {
				nodeID = "spark"
				instanceID = "inst-spark"
				decision = domain.ActionHardPreempted
				instances[nodeID] = append(instances[nodeID], domain.ModelInstance{ID: instanceID, NodeID: nodeID, PresetID: "preset-122b", State: domain.InstReady})
			} else if liveCalls == 1 {
				instances[nodeID] = append(instances[nodeID], domain.ModelInstance{ID: instanceID, NodeID: nodeID, PresetID: "preset-9b", State: domain.InstReady})
			}
			w.Header().Set(fleetHeaderDecision, string(decision))
			w.Header().Set(fleetHeaderNode, nodeID)
			w.Header().Set(fleetHeaderInstance, instanceID)
			w.Header().Set(fleetHeaderBackend, string(domain.BackendLlamaCpp))
			w.Header().Set(fleetHeaderAttempts, "1")
			_ = json.NewEncoder(w).Encode(api.OpenAIChatResponse{
				Choices: []api.OpenAIChatChoice{{Message: api.OpenAIMessage{Role: "assistant", Content: "cold ok"}}},
				Usage:   api.OpenAIUsage{CompletionTokens: 2, TotalTokens: 4},
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
		t.Fatalf("RunFleet clean start: %v failures=%+v", err, result.Failures)
	}
	for _, inst := range instances["b70"] {
		if inst.ID == "inst-benchmark" {
			t.Fatalf("benchmark instance was not unloaded: %+v", instances["b70"])
		}
		if inst.ID == "inst-user" && inst.PresetID != "user-preset" {
			t.Fatalf("user instance changed: %+v", inst)
		}
	}
	resetEventFound := false
	for _, event := range result.Events {
		if event.Type == "reset_unload" && event.Instance == "inst-benchmark" {
			resetEventFound = true
		}
	}
	if !resetEventFound {
		t.Fatalf("events = %+v", result.Events)
	}
	if len(result.Snapshots) < 4 || result.Snapshots[2].Stage != "after_reset" {
		t.Fatalf("snapshots = %+v", result.Snapshots)
	}
}

func TestFleetBenchmarkCleanStartRejectsUnsafeInstances(t *testing.T) {
	cfg := fleetTestConfig()
	cfg.Safety.ResetBenchmarkInstances = true
	client := directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshot":
			node := cfg.Simulation.Nodes[0]
			var instances []domain.ModelInstance
			if strings.Contains(r.Host, "b70") {
				node = cfg.Simulation.Nodes[1]
				instances = []domain.ModelInstance{{
					ID:       "inst-pinned",
					NodeID:   node.ID,
					PresetID: "preset-9b",
					State:    domain.InstReady,
					Pinned:   true,
				}}
			}
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: node, Instances: instances})
		case "/telemetry/metrics":
			_ = json.NewEncoder(w).Encode([]domain.RunMetric{})
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
	if err == nil || !strings.Contains(err.Error(), "evidence collection") {
		t.Fatalf("RunFleet unsafe reset err = %v", err)
	}
	if len(result.Failures) != 1 || !strings.Contains(result.Failures[0].Error, "pinned=true") {
		t.Fatalf("failures = %+v", result.Failures)
	}
}

func TestFleetBenchmarkLiveRunnerHonorsExpectedFailuresAndTraceExpectations(t *testing.T) {
	cfg := fleetTestConfig()
	cfg.Waves = []FleetWave{
		{ID: "expected-cap", Jobs: []FleetWaveJob{{
			ID:                    "cap-reject",
			ModelID:               "qwen9b",
			ExpectedFailure:       true,
			ExpectedStatus:        http.StatusBadGateway,
			ExpectedErrorContains: "no instance",
		}}},
		{ID: "dynamic-preempt", Jobs: []FleetWaveJob{{
			ID:                    "preempt-122b",
			ModelID:               "qwen122b",
			ExpectedStatus:        http.StatusOK,
			ExpectedNodeID:        "spark",
			ExpectedDecision:      string(domain.ActionHardPreempted),
			ExpectedBackend:       string(domain.BackendVLLM),
			ExpectedAttempts:      1,
			ExpectedTraceContains: []string{"preempt", "replace"},
		}}},
	}
	clock := mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))
	client := directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshot":
			node := cfg.Simulation.Nodes[0]
			instances := []domain.ModelInstance{{ID: "inst-preempted", NodeID: node.ID, PresetID: "preset-122b", State: domain.InstReady}}
			if strings.Contains(r.Host, "b70") {
				node = cfg.Simulation.Nodes[1]
				instances = nil
			}
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: node, Instances: instances})
		case "/telemetry/metrics":
			_ = json.NewEncoder(w).Encode([]domain.RunMetric{})
		case "/v1/chat/completions":
			var req api.OpenAIChatRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			if req.Model == "qwen9b" {
				http.Error(w, `{"error":"job queued: no instance available"}`, http.StatusBadGateway)
				return
			}
			w.Header().Set(fleetHeaderDecision, string(domain.ActionHardPreempted))
			w.Header().Set(fleetHeaderNode, "spark")
			w.Header().Set(fleetHeaderInstance, "inst-preempted")
			w.Header().Set(fleetHeaderBackend, string(domain.BackendVLLM))
			w.Header().Set(fleetHeaderAttempts, "1")
			w.Header().Set(fleetHeaderTrace, `[{"step":"preempt","result":"victims=[inst-a] target=spark"},{"step":"replace","result":"replaced=[inst-a]"}]`)
			_ = json.NewEncoder(w).Encode(api.OpenAIChatResponse{
				Model: req.Model,
				Choices: []api.OpenAIChatChoice{{
					Message: api.OpenAIMessage{Role: "assistant", Content: "dynamic ok"},
				}},
				Usage: api.OpenAIUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5},
			})
		default:
			http.NotFound(w, r)
		}
	}))

	result, err := RunFleet(context.Background(), cfg, FleetRunOptions{
		Profile:    FleetProfileSaturation,
		OutputRoot: t.TempDir(),
		Client:     client,
		Clock:      clock,
	})
	if err != nil {
		t.Fatalf("RunFleet live: %v", err)
	}
	if len(result.Failures) != 0 || !result.Results[0].ExpectedFailure || len(result.Results[0].ExpectationErrors) != 0 {
		t.Fatalf("result failures=%+v results=%+v", result.Failures, result.Results)
	}
	if result.Results[1].Decision != string(domain.ActionHardPreempted) || len(result.Results[1].ExpectationErrors) != 0 {
		t.Fatalf("dynamic result = %+v", result.Results[1])
	}
}

func TestFleetBenchmarkLiveRunnerFailsExpectationMismatches(t *testing.T) {
	cfg := fleetTestConfig()
	cfg.Waves = []FleetWave{{ID: "wrong-node", Jobs: []FleetWaveJob{{
		ID:               "wrong-node",
		ModelID:          "qwen9b",
		ExpectedStatus:   http.StatusOK,
		ExpectedNodeID:   "spark",
		ExpectedDecision: string(domain.ActionLoadedNew),
	}}}}
	client := directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshot":
			node := cfg.Simulation.Nodes[0]
			instances := []domain.ModelInstance{}
			if strings.Contains(r.Host, "b70") {
				node = cfg.Simulation.Nodes[1]
				instances = []domain.ModelInstance{{ID: "inst-b70", NodeID: node.ID, PresetID: "preset-9b", State: domain.InstReady}}
			}
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: node, Instances: instances})
		case "/telemetry/metrics":
			_ = json.NewEncoder(w).Encode([]domain.RunMetric{})
		case "/v1/chat/completions":
			w.Header().Set(fleetHeaderDecision, string(domain.ActionLoadedNew))
			w.Header().Set(fleetHeaderNode, "b70")
			w.Header().Set(fleetHeaderInstance, "inst-b70")
			w.Header().Set(fleetHeaderBackend, string(domain.BackendLlamaCpp))
			_ = json.NewEncoder(w).Encode(api.OpenAIChatResponse{
				Choices: []api.OpenAIChatChoice{{
					Message: api.OpenAIMessage{Role: "assistant", Content: "wrong node"},
				}},
				Usage: api.OpenAIUsage{CompletionTokens: 2, TotalTokens: 4},
			})
		default:
			http.NotFound(w, r)
		}
	}))

	result, err := RunFleet(context.Background(), cfg, FleetRunOptions{
		Profile:    FleetProfileSaturation,
		OutputRoot: t.TempDir(),
		Client:     client,
		Clock:      mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)),
	})
	if err == nil || !strings.Contains(err.Error(), "failures") {
		t.Fatalf("RunFleet mismatch err = %v", err)
	}
	if len(result.Failures) != 1 || !strings.Contains(result.Failures[0].Error, `expected node "spark"`) {
		t.Fatalf("failures = %+v", result.Failures)
	}
}

func TestFleetExpectationValidationAndDelay(t *testing.T) {
	spec := FleetWaveJob{
		ExpectedStatus:        http.StatusOK,
		ExpectedNodeID:        "spark",
		ExpectedDecision:      string(domain.ActionHardPreempted),
		ExpectedBackend:       string(domain.BackendVLLM),
		ExpectedAttempts:      2,
		ExpectedErrorContains: "overflow",
		ExpectedTraceContains: []string{"preempt", "replace"},
		ExpectedFailure:       true,
	}
	result := FleetJobResult{
		StatusCode: http.StatusBadGateway,
		NodeID:     "b70",
		Decision:   string(domain.ActionLoadedNew),
		Backend:    string(domain.BackendLlamaCpp),
		Attempts:   1,
		Error:      "no fit",
		Headers:    map[string]string{fleetHeaderTrace: `[{"step":"score"}]`},
	}
	errs := validateFleetExpectation(spec, result)
	for _, want := range []string{"expected HTTP 200", "expected node", "expected decision", "expected backend", "expected attempts", "expected error containing", "expected trace containing"} {
		if !containsString(errs, want) {
			t.Fatalf("errs missing %q: %+v", want, errs)
		}
	}
	spec = FleetWaveJob{ExpectedFailure: true}
	if errs := validateFleetExpectation(spec, FleetJobResult{StatusCode: http.StatusOK}); !containsString(errs, "expected failure") {
		t.Fatalf("expected failure errs = %+v", errs)
	}
	spec = FleetWaveJob{ExpectedTraceContains: []string{"preempt"}}
	result = FleetJobResult{StatusCode: http.StatusOK, Trace: []domain.TraceStep{{Step: "preempt", Result: "victims=[a]"}}}
	if errs := validateFleetExpectation(spec, result); len(errs) != 0 {
		t.Fatalf("trace expectation errs = %+v", errs)
	}

	clock := mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))
	done := make(chan error, 1)
	go func() {
		done <- waitFleetDelay(context.Background(), clock, 100)
	}()
	for clock.TimerCount() == 0 {
		runtime.Gosched()
	}
	select {
	case err := <-done:
		t.Fatalf("delay finished early: %v", err)
	default:
	}
	clock.Advance(100 * time.Millisecond)
	if err := <-done; err != nil {
		t.Fatalf("delay err = %v", err)
	}
	if err := waitFleetDelay(context.Background(), clock, 0); err != nil {
		t.Fatalf("zero delay err = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitFleetDelay(ctx, clock, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled delay err = %v", err)
	}
}

func TestFleetLivePreflightMismatchEvidence(t *testing.T) {
	plan := FleetSimulationDecision{
		JobID: "job-a",
		Decision: domain.PlacementDecision{
			NodeID: "spark",
			Action: domain.ActionHardPreempted,
		},
	}
	matched := attachPreflightResult(FleetJobResult{JobID: "job-a", NodeID: "spark", Decision: string(domain.ActionHardPreempted)}, plan)
	if !matched.LiveMatchesPreflight || matched.PreflightNodeID != "spark" || matched.PreflightDecision != string(domain.ActionHardPreempted) {
		t.Fatalf("matched = %+v", matched)
	}
	mismatched := attachPreflightResult(FleetJobResult{JobID: "job-a", ModelID: "qwen122b", NodeID: "b70", Decision: string(domain.ActionLoadedNew)}, plan)
	failures := livePreflightMismatchFailures([]FleetJobResult{matched, mismatched})
	if len(failures) != 1 || !strings.Contains(failures[0].Error, "did not match preflight") {
		t.Fatalf("mismatch failures = %+v", failures)
	}
	expectedFailure := attachPreflightResult(FleetJobResult{JobID: "job-a", ModelID: "qwen122b", ExpectedFailure: true}, plan)
	if failures := livePreflightMismatchFailures([]FleetJobResult{expectedFailure}); len(failures) != 0 {
		t.Fatalf("expected-failure mismatch should be ignored: %+v", failures)
	}
}

func TestSubmitFleetJobTransportAndReadFailures(t *testing.T) {
	cfg := fleetTestConfig()
	model := cfg.Models[0]
	spec := FleetWaveJob{ID: "job-a", ModelID: model.ID, ExpectedStatus: http.StatusOK}
	clock := mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))
	result := submitFleetJob(context.Background(), cfg, "wave-a", 0, spec, model, FleetGateway{ID: "bad", URL: "://bad"}, t.TempDir(), http.DefaultClient, clock)
	if result.Error == "" || len(result.ExpectationErrors) == 0 {
		t.Fatalf("bad url result = %+v", result)
	}

	transportErr := errors.New("transport failed")
	client := &http.Client{Transport: fleetRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, transportErr
	})}
	result = submitFleetJob(context.Background(), cfg, "wave-a", 0, spec, model, cfg.Gateways[0], t.TempDir(), client, clock)
	if !strings.Contains(result.Error, transportErr.Error()) {
		t.Fatalf("transport result = %+v", result)
	}

	client = &http.Client{Transport: fleetRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       errReadCloser{err: errors.New("read failed")},
			Request:    req,
		}, nil
	})}
	result = submitFleetJob(context.Background(), cfg, "wave-a", 0, spec, model, cfg.Gateways[0], t.TempDir(), client, clock)
	if !strings.Contains(result.Error, "read failed") {
		t.Fatalf("read result = %+v", result)
	}

	client = directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(fleetHeaderTrace, `{`)
		_ = json.NewEncoder(w).Encode(api.OpenAIChatResponse{
			Choices: []api.OpenAIChatChoice{{Message: api.OpenAIMessage{Role: "assistant", Content: "bad trace"}}},
			Usage:   api.OpenAIUsage{CompletionTokens: 1, TotalTokens: 2},
		})
	}))
	result = submitFleetJob(context.Background(), cfg, "wave-a", 0, spec, model, cfg.Gateways[0], t.TempDir(), client, clock)
	if !strings.Contains(result.Error, "X-Myc-Trace") {
		t.Fatalf("trace result = %+v", result)
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
	conservative := defaultWaves(loaded, FleetProfileConservative)
	if len(conservative) != 3 || conservative[0].ID != "conservative-cold" || conservative[1].ID != "conservative-warm" || conservative[2].ID != "conservative-fit-forced" {
		t.Fatalf("conservative defaults = %+v", conservative)
	}
	result, err := RunFleet(context.Background(), loaded, FleetRunOptions{
		Profile:    FleetProfileConservative,
		Simulate:   true,
		OutputRoot: t.TempDir(),
		Clock:      mocks.NewFakeClock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)),
	})
	if err != nil || !result.Preflight.Passed || len(result.Preflight.Plans) != 3 {
		t.Fatalf("conservative default RunFleet result=%+v err=%v", result.Preflight, err)
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

func TestFleetBenchmarkLiveRunnerFailsRequiredEvidenceCollection(t *testing.T) {
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
	if len(result.Failures) != 4 || result.Failures[0].RetryAllowed {
		t.Fatalf("failures = %+v", result.Failures)
	}
	if len(result.Snapshots) == 0 || result.Snapshots[0].Error == "" {
		t.Fatalf("snapshots = %+v", result.Snapshots)
	}
}

func TestFleetBenchmarkLiveRunnerRecordsHTTPFailures(t *testing.T) {
	cfg := fleetTestConfig()
	client := directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/snapshot":
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: cfg.Simulation.Nodes[0]})
		case "/telemetry/metrics":
			_ = json.NewEncoder(w).Encode([]domain.RunMetric{})
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
}

func TestFleetBenchmarkEvidenceHelpersFailLoudly(t *testing.T) {
	cfg := fleetTestConfig()
	required := true
	cfg.Safety.RequireTelemetry = &required
	snapshots := []FleetSnapshotMark{
		{Stage: "before", PeerID: "spark", Error: "offline"},
		{Stage: "before", PeerID: "b70", Snapshot: domain.NodeSnapshot{Node: domain.Node{ID: "not-in-sim"}}},
	}
	if failures := snapshotFailures(cfg, snapshots); len(failures) != 1 || !strings.Contains(failures[0].Error, "offline") {
		t.Fatalf("snapshot failures = %+v", failures)
	}
	if failures := liveSnapshotMismatchFailures(cfg, snapshots); len(failures) != 1 || failures[0].NodeID != "not-in-sim" {
		t.Fatalf("mismatch failures = %+v", failures)
	}
	if resources := resourcesFromSnapshots(cfg, snapshots); len(resources) != 2 || resources[0].Error == "" || resources[1].NodeID != "not-in-sim" {
		t.Fatalf("resources = %+v", resources)
	}
	resourceSnapshot := []FleetSnapshotMark{{
		Stage:  "after",
		PeerID: "spark",
		Snapshot: domain.NodeSnapshot{
			Node: cfg.Simulation.Nodes[0],
			Instances: []domain.ModelInstance{{
				ID:             "inst-122b",
				PresetID:       "preset-122b",
				NodeID:         "spark",
				AcceleratorSet: []int{0},
				Claim:          domain.Claim{WeightsMB: 76000, KVReservedMB: 1024},
				State:          domain.InstReady,
				Priority:       domain.PriorityInteractive,
			}},
		},
	}}
	resource := resourcesFromSnapshots(cfg, resourceSnapshot)[0]
	if resource.DiskFloorMB == 0 || resource.LargestArtifactMB == 0 || resource.DiskFreeAfterLargestArtifactMB != resource.DiskFreeMB-resource.LargestArtifactMB {
		t.Fatalf("resource disk fields = %+v", resource)
	}
	if len(resource.Instances) != 1 || len(resource.AcceleratorUsage) != 1 || resource.AcceleratorUsage[0].ReservedClaimMB != 77024 || resource.AcceleratorUsage[0].BenchmarkUsedMB != 77024 {
		t.Fatalf("resource claim fields = %+v", resource)
	}
	placementFailures := livePlacementEvidenceFailures([]FleetJobResult{
		{JobID: "missing-node", ModelID: "qwen9b", StatusCode: http.StatusOK},
		{JobID: "unknown-node", ModelID: "qwen9b", StatusCode: http.StatusOK, NodeID: "not-seen", InstanceID: "inst-a", Backend: string(domain.BackendLlamaCpp), Decision: string(domain.ActionLoadedNew)},
		{JobID: "missing-headers", ModelID: "qwen9b", StatusCode: http.StatusOK, NodeID: "not-in-sim"},
		{JobID: "unseen-instance", ModelID: "qwen9b", StatusCode: http.StatusOK, NodeID: "not-in-sim", InstanceID: "missing", Backend: string(domain.BackendLlamaCpp), Decision: string(domain.ActionLoadedNew)},
		{JobID: "failed", ModelID: "qwen9b", StatusCode: http.StatusBadGateway, Error: "failed"},
	}, snapshots)
	if len(placementFailures) != 6 || !containsString([]string{placementFailures[5].Error}, "instance") {
		t.Fatalf("placement failures = %+v", placementFailures)
	}
	unsafeResources := []FleetResourceMark{
		{PeerID: "empty"},
		{NodeID: "bad-disk", DiskTotalMB: 1000, DiskFreeMB: 200, DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio, MaxUtil: 0.90, OOMSeverity: domain.OOMSoft, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 1000}}},
		{NodeID: "bad-ratio", DiskTotalMB: 1000, DiskFreeMB: 800, DiskMinFreeRatio: 1.2, MaxUtil: 0.90, OOMSeverity: domain.OOMSoft, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 1000}}},
		{NodeID: "bad-util", DiskTotalMB: 1000, DiskFreeMB: 800, DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio, MaxUtil: 0, OOMSeverity: domain.OOMSoft},
		{NodeID: "bad-vram", DiskTotalMB: 1000, DiskFreeMB: 800, DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio, MaxUtil: 0.90, OOMSeverity: domain.OOMSoft, Accelerators: []domain.Accelerator{{Index: 0}}},
		{NodeID: "bad-oom", DiskTotalMB: 1000, DiskFreeMB: 800, DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio, MaxUtil: 0.90, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 1000}}},
		{NodeID: "bad-artifact", DiskTotalMB: 1000, DiskFreeMB: 300, DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio, DiskFreeAfterLargestArtifactMB: 240, LargestArtifactMB: 60, MaxUtil: 0.90, OOMSeverity: domain.OOMSoft, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 1000}}},
		{NodeID: "spark-hot", DiskTotalMB: 1000, DiskFreeMB: 800, DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio, MaxUtil: 0.90, OOMSeverity: domain.OOMCatastrophic, AcceleratorUsage: []FleetAcceleratorUsage{{Index: 0, VRAMTotalMB: 1000, SnapshotUsedMB: 0, ReservedClaimMB: 851, BenchmarkUsedMB: 851, UsableMB: 850}}},
	}
	if failures := resourceSafetyFailures(cfg, unsafeResources); len(failures) != 8 {
		t.Fatalf("resource failures = %+v", failures)
	}

	client := directFleetHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/telemetry/metrics":
			_, _ = w.Write([]byte(`{`))
		default:
			http.NotFound(w, r)
		}
	}))
	metrics, failures := collectMetrics(context.Background(), cfg, client)
	if len(metrics) != 0 || len(failures) != 2 || !strings.Contains(failures[0].Error, "decode") {
		t.Fatalf("metrics=%+v failures=%+v", metrics, failures)
	}

	required = false
	cfg.Safety.RequireTelemetry = &required
	if failures := snapshotFailures(cfg, snapshots); len(failures) != 0 {
		t.Fatalf("optional snapshot failures = %+v", failures)
	}
	_, failures = collectMetrics(context.Background(), cfg, client)
	if len(failures) != 0 || telemetryRequired(cfg) {
		t.Fatalf("optional telemetry failures=%+v required=%v", failures, telemetryRequired(cfg))
	}
	if _, err := gatewayForJob(cfg.Gateways, gatewayByID(cfg.Gateways), "missing", 0); err == nil || !strings.Contains(err.Error(), "unknown gateway") {
		t.Fatalf("unknown gateway err = %v", err)
	}
}

func containsProof(proofs []string, want string) bool {
	return containsString(proofs, want)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
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

type errReadCloser struct {
	err error
}

func (r errReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (r errReadCloser) Close() error {
	return nil
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
