package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
	"mycelium/pkg/api"
)

const (
	fleetHeaderDecision   = "X-Myc-Decision"
	fleetHeaderNode       = "X-Myc-Node"
	fleetHeaderInstance   = "X-Myc-Instance"
	fleetHeaderBackend    = "X-Myc-Backend"
	fleetHeaderAttempts   = "X-Myc-Attempts"
	fleetHeaderTrace      = "X-Myc-Trace"
	fleetHeaderProject    = "X-Myc-Project"
	fleetHeaderPriority   = "X-Myc-Priority"
	fleetHeaderSpeedPref  = "X-Myc-Speed-Pref"
	fleetHeaderContextCap = "X-Myc-Context-Cap"
	fleetHeaderPreemption = "X-Myc-Preemption"
)

const (
	FleetProfileConservative = "conservative"
	FleetProfileSaturation   = "saturation"
	FleetProfileSoak         = "soak"
)

type FleetBenchmarkConfig struct {
	ID         string                `json:"id,omitempty"`
	Project    string                `json:"project"`
	RPCToken   string                `json:"rpc_token,omitempty"`
	Gateways   []FleetGateway        `json:"gateways"`
	Peers      []FleetPeer           `json:"peers,omitempty"`
	Models     []FleetModel          `json:"models"`
	Prompts    []FleetPrompt         `json:"prompts"`
	Waves      []FleetWave           `json:"waves,omitempty"`
	Simulation FleetSimulationConfig `json:"simulation"`
	Safety     FleetBenchmarkSafety  `json:"safety,omitempty"`
	Metadata   map[string]string     `json:"metadata,omitempty"`
}

type FleetGateway struct {
	ID     string `json:"id"`
	URL    string `json:"url"`
	NodeID string `json:"node_id,omitempty"`
}

type FleetPeer struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	RPCToken string `json:"rpc_token,omitempty"`
}

type FleetModel struct {
	ID                  string            `json:"id"`
	RequestModel        string            `json:"request_model,omitempty"`
	PresetID            string            `json:"preset_id,omitempty"`
	PromptID            string            `json:"prompt_id,omitempty"`
	MaxTokens           int               `json:"max_tokens,omitempty"`
	ContextRequest      int               `json:"context_request,omitempty"`
	ExpectedConcurrency int               `json:"expected_concurrency,omitempty"`
	Priority            domain.Priority   `json:"priority,omitempty"`
	SpeedPref           domain.SpeedPref  `json:"speed_pref,omitempty"`
	Preemption          domain.Preemption `json:"preemption,omitempty"`
	NodeSelector        map[string]string `json:"node_selector,omitempty"`
}

type FleetPrompt struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

type FleetWave struct {
	ID       string         `json:"id"`
	Repeat   int            `json:"repeat,omitempty"`
	Parallel int            `json:"parallel,omitempty"`
	Jobs     []FleetWaveJob `json:"jobs"`
}

type FleetWaveJob struct {
	ID                    string   `json:"id,omitempty"`
	ModelID               string   `json:"model_id"`
	PromptID              string   `json:"prompt_id,omitempty"`
	GatewayID             string   `json:"gateway_id,omitempty"`
	DelayMS               int      `json:"delay_ms,omitempty"`
	ExpectedStatus        int      `json:"expected_status,omitempty"`
	ExpectedNodeID        string   `json:"expected_node_id,omitempty"`
	ExpectedDecision      string   `json:"expected_decision,omitempty"`
	ExpectedBackend       string   `json:"expected_backend,omitempty"`
	ExpectedAttempts      int      `json:"expected_attempts,omitempty"`
	ExpectedErrorContains string   `json:"expected_error_contains,omitempty"`
	ExpectedTraceContains []string `json:"expected_trace_contains,omitempty"`
	ExpectedFailure       bool     `json:"expected_failure,omitempty"`
}

type FleetSimulationConfig struct {
	Nodes     []domain.Node          `json:"nodes"`
	Presets   []domain.Preset        `json:"presets"`
	Instances []domain.ModelInstance `json:"instances,omitempty"`
}

type FleetBenchmarkSafety struct {
	MinDiskFreeRatio      float64 `json:"min_disk_free_ratio,omitempty"`
	MaxSparkGPUMemoryUtil float64 `json:"max_spark_gpu_memory_utilization,omitempty"`
	RequireTelemetry      *bool   `json:"require_telemetry,omitempty"`
}

type FleetRunOptions struct {
	Profile    string
	Simulate   bool
	OutputRoot string
	Client     *http.Client
	Clock      ports.Clock
}

type FleetRunResult struct {
	RunID     string              `json:"run_id"`
	Profile   string              `json:"profile"`
	Simulated bool                `json:"simulated"`
	OutputDir string              `json:"output_dir"`
	Manifest  FleetManifest       `json:"manifest"`
	Preflight FleetPreflight      `json:"preflight"`
	Events    []FleetEvent        `json:"events"`
	Results   []FleetJobResult    `json:"results"`
	Failures  []FleetFailure      `json:"failures"`
	Snapshots []FleetSnapshotMark `json:"snapshots"`
	Metrics   []domain.RunMetric  `json:"metrics"`
}

type FleetManifest struct {
	RunID      string              `json:"run_id"`
	ConfigID   string              `json:"config_id,omitempty"`
	Project    string              `json:"project"`
	Profile    string              `json:"profile"`
	Simulated  bool                `json:"simulated"`
	GitCommit  string              `json:"git_commit,omitempty"`
	Gateways   []FleetGateway      `json:"gateways"`
	Peers      []FleetPeerManifest `json:"peers,omitempty"`
	StartedAt  time.Time           `json:"started_at"`
	FinishedAt time.Time           `json:"finished_at"`
	Metadata   map[string]string   `json:"metadata,omitempty"`
}

type FleetPeerManifest struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type FleetPreflight struct {
	Profile string                    `json:"profile"`
	Passed  bool                      `json:"passed"`
	Proofs  []string                  `json:"proofs"`
	Plans   []FleetSimulationDecision `json:"plans"`
}

type FleetSimulationDecision struct {
	WaveID        string                   `json:"wave_id"`
	JobID         string                   `json:"job_id"`
	ModelID       string                   `json:"model_id"`
	GatewayID     string                   `json:"gateway_id,omitempty"`
	GatewayNodeID string                   `json:"gateway_node_id,omitempty"`
	Decision      domain.PlacementDecision `json:"decision"`
}

type FleetEvent struct {
	At        time.Time      `json:"at"`
	Type      string         `json:"type"`
	WaveID    string         `json:"wave_id,omitempty"`
	JobID     string         `json:"job_id,omitempty"`
	ModelID   string         `json:"model_id,omitempty"`
	GatewayID string         `json:"gateway_id,omitempty"`
	NodeID    string         `json:"node_id,omitempty"`
	Instance  string         `json:"instance_id,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
}

type FleetJobResult struct {
	WaveID            string             `json:"wave_id"`
	JobID             string             `json:"job_id"`
	ModelID           string             `json:"model_id"`
	RequestModel      string             `json:"request_model"`
	GatewayID         string             `json:"gateway_id"`
	NodeID            string             `json:"node_id,omitempty"`
	InstanceID        string             `json:"instance_id,omitempty"`
	Backend           string             `json:"backend,omitempty"`
	Decision          string             `json:"decision,omitempty"`
	Attempts          int                `json:"attempts,omitempty"`
	StatusCode        int                `json:"status_code"`
	OutputPath        string             `json:"output_path,omitempty"`
	Bytes             int                `json:"bytes"`
	DurationMS        int                `json:"duration_ms"`
	ContextTokens     int                `json:"context_tokens,omitempty"`
	TokensPerSec      float64            `json:"tokens_per_sec,omitempty"`
	Headers           map[string]string  `json:"headers,omitempty"`
	Trace             []domain.TraceStep `json:"trace,omitempty"`
	Error             string             `json:"error,omitempty"`
	RetryAllowed      bool               `json:"retry_allowed"`
	ExpectedFailure   bool               `json:"expected_failure,omitempty"`
	ExpectationErrors []string           `json:"expectation_errors,omitempty"`
}

type FleetFailure struct {
	JobID        string `json:"job_id,omitempty"`
	ModelID      string `json:"model_id,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	Error        string `json:"error"`
	RetryAllowed bool   `json:"retry_allowed"`
}

type FleetSnapshotMark struct {
	At       time.Time           `json:"at"`
	Stage    string              `json:"stage"`
	PeerID   string              `json:"peer_id"`
	Snapshot domain.NodeSnapshot `json:"snapshot"`
	Error    string              `json:"error,omitempty"`
}

func LoadFleetConfig(path string) (FleetBenchmarkConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return FleetBenchmarkConfig{}, err
	}
	var cfg FleetBenchmarkConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return FleetBenchmarkConfig{}, err
	}
	return cfg, nil
}

func RunFleet(ctx context.Context, cfg FleetBenchmarkConfig, opts FleetRunOptions) (FleetRunResult, error) {
	profile := opts.Profile
	if profile == "" {
		profile = FleetProfileConservative
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.System{}
	}
	if err := ValidateFleetConfig(cfg, profile, opts.Simulate); err != nil {
		return FleetRunResult{}, err
	}
	runID := cfg.ID
	if runID == "" {
		runID = "fleet-benchmark-" + strconv.FormatInt(clk.Now().UnixNano(), 10)
	}
	outputDir := filepath.Join(opts.OutputRoot, runID)
	if opts.OutputRoot == "" {
		return FleetRunResult{}, fmt.Errorf("output root is required")
	}
	if err := os.MkdirAll(filepath.Join(outputDir, "outputs"), 0755); err != nil {
		return FleetRunResult{}, err
	}
	result := FleetRunResult{
		RunID:     runID,
		Profile:   profile,
		Simulated: opts.Simulate,
		OutputDir: outputDir,
		Manifest: FleetManifest{
			RunID:     runID,
			ConfigID:  cfg.ID,
			Project:   cfg.Project,
			Profile:   profile,
			Simulated: opts.Simulate,
			GitCommit: gitCommit(),
			Gateways:  append([]FleetGateway(nil), cfg.Gateways...),
			Peers:     peerManifest(cfg.Peers),
			StartedAt: clk.Now(),
			Metadata:  copyStringMap(cfg.Metadata),
		},
	}
	preflight, err := SimulateFleet(ctx, cfg, profile, clk)
	result.Preflight = preflight
	if err != nil {
		result.Failures = append(result.Failures, FleetFailure{Error: err.Error()})
		result.Manifest.FinishedAt = clk.Now()
		_ = writeFleetArtifacts(outputDir, result)
		return result, err
	}
	result.Events = append(result.Events, FleetEvent{At: clk.Now(), Type: "preflight", Data: map[string]any{"proofs": preflight.Proofs}})
	if !opts.Simulate {
		live, err := runFleetLive(ctx, cfg, profile, outputDir, opts.Client, clk)
		result.Events = append(result.Events, live.Events...)
		result.Results = append(result.Results, live.Results...)
		result.Failures = append(result.Failures, live.Failures...)
		result.Snapshots = append(result.Snapshots, live.Snapshots...)
		result.Metrics = append(result.Metrics, live.Metrics...)
		if err != nil {
			result.Manifest.FinishedAt = clk.Now()
			_ = writeFleetArtifacts(outputDir, result)
			return result, err
		}
	}
	result.Manifest.FinishedAt = clk.Now()
	if err := writeFleetArtifacts(outputDir, result); err != nil {
		return result, err
	}
	return result, nil
}

func ValidateFleetConfig(cfg FleetBenchmarkConfig, profile string, simulate bool) error {
	if profile == "" {
		profile = FleetProfileConservative
	}
	switch profile {
	case FleetProfileConservative, FleetProfileSaturation, FleetProfileSoak:
	default:
		return fmt.Errorf("unknown fleet benchmark profile %q", profile)
	}
	if cfg.Project == "" {
		return fmt.Errorf("project is required")
	}
	if len(cfg.Gateways) == 0 {
		return fmt.Errorf("at least one gateway is required")
	}
	seenGateways := map[string]struct{}{}
	for _, gw := range cfg.Gateways {
		if gw.ID == "" {
			return fmt.Errorf("gateway id is required")
		}
		if gw.URL == "" {
			return fmt.Errorf("gateway %q url is required", gw.ID)
		}
		if _, ok := seenGateways[gw.ID]; ok {
			return fmt.Errorf("duplicate gateway id %q", gw.ID)
		}
		seenGateways[gw.ID] = struct{}{}
	}
	if len(cfg.Models) == 0 {
		return fmt.Errorf("at least one model is required")
	}
	seenModels := map[string]struct{}{}
	for _, model := range cfg.Models {
		if model.ID == "" {
			return fmt.Errorf("model id is required")
		}
		if _, ok := seenModels[model.ID]; ok {
			return fmt.Errorf("duplicate model id %q", model.ID)
		}
		seenModels[model.ID] = struct{}{}
	}
	if len(cfg.Prompts) == 0 {
		return fmt.Errorf("at least one prompt is required")
	}
	seenPrompts := map[string]struct{}{}
	for _, prompt := range cfg.Prompts {
		if prompt.ID == "" || prompt.Text == "" {
			return fmt.Errorf("prompt id and text are required")
		}
		if _, ok := seenPrompts[prompt.ID]; ok {
			return fmt.Errorf("duplicate prompt id %q", prompt.ID)
		}
		seenPrompts[prompt.ID] = struct{}{}
	}
	for _, wave := range cfg.Waves {
		for _, job := range wave.Jobs {
			if job.DelayMS < 0 {
				return fmt.Errorf("wave %q job %q delay_ms must be non-negative", wave.ID, job.ID)
			}
			if job.ExpectedStatus < 0 || job.ExpectedStatus > 599 {
				return fmt.Errorf("wave %q job %q expected_status must be between 0 and 599", wave.ID, job.ID)
			}
		}
	}
	minDisk := cfg.Safety.MinDiskFreeRatio
	if minDisk == 0 {
		minDisk = domain.DefaultDiskMinFreeRatio
	}
	if minDisk <= 0 || minDisk >= 1 {
		return fmt.Errorf("min_disk_free_ratio must be between 0 and 1")
	}
	if len(cfg.Simulation.Nodes) == 0 || len(cfg.Simulation.Presets) == 0 {
		return fmt.Errorf("simulation nodes and presets are required")
	}
	for _, node := range cfg.Simulation.Nodes {
		ratio := node.DiskMinFreeRatio
		if ratio == 0 {
			ratio = minDisk
		}
		if ratio <= 0 || ratio >= 1 {
			return fmt.Errorf("node %q disk_min_free_ratio must be between 0 and 1", node.ID)
		}
	}
	maxSparkUtil := cfg.Safety.MaxSparkGPUMemoryUtil
	if maxSparkUtil == 0 {
		maxSparkUtil = 0.85
	}
	if hasSparkNode(cfg.Simulation.Nodes) {
		for _, preset := range cfg.Simulation.Presets {
			if preset.Backend == domain.BackendVLLM {
				util, ok, err := gpuMemoryUtilization(preset.LaunchArgs)
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("vllm preset %q must declare --gpu-memory-utilization for Spark safety", preset.ID)
				}
				if util > maxSparkUtil {
					return fmt.Errorf("vllm preset %q gpu-memory-utilization %.2f exceeds Spark safety cap %.2f", preset.ID, util, maxSparkUtil)
				}
			}
		}
	}
	if !simulate && telemetryRequired(cfg) && len(cfg.Peers) == 0 {
		return fmt.Errorf("at least one telemetry peer endpoint is required for real fleet benchmark")
	}
	return nil
}

func SimulateFleet(ctx context.Context, cfg FleetBenchmarkConfig, profile string, clk ports.Clock) (FleetPreflight, error) {
	presets := append([]domain.Preset(nil), cfg.Simulation.Presets...)
	placer := scheduler.NewPlacer(
		estimate.NewBackendAware(estimate.NewInMemory(), estimate.NewInMemory()),
		lease.NewAllocator(),
		clk,
		presets...,
	)
	fleet := domain.FleetSnapshot{
		Nodes:     append([]domain.Node(nil), cfg.Simulation.Nodes...),
		Instances: append([]domain.ModelInstance(nil), cfg.Simulation.Instances...),
	}
	waves := cfg.Waves
	if len(waves) == 0 {
		waves = defaultWaves(cfg, profile)
	}
	models := modelByID(cfg.Models)
	gateways := gatewayByID(cfg.Gateways)
	var decisions []FleetSimulationDecision
	for waveIndex, wave := range waves {
		jobs := expandedWaveJobs(wave)
		for jobIndex, spec := range jobs {
			model, ok := models[spec.ModelID]
			if !ok {
				return FleetPreflight{}, fmt.Errorf("wave %q references unknown model %q", wave.ID, spec.ModelID)
			}
			gw := gatewayForJob(cfg.Gateways, gateways, spec.GatewayID, waveIndex+jobIndex)
			job := simulationJob(cfg, model, spec, wave.ID, jobIndex)
			decision, err := placer.Place(ctx, job, fleet)
			if err != nil {
				return FleetPreflight{}, err
			}
			decisions = append(decisions, FleetSimulationDecision{
				WaveID:        wave.ID,
				JobID:         job.ID,
				ModelID:       model.ID,
				GatewayID:     gw.ID,
				GatewayNodeID: gw.NodeID,
				Decision:      decision,
			})
			fleet = applySimulationDecision(fleet, job, model, decision)
		}
	}
	proofs, err := proveSimulation(profile, decisions)
	return FleetPreflight{Profile: profile, Passed: err == nil, Proofs: proofs, Plans: decisions}, err
}

type liveRun struct {
	Events    []FleetEvent
	Results   []FleetJobResult
	Failures  []FleetFailure
	Snapshots []FleetSnapshotMark
	Metrics   []domain.RunMetric
}

func runFleetLive(ctx context.Context, cfg FleetBenchmarkConfig, profile, outputDir string, client *http.Client, clk ports.Clock) (liveRun, error) {
	if client == nil {
		client = http.DefaultClient
	}
	waves := cfg.Waves
	if len(waves) == 0 {
		waves = defaultWaves(cfg, profile)
	}
	state := liveRun{}
	state.Snapshots = append(state.Snapshots, collectSnapshots(ctx, cfg, client, "before", clk)...)
	state.Metrics = append(state.Metrics, collectMetrics(ctx, cfg, client)...)
	models := modelByID(cfg.Models)
	gateways := gatewayByID(cfg.Gateways)
	for waveIndex, wave := range waves {
		state.Events = append(state.Events, FleetEvent{At: clk.Now(), Type: "wave_start", WaveID: wave.ID})
		jobs := expandedWaveJobs(wave)
		parallel := wave.Parallel
		if parallel <= 0 || parallel > len(jobs) {
			parallel = len(jobs)
		}
		results := make([]FleetJobResult, len(jobs))
		var wg sync.WaitGroup
		sem := make(chan struct{}, parallel)
		for idx, spec := range jobs {
			idx, spec := idx, spec
			model, ok := models[spec.ModelID]
			if !ok {
				return state, fmt.Errorf("wave %q references unknown model %q", wave.ID, spec.ModelID)
			}
			gw := gatewayForJob(cfg.Gateways, gateways, spec.GatewayID, waveIndex+idx)
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				result := submitFleetJob(ctx, cfg, wave.ID, idx, spec, model, gw, filepath.Join(outputDir, "outputs"), client, clk)
				results[idx] = result
			}()
		}
		wg.Wait()
		for _, result := range results {
			state.Results = append(state.Results, result)
			eventType := "complete"
			if result.Error != "" {
				eventType = "error"
				if result.ExpectedFailure && len(result.ExpectationErrors) == 0 {
					eventType = "expected_error"
				}
			}
			if len(result.ExpectationErrors) > 0 || (result.Error != "" && !result.ExpectedFailure) {
				failureText := result.Error
				if len(result.ExpectationErrors) > 0 {
					failureText = strings.Join(result.ExpectationErrors, "; ")
				}
				state.Failures = append(state.Failures, FleetFailure{JobID: result.JobID, ModelID: result.ModelID, NodeID: result.NodeID, Error: failureText, RetryAllowed: result.RetryAllowed})
			}
			state.Events = append(state.Events, FleetEvent{
				At:        clk.Now(),
				Type:      eventType,
				WaveID:    result.WaveID,
				JobID:     result.JobID,
				ModelID:   result.ModelID,
				GatewayID: result.GatewayID,
				NodeID:    result.NodeID,
				Instance:  result.InstanceID,
				Data: map[string]any{
					"decision":    result.Decision,
					"duration_ms": result.DurationMS,
					"attempts":    result.Attempts,
					"status":      result.StatusCode,
				},
			})
		}
		state.Snapshots = append(state.Snapshots, collectSnapshots(ctx, cfg, client, "after_"+wave.ID, clk)...)
		state.Events = append(state.Events, FleetEvent{At: clk.Now(), Type: "wave_done", WaveID: wave.ID})
	}
	state.Metrics = append(state.Metrics, collectMetrics(ctx, cfg, client)...)
	if len(state.Failures) > 0 {
		return state, fmt.Errorf("fleet benchmark completed with %d failures", len(state.Failures))
	}
	return state, nil
}

func submitFleetJob(ctx context.Context, cfg FleetBenchmarkConfig, waveID string, index int, spec FleetWaveJob, model FleetModel, gw FleetGateway, outputDir string, client *http.Client, clk ports.Clock) FleetJobResult {
	prompt, ok := promptByID(cfg.Prompts)[firstNonEmpty(spec.PromptID, model.PromptID)]
	if !ok {
		prompt = cfg.Prompts[0]
	}
	jobID := spec.ID
	if jobID == "" {
		jobID = fmt.Sprintf("%s-%s-%d", waveID, safeName(model.ID), index+1)
	}
	requestModel := model.RequestModel
	if requestModel == "" {
		requestModel = firstNonEmpty(model.PresetID, model.ID)
	}
	if err := waitFleetDelay(ctx, clk, spec.DelayMS); err != nil {
		result := FleetJobResult{WaveID: waveID, JobID: jobID, ModelID: model.ID, RequestModel: requestModel, GatewayID: gw.ID, Error: err.Error(), RetryAllowed: false, ExpectedFailure: spec.ExpectedFailure}
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
	}
	body, err := json.Marshal(api.OpenAIChatRequest{
		Model: requestModel,
		Messages: []api.OpenAIMessage{{
			Role:    "user",
			Content: prompt.Text,
		}},
		MaxTokens: model.MaxTokens,
	})
	if err != nil {
		result := FleetJobResult{WaveID: waveID, JobID: jobID, ModelID: model.ID, RequestModel: requestModel, GatewayID: gw.ID, Error: err.Error(), RetryAllowed: false, ExpectedFailure: spec.ExpectedFailure}
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(gw.URL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		result := FleetJobResult{WaveID: waveID, JobID: jobID, ModelID: model.ID, RequestModel: requestModel, GatewayID: gw.ID, Error: err.Error(), RetryAllowed: false, ExpectedFailure: spec.ExpectedFailure}
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Project != "" {
		req.Header.Set(fleetHeaderProject, cfg.Project)
	}
	if model.Priority != "" {
		req.Header.Set(fleetHeaderPriority, string(model.Priority))
	}
	if model.SpeedPref != "" {
		req.Header.Set(fleetHeaderSpeedPref, string(model.SpeedPref))
	}
	if model.Preemption != "" {
		req.Header.Set(fleetHeaderPreemption, string(model.Preemption))
	}
	if model.ContextRequest > 0 {
		req.Header.Set(fleetHeaderContextCap, strconv.Itoa(model.ContextRequest))
	}
	start := clk.Now()
	resp, err := client.Do(req)
	duration := elapsedMS(clk.Now().Sub(start))
	if err != nil {
		result := FleetJobResult{WaveID: waveID, JobID: jobID, ModelID: model.ID, RequestModel: requestModel, GatewayID: gw.ID, Error: err.Error(), DurationMS: duration, RetryAllowed: false, ExpectedFailure: spec.ExpectedFailure}
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		result := FleetJobResult{WaveID: waveID, JobID: jobID, ModelID: model.ID, RequestModel: requestModel, GatewayID: gw.ID, StatusCode: resp.StatusCode, Error: readErr.Error(), DurationMS: duration, RetryAllowed: false, ExpectedFailure: spec.ExpectedFailure}
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
	}
	headers := decisionHeaders(resp.Header)
	result := FleetJobResult{
		WaveID:          waveID,
		JobID:           jobID,
		ModelID:         model.ID,
		RequestModel:    requestModel,
		GatewayID:       gw.ID,
		NodeID:          headers[fleetHeaderNode],
		InstanceID:      headers[fleetHeaderInstance],
		Backend:         headers[fleetHeaderBackend],
		Decision:        headers[fleetHeaderDecision],
		StatusCode:      resp.StatusCode,
		DurationMS:      duration,
		Headers:         headers,
		ExpectedFailure: spec.ExpectedFailure,
	}
	if attempts, err := strconv.Atoi(headers[fleetHeaderAttempts]); err == nil {
		result.Attempts = attempts
	}
	if raw := headers[fleetHeaderTrace]; raw != "" {
		_ = json.Unmarshal([]byte(raw), &result.Trace)
	}
	if resp.StatusCode >= 400 {
		result.Error = strings.TrimSpace(string(data))
		result.RetryAllowed = resp.StatusCode == http.StatusTooManyRequests
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
	}
	var chat api.OpenAIChatResponse
	if err := json.Unmarshal(data, &chat); err != nil {
		result.Error = err.Error()
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
	}
	if len(chat.Choices) == 0 {
		result.Error = "gateway response had no choices"
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
	}
	text := chat.Choices[0].Message.Content
	path := filepath.Join(outputDir, safeName(jobID)+".txt")
	if err := os.WriteFile(path, []byte(text), 0644); err != nil {
		result.Error = err.Error()
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
	}
	result.OutputPath = path
	result.Bytes = len(text)
	result.ContextTokens = chat.Usage.TotalTokens
	if duration > 0 && chat.Usage.CompletionTokens > 0 {
		result.TokensPerSec = float64(chat.Usage.CompletionTokens) / (float64(duration) / 1000)
	}
	result.ExpectationErrors = validateFleetExpectation(spec, result)
	return result
}

func collectSnapshots(ctx context.Context, cfg FleetBenchmarkConfig, client *http.Client, stage string, clk ports.Clock) []FleetSnapshotMark {
	var out []FleetSnapshotMark
	for _, peer := range cfg.Peers {
		token := firstNonEmpty(peer.RPCToken, cfg.RPCToken)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peer.URL, "/")+"/snapshot", nil)
		if err != nil {
			out = append(out, FleetSnapshotMark{At: clk.Now(), Stage: stage, PeerID: peer.ID, Error: err.Error()})
			continue
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			out = append(out, FleetSnapshotMark{At: clk.Now(), Stage: stage, PeerID: peer.ID, Error: err.Error()})
			continue
		}
		var snap domain.NodeSnapshot
		if resp.StatusCode >= 400 {
			data, _ := io.ReadAll(resp.Body)
			out = append(out, FleetSnapshotMark{At: clk.Now(), Stage: stage, PeerID: peer.ID, Error: strings.TrimSpace(string(data))})
			_ = resp.Body.Close()
			continue
		}
		err = json.NewDecoder(resp.Body).Decode(&snap)
		_ = resp.Body.Close()
		mark := FleetSnapshotMark{At: clk.Now(), Stage: stage, PeerID: peer.ID, Snapshot: snap}
		if err != nil {
			mark.Error = err.Error()
		}
		out = append(out, mark)
	}
	return out
}

func waitFleetDelay(ctx context.Context, clk ports.Clock, delayMS int) error {
	if delayMS <= 0 {
		return nil
	}
	timer := clk.NewTimer(time.Duration(delayMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return nil
	}
}

func validateFleetExpectation(spec FleetWaveJob, result FleetJobResult) []string {
	var errs []string
	if spec.ExpectedFailure && result.Error == "" {
		errs = append(errs, "expected failure but request succeeded")
	}
	if !spec.ExpectedFailure && result.StatusCode >= 400 {
		errs = append(errs, fmt.Sprintf("expected success but got HTTP %d", result.StatusCode))
	}
	if spec.ExpectedStatus != 0 && result.StatusCode != spec.ExpectedStatus {
		errs = append(errs, fmt.Sprintf("expected HTTP %d, got %d", spec.ExpectedStatus, result.StatusCode))
	}
	if spec.ExpectedNodeID != "" && result.NodeID != spec.ExpectedNodeID {
		errs = append(errs, fmt.Sprintf("expected node %q, got %q", spec.ExpectedNodeID, result.NodeID))
	}
	if spec.ExpectedDecision != "" && result.Decision != spec.ExpectedDecision {
		errs = append(errs, fmt.Sprintf("expected decision %q, got %q", spec.ExpectedDecision, result.Decision))
	}
	if spec.ExpectedBackend != "" && result.Backend != spec.ExpectedBackend {
		errs = append(errs, fmt.Sprintf("expected backend %q, got %q", spec.ExpectedBackend, result.Backend))
	}
	if spec.ExpectedAttempts != 0 && result.Attempts != spec.ExpectedAttempts {
		errs = append(errs, fmt.Sprintf("expected attempts %d, got %d", spec.ExpectedAttempts, result.Attempts))
	}
	if spec.ExpectedErrorContains != "" && !strings.Contains(result.Error, spec.ExpectedErrorContains) {
		errs = append(errs, fmt.Sprintf("expected error containing %q, got %q", spec.ExpectedErrorContains, result.Error))
	}
	if len(spec.ExpectedTraceContains) > 0 {
		trace := result.Headers[fleetHeaderTrace]
		if trace == "" && len(result.Trace) > 0 {
			data, _ := json.Marshal(result.Trace)
			trace = string(data)
		}
		for _, want := range spec.ExpectedTraceContains {
			if !strings.Contains(trace, want) {
				errs = append(errs, fmt.Sprintf("expected trace containing %q, got %q", want, trace))
			}
		}
	}
	return errs
}

func collectMetrics(ctx context.Context, cfg FleetBenchmarkConfig, client *http.Client) []domain.RunMetric {
	var out []domain.RunMetric
	for _, peer := range cfg.Peers {
		token := firstNonEmpty(peer.RPCToken, cfg.RPCToken)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peer.URL, "/")+"/telemetry/metrics", nil)
		if err != nil {
			continue
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		if resp.StatusCode >= 400 {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			continue
		}
		var metrics []domain.RunMetric
		if err := json.NewDecoder(resp.Body).Decode(&metrics); err == nil {
			out = append(out, metrics...)
		}
		_ = resp.Body.Close()
	}
	return out
}

func writeFleetArtifacts(outputDir string, result FleetRunResult) error {
	if err := writeJSON(filepath.Join(outputDir, "manifest.json"), result.Manifest); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(outputDir, "results.json"), result); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(outputDir, "failures.json"), result.Failures); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(outputDir, "events.jsonl"), result.Events); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(outputDir, "snapshots.jsonl"), result.Snapshots); err != nil {
		return err
	}
	return writeReport(filepath.Join(outputDir, "report.html"), result)
}

func writeJSON(path string, value any) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func writeJSONL(path string, values any) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	defer w.Flush()
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	var list []json.RawMessage
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	for _, item := range list {
		if _, err := w.Write(item); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return nil
}

func writeReport(path string, result FleetRunResult) error {
	var b strings.Builder
	b.WriteString("<!doctype html><html><head><meta charset=\"utf-8\"><title>Mycelium Fleet Benchmark</title>")
	b.WriteString("<style>body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;margin:24px;color:#172026}table{border-collapse:collapse;width:100%;margin:12px 0}th,td{border:1px solid #d5dde5;padding:6px 8px;text-align:left;font-size:13px}th{background:#edf3f7}.ok{color:#146c43}.err{color:#b42318}code{background:#f2f5f7;padding:1px 4px;border-radius:3px}</style></head><body>")
	b.WriteString("<h1>Mycelium Fleet Benchmark</h1>")
	b.WriteString(fmt.Sprintf("<p><b>Run:</b> <code>%s</code> <b>Profile:</b> %s <b>Mode:</b> %s</p>", html.EscapeString(result.RunID), html.EscapeString(result.Profile), modeName(result.Simulated)))
	b.WriteString(fmt.Sprintf("<p><b>Preflight:</b> <span class=\"%s\">%s</span></p>", okClass(result.Preflight.Passed), passText(result.Preflight.Passed)))
	b.WriteString("<h2>Preflight Proofs</h2><ul>")
	for _, proof := range result.Preflight.Proofs {
		b.WriteString("<li>" + html.EscapeString(proof) + "</li>")
	}
	b.WriteString("</ul><h2>Results</h2><table><tr><th>Job</th><th>Model</th><th>Gateway</th><th>Node</th><th>Decision</th><th>Status</th><th>Expected failure</th><th>Duration ms</th><th>Tokens/sec</th><th>Error</th><th>Expectation errors</th></tr>")
	for _, row := range result.Results {
		b.WriteString("<tr>")
		for _, cell := range []string{row.JobID, row.ModelID, row.GatewayID, row.NodeID, row.Decision, strconv.Itoa(row.StatusCode), strconv.FormatBool(row.ExpectedFailure), strconv.Itoa(row.DurationMS), fmt.Sprintf("%.2f", row.TokensPerSec), row.Error, strings.Join(row.ExpectationErrors, "; ")} {
			b.WriteString("<td>" + html.EscapeString(cell) + "</td>")
		}
		b.WriteString("</tr>")
	}
	b.WriteString("</table><h2>Simulation Decisions</h2><table><tr><th>Job</th><th>Model</th><th>Submitter</th><th>Node</th><th>Action</th><th>Trace</th></tr>")
	for _, plan := range result.Preflight.Plans {
		trace, _ := json.Marshal(plan.Decision.Trace)
		b.WriteString("<tr><td>" + html.EscapeString(plan.JobID) + "</td><td>" + html.EscapeString(plan.ModelID) + "</td><td>" + html.EscapeString(plan.GatewayID) + "</td><td>" + html.EscapeString(plan.Decision.NodeID) + "</td><td>" + html.EscapeString(string(plan.Decision.Action)) + "</td><td><code>" + html.EscapeString(string(trace)) + "</code></td></tr>")
	}
	b.WriteString("</table></body></html>")
	return os.WriteFile(path, []byte(b.String()), 0644)
}

func defaultWaves(cfg FleetBenchmarkConfig, profile string) []FleetWave {
	if len(cfg.Models) == 0 {
		return nil
	}
	switch profile {
	case FleetProfileSaturation:
		var jobs []FleetWaveJob
		for _, model := range cfg.Models {
			for i := 0; i < 3; i++ {
				jobs = append(jobs, FleetWaveJob{ModelID: model.ID})
			}
		}
		return []FleetWave{{ID: "saturation", Parallel: len(jobs), Jobs: jobs}}
	case FleetProfileSoak:
		var waves []FleetWave
		for i := 0; i < 5; i++ {
			var jobs []FleetWaveJob
			for _, model := range cfg.Models {
				jobs = append(jobs, FleetWaveJob{ModelID: model.ID})
			}
			waves = append(waves, FleetWave{ID: fmt.Sprintf("soak-%02d", i+1), Parallel: len(jobs), Jobs: jobs})
		}
		return waves
	default:
		var jobs []FleetWaveJob
		for _, model := range cfg.Models {
			jobs = append(jobs, FleetWaveJob{ModelID: model.ID})
		}
		return []FleetWave{{ID: "conservative", Parallel: len(jobs), Jobs: jobs}}
	}
}

func expandedWaveJobs(wave FleetWave) []FleetWaveJob {
	repeat := wave.Repeat
	if repeat <= 0 {
		repeat = 1
	}
	var out []FleetWaveJob
	for i := 0; i < repeat; i++ {
		out = append(out, wave.Jobs...)
	}
	return out
}

func simulationJob(cfg FleetBenchmarkConfig, model FleetModel, spec FleetWaveJob, waveID string, idx int) domain.Job {
	modelName := model.RequestModel
	if modelName == "" {
		modelName = model.ID
	}
	priority := model.Priority
	if priority == "" {
		priority = domain.PriorityBackground
	}
	speed := model.SpeedPref
	if speed == "" {
		speed = domain.SpeedThroughput
	}
	preemption := model.Preemption
	if preemption == "" {
		preemption = domain.PreemptSoft
	}
	id := spec.ID
	if id == "" {
		id = fmt.Sprintf("sim-%s-%s-%d", waveID, safeName(model.ID), idx+1)
	}
	return domain.Job{
		ID:                  id,
		TaskType:            string(domain.CapabilityChat),
		Model:               modelName,
		PresetID:            model.PresetID,
		Project:             cfg.Project,
		Priority:            priority,
		SpeedPref:           speed,
		ContextRequest:      model.ContextRequest,
		ExpectedConcurrency: model.ExpectedConcurrency,
		Preemption:          preemption,
		NodeSelector:        copyStringMap(model.NodeSelector),
	}
}

func applySimulationDecision(fleet domain.FleetSnapshot, job domain.Job, model FleetModel, decision domain.PlacementDecision) domain.FleetSnapshot {
	out := domain.FleetSnapshot{
		Nodes:     append([]domain.Node(nil), fleet.Nodes...),
		Instances: append([]domain.ModelInstance(nil), fleet.Instances...),
	}
	for _, id := range decision.Preempted {
		out.Instances = removeBenchInstance(out.Instances, id)
	}
	switch decision.Action {
	case domain.ActionLoadedNew, domain.ActionDedicatedUnit, domain.ActionHardPreempted:
		presetID := firstNonEmpty(job.PresetID, model.PresetID, model.ID, job.Model)
		out.Instances = append(out.Instances, domain.ModelInstance{
			ID:             "sim-inst-" + safeName(job.ID),
			PresetID:       presetID,
			NodeID:         decision.NodeID,
			AcceleratorSet: append([]int(nil), decision.AcceleratorSet...),
			Claim:          decision.Claim,
			State:          domain.InstReady,
			Priority:       job.Priority,
		})
	case domain.ActionWarmInstance:
		for i := range out.Instances {
			if out.Instances[i].ID == decision.InstanceID {
				out.Instances[i].InFlight++
			}
		}
	}
	return out
}

func proveSimulation(profile string, decisions []FleetSimulationDecision) ([]string, error) {
	var proofs []string
	hasCold, hasWarm, hasPreempt, hasRemote, hasDiskDrop := false, false, false, false, false
	nodes := map[string]struct{}{}
	for _, plan := range decisions {
		if plan.Decision.NodeID != "" {
			nodes[plan.Decision.NodeID] = struct{}{}
		}
		switch plan.Decision.Action {
		case domain.ActionLoadedNew, domain.ActionDedicatedUnit:
			hasCold = true
		case domain.ActionWarmInstance:
			hasWarm = true
		case domain.ActionHardPreempted:
			hasPreempt = true
		}
		if plan.GatewayNodeID != "" && plan.Decision.NodeID != "" && plan.GatewayNodeID != plan.Decision.NodeID {
			hasRemote = true
		}
		if traceHasDiskDrop(plan.Decision.Trace) {
			hasDiskDrop = true
		}
		if plan.Decision.Action != domain.ActionQueued && plan.Decision.NodeID == "" {
			return proofs, fmt.Errorf("simulation produced non-queued decision without node for job %q", plan.JobID)
		}
	}
	if len(nodes) >= 2 {
		proofs = append(proofs, "multi-node placement observed")
	}
	if hasCold {
		proofs = append(proofs, "cold placement observed")
	}
	if hasWarm {
		proofs = append(proofs, "warm reuse observed")
	}
	if hasPreempt {
		proofs = append(proofs, "hard preemption/reallocation pressure observed")
	}
	if hasRemote {
		proofs = append(proofs, "submitted-from-one-peer placed-on-another observed")
	}
	if hasDiskDrop {
		proofs = append(proofs, "disk-headroom drop observed")
	}
	if profile == FleetProfileConservative {
		missing := []string{}
		if !hasCold {
			missing = append(missing, "cold placement")
		}
		if !hasWarm {
			missing = append(missing, "warm reuse")
		}
		if !hasPreempt {
			missing = append(missing, "hard preemption")
		}
		if !hasRemote {
			missing = append(missing, "remote placement")
		}
		if !hasDiskDrop {
			missing = append(missing, "disk-headroom drop")
		}
		if len(missing) > 0 {
			return proofs, fmt.Errorf("conservative simulation did not prove: %s", strings.Join(missing, ", "))
		}
	}
	if len(proofs) == 0 {
		return proofs, fmt.Errorf("simulation produced no benchmark proofs")
	}
	sort.Strings(proofs)
	return proofs, nil
}

func traceHasDiskDrop(trace []domain.TraceStep) bool {
	for _, step := range trace {
		if strings.Contains(step.Result, "disk.") {
			return true
		}
		for _, value := range step.Data {
			if strings.Contains(fmt.Sprint(value), "disk.") {
				return true
			}
		}
	}
	return false
}

func decisionHeaders(h http.Header) map[string]string {
	out := map[string]string{}
	for _, key := range []string{fleetHeaderDecision, fleetHeaderNode, fleetHeaderInstance, fleetHeaderBackend, fleetHeaderAttempts, fleetHeaderTrace} {
		if value := h.Get(key); value != "" {
			out[key] = value
		}
	}
	return out
}

func modelByID(models []FleetModel) map[string]FleetModel {
	out := map[string]FleetModel{}
	for _, model := range models {
		out[model.ID] = model
	}
	return out
}

func promptByID(prompts []FleetPrompt) map[string]FleetPrompt {
	out := map[string]FleetPrompt{}
	for _, prompt := range prompts {
		out[prompt.ID] = prompt
	}
	return out
}

func gatewayByID(gateways []FleetGateway) map[string]FleetGateway {
	out := map[string]FleetGateway{}
	for _, gw := range gateways {
		out[gw.ID] = gw
	}
	return out
}

func gatewayForJob(gateways []FleetGateway, byID map[string]FleetGateway, id string, index int) FleetGateway {
	if id != "" {
		if gw, ok := byID[id]; ok {
			return gw
		}
	}
	return gateways[index%len(gateways)]
}

func telemetryRequired(cfg FleetBenchmarkConfig) bool {
	if cfg.Safety.RequireTelemetry != nil {
		return *cfg.Safety.RequireTelemetry
	}
	return true
}

func peerManifest(peers []FleetPeer) []FleetPeerManifest {
	out := make([]FleetPeerManifest, 0, len(peers))
	for _, peer := range peers {
		out = append(out, FleetPeerManifest{ID: peer.ID, URL: peer.URL})
	}
	return out
}

func hasSparkNode(nodes []domain.Node) bool {
	for _, node := range nodes {
		if strings.Contains(strings.ToLower(node.ID+" "+node.Name), "spark") || node.Labels["gpu.kind"] == "gb10" {
			return true
		}
	}
	return false
}

func gpuMemoryUtilization(args []string) (float64, bool, error) {
	for i, arg := range args {
		if arg == "--gpu-memory-utilization" {
			if i+1 >= len(args) {
				return 0, false, fmt.Errorf("--gpu-memory-utilization missing value")
			}
			value, err := strconv.ParseFloat(args[i+1], 64)
			return value, true, err
		}
		if strings.HasPrefix(arg, "--gpu-memory-utilization=") {
			value, err := strconv.ParseFloat(strings.TrimPrefix(arg, "--gpu-memory-utilization="), 64)
			return value, true, err
		}
	}
	return 0, false, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func removeBenchInstance(instances []domain.ModelInstance, id string) []domain.ModelInstance {
	out := make([]domain.ModelInstance, 0, len(instances))
	for _, inst := range instances {
		if inst.ID != id {
			out = append(out, inst)
		}
	}
	return out
}

func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func modeName(simulated bool) string {
	if simulated {
		return "simulation"
	}
	return "real fleet"
}

func okClass(ok bool) string {
	if ok {
		return "ok"
	}
	return "err"
}

func passText(ok bool) string {
	if ok {
		return "passed"
	}
	return "failed"
}
