package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math"
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
	MinDiskFreeRatio        float64 `json:"min_disk_free_ratio,omitempty"`
	MaxSparkGPUMemoryUtil   float64 `json:"max_spark_gpu_memory_utilization,omitempty"`
	RequireTelemetry        *bool   `json:"require_telemetry,omitempty"`
	ResetBenchmarkInstances bool    `json:"reset_benchmark_instances,omitempty"`
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
	Resources []FleetResourceMark `json:"resources"`
	Metrics   []domain.RunMetric  `json:"metrics"`
}

type FleetManifest struct {
	Config     FleetBenchmarkConfig `json:"-"`
	RunID      string               `json:"run_id"`
	ConfigID   string               `json:"config_id,omitempty"`
	Project    string               `json:"project"`
	Profile    string               `json:"profile"`
	Simulated  bool                 `json:"simulated"`
	GitCommit  string               `json:"git_commit,omitempty"`
	Gateways   []FleetGateway       `json:"gateways"`
	Peers      []FleetPeerManifest  `json:"peers,omitempty"`
	StartedAt  time.Time            `json:"started_at"`
	FinishedAt time.Time            `json:"finished_at"`
	Metadata   map[string]string    `json:"metadata,omitempty"`
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
	WaveID               string             `json:"wave_id"`
	JobID                string             `json:"job_id"`
	ModelID              string             `json:"model_id"`
	RequestModel         string             `json:"request_model"`
	GatewayID            string             `json:"gateway_id"`
	PreflightNodeID      string             `json:"preflight_node_id,omitempty"`
	PreflightDecision    string             `json:"preflight_decision,omitempty"`
	LiveMatchesPreflight bool               `json:"live_matches_preflight,omitempty"`
	NodeID               string             `json:"node_id,omitempty"`
	InstanceID           string             `json:"instance_id,omitempty"`
	Backend              string             `json:"backend,omitempty"`
	Decision             string             `json:"decision,omitempty"`
	Attempts             int                `json:"attempts,omitempty"`
	StatusCode           int                `json:"status_code"`
	OutputPath           string             `json:"output_path,omitempty"`
	Bytes                int                `json:"bytes"`
	DurationMS           int                `json:"duration_ms"`
	ContextTokens        int                `json:"context_tokens,omitempty"`
	TokensPerSec         float64            `json:"tokens_per_sec,omitempty"`
	Headers              map[string]string  `json:"headers,omitempty"`
	Trace                []domain.TraceStep `json:"trace,omitempty"`
	Error                string             `json:"error,omitempty"`
	RetryAllowed         bool               `json:"retry_allowed"`
	ExpectedFailure      bool               `json:"expected_failure,omitempty"`
	ExpectationErrors    []string           `json:"expectation_errors,omitempty"`
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

type FleetResourceMark struct {
	At                             time.Time               `json:"at"`
	Stage                          string                  `json:"stage"`
	PeerID                         string                  `json:"peer_id"`
	NodeID                         string                  `json:"node_id"`
	DiskTotalMB                    int                     `json:"disk_total_mb,omitempty"`
	DiskFreeMB                     int                     `json:"disk_free_mb,omitempty"`
	DiskMinFreeRatio               float64                 `json:"disk_min_free_ratio,omitempty"`
	DiskFloorMB                    int                     `json:"disk_floor_mb,omitempty"`
	DiskHeadroomMB                 int                     `json:"disk_headroom_mb,omitempty"`
	LargestArtifactMB              int                     `json:"largest_artifact_mb,omitempty"`
	DiskFreeAfterLargestArtifactMB int                     `json:"disk_free_after_largest_artifact_mb,omitempty"`
	MaxUtil                        float64                 `json:"max_util,omitempty"`
	OOMSeverity                    domain.OOMSeverity      `json:"oom_severity,omitempty"`
	Accelerators                   []domain.Accelerator    `json:"accelerators,omitempty"`
	AcceleratorUsage               []FleetAcceleratorUsage `json:"accelerator_usage,omitempty"`
	Instances                      []FleetInstanceClaim    `json:"instances,omitempty"`
	Error                          string                  `json:"error,omitempty"`
}

type FleetAcceleratorUsage struct {
	Index                int `json:"index"`
	VRAMTotalMB          int `json:"vram_total_mb"`
	SnapshotUsedMB       int `json:"snapshot_used_mb"`
	ReservedClaimMB      int `json:"reserved_claim_mb,omitempty"`
	BenchmarkUsedMB      int `json:"benchmark_used_mb"`
	UsableMB             int `json:"usable_mb"`
	CatastrophicMarginMB int `json:"catastrophic_margin_mb,omitempty"`
}

type FleetInstanceClaim struct {
	ID             string               `json:"id"`
	PresetID       string               `json:"preset_id"`
	AcceleratorSet []int                `json:"accelerator_set,omitempty"`
	Claim          domain.Claim         `json:"claim"`
	State          domain.InstanceState `json:"state,omitempty"`
	Priority       domain.Priority      `json:"priority,omitempty"`
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
			Config:    cfg,
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
		live, err := runFleetLive(ctx, cfg, profile, preflight, outputDir, opts.Client, clk)
		result.Events = append(result.Events, live.Events...)
		result.Results = append(result.Results, live.Results...)
		result.Failures = append(result.Failures, live.Failures...)
		result.Snapshots = append(result.Snapshots, live.Snapshots...)
		result.Resources = append(result.Resources, live.Resources...)
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
	gatewaysByID := map[string]struct{}{}
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
		gatewaysByID[gw.ID] = struct{}{}
	}
	seenPeers := map[string]struct{}{}
	for _, peer := range cfg.Peers {
		if peer.ID == "" {
			return fmt.Errorf("peer id is required")
		}
		if peer.URL == "" {
			return fmt.Errorf("peer %q url is required", peer.ID)
		}
		if _, ok := seenPeers[peer.ID]; ok {
			return fmt.Errorf("duplicate peer id %q", peer.ID)
		}
		seenPeers[peer.ID] = struct{}{}
	}
	if len(cfg.Models) == 0 {
		return fmt.Errorf("at least one model is required")
	}
	seenModels := map[string]struct{}{}
	modelsByID := map[string]struct{}{}
	for _, model := range cfg.Models {
		if model.ID == "" {
			return fmt.Errorf("model id is required")
		}
		if _, ok := seenModels[model.ID]; ok {
			return fmt.Errorf("duplicate model id %q", model.ID)
		}
		seenModels[model.ID] = struct{}{}
		modelsByID[model.ID] = struct{}{}
		if model.Priority != "" && !validPriority(model.Priority) {
			return fmt.Errorf("model %q priority %q is invalid", model.ID, model.Priority)
		}
		if model.SpeedPref != "" && !validSpeedPref(model.SpeedPref) {
			return fmt.Errorf("model %q speed_pref %q is invalid", model.ID, model.SpeedPref)
		}
		if model.Preemption != "" && !validPreemption(model.Preemption) {
			return fmt.Errorf("model %q preemption %q is invalid", model.ID, model.Preemption)
		}
	}
	if len(cfg.Prompts) == 0 {
		return fmt.Errorf("at least one prompt is required")
	}
	seenPrompts := map[string]struct{}{}
	promptsByID := map[string]struct{}{}
	for _, prompt := range cfg.Prompts {
		if prompt.ID == "" || prompt.Text == "" {
			return fmt.Errorf("prompt id and text are required")
		}
		if _, ok := seenPrompts[prompt.ID]; ok {
			return fmt.Errorf("duplicate prompt id %q", prompt.ID)
		}
		seenPrompts[prompt.ID] = struct{}{}
		promptsByID[prompt.ID] = struct{}{}
	}
	for _, model := range cfg.Models {
		if model.PromptID != "" {
			if _, ok := promptsByID[model.PromptID]; !ok {
				return fmt.Errorf("model %q references unknown prompt %q", model.ID, model.PromptID)
			}
		}
	}
	for _, wave := range cfg.Waves {
		if wave.ID == "" {
			return fmt.Errorf("wave id is required")
		}
		for _, job := range wave.Jobs {
			if _, ok := modelsByID[job.ModelID]; !ok {
				return fmt.Errorf("wave %q references unknown model %q", wave.ID, job.ModelID)
			}
			if job.GatewayID != "" {
				if _, ok := gatewaysByID[job.GatewayID]; !ok {
					return fmt.Errorf("wave %q job %q references unknown gateway %q", wave.ID, job.ID, job.GatewayID)
				}
			}
			if job.PromptID != "" {
				if _, ok := promptsByID[job.PromptID]; !ok {
					return fmt.Errorf("wave %q job %q references unknown prompt %q", wave.ID, job.ID, job.PromptID)
				}
			}
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
	seenNodes := map[string]struct{}{}
	for _, node := range cfg.Simulation.Nodes {
		if node.ID == "" {
			return fmt.Errorf("simulation node id is required")
		}
		if _, ok := seenNodes[node.ID]; ok {
			return fmt.Errorf("duplicate simulation node id %q", node.ID)
		}
		seenNodes[node.ID] = struct{}{}
		if node.MaxUtil <= 0 || node.MaxUtil > 1 {
			return fmt.Errorf("node %q max_util must be between 0 and 1", node.ID)
		}
		if !validNodeStatus(node.Status) {
			return fmt.Errorf("node %q status %q is invalid", node.ID, node.Status)
		}
		if !validOOMSeverity(node.OOMSeverity) {
			return fmt.Errorf("node %q oom_severity %q is invalid", node.ID, node.OOMSeverity)
		}
		if node.DiskTotalMB > 0 && node.DiskFreeMB > node.DiskTotalMB {
			return fmt.Errorf("node %q disk_free_mb exceeds disk_total_mb", node.ID)
		}
		for _, acc := range node.Accelerators {
			if acc.VRAMTotalMB <= 0 {
				return fmt.Errorf("node %q accelerator %d vram_total_mb must be positive", node.ID, acc.Index)
			}
		}
		ratio := node.DiskMinFreeRatio
		if ratio == 0 {
			ratio = minDisk
		}
		if ratio <= 0 || ratio >= 1 {
			return fmt.Errorf("node %q disk_min_free_ratio must be between 0 and 1", node.ID)
		}
	}
	seenPresets := map[string]struct{}{}
	for _, preset := range cfg.Simulation.Presets {
		if preset.ID == "" {
			return fmt.Errorf("simulation preset id is required")
		}
		if _, ok := seenPresets[preset.ID]; ok {
			return fmt.Errorf("duplicate simulation preset id %q", preset.ID)
		}
		seenPresets[preset.ID] = struct{}{}
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
			gw, err := gatewayForJob(cfg.Gateways, gateways, spec.GatewayID, waveIndex+jobIndex)
			if err != nil {
				return FleetPreflight{}, err
			}
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
	Resources []FleetResourceMark
	Metrics   []domain.RunMetric
}

func runFleetLive(ctx context.Context, cfg FleetBenchmarkConfig, profile string, preflight FleetPreflight, outputDir string, client *http.Client, clk ports.Clock) (liveRun, error) {
	if client == nil {
		client = http.DefaultClient
	}
	waves := cfg.Waves
	if len(waves) == 0 {
		waves = defaultWaves(cfg, profile)
	}
	state := liveRun{}
	beforeSnapshots := collectSnapshots(ctx, cfg, client, "before", clk)
	state.Snapshots = append(state.Snapshots, beforeSnapshots...)
	state.Resources = append(state.Resources, resourcesFromSnapshots(cfg, beforeSnapshots)...)
	state.Failures = append(state.Failures, snapshotFailures(cfg, beforeSnapshots)...)
	state.Failures = append(state.Failures, liveSnapshotMismatchFailures(cfg, beforeSnapshots)...)
	if cfg.Safety.ResetBenchmarkInstances {
		prepEvents, prepFailures := resetBenchmarkInstances(ctx, cfg, beforeSnapshots, client, clk)
		state.Events = append(state.Events, prepEvents...)
		state.Failures = append(state.Failures, prepFailures...)
		afterReset := collectSnapshots(ctx, cfg, client, "after_reset", clk)
		state.Snapshots = append(state.Snapshots, afterReset...)
		afterResetResources := resourcesFromSnapshots(cfg, afterReset)
		state.Resources = append(state.Resources, afterResetResources...)
		state.Failures = append(state.Failures, snapshotFailures(cfg, afterReset)...)
		state.Failures = append(state.Failures, liveSnapshotMismatchFailures(cfg, afterReset)...)
		state.Failures = append(state.Failures, resourceSafetyFailures(cfg, afterResetResources)...)
	} else {
		state.Failures = append(state.Failures, resourceSafetyFailures(cfg, state.Resources)...)
	}
	metrics, metricFailures := collectMetrics(ctx, cfg, client)
	state.Metrics = append(state.Metrics, metrics...)
	state.Failures = append(state.Failures, metricFailures...)
	if len(state.Failures) > 0 {
		return state, fmt.Errorf("fleet benchmark evidence collection failed with %d failures", len(state.Failures))
	}
	models := modelByID(cfg.Models)
	gateways := gatewayByID(cfg.Gateways)
	preflightPlans := preflightByJob(preflight.Plans)
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
			gw, err := gatewayForJob(cfg.Gateways, gateways, spec.GatewayID, waveIndex+idx)
			if err != nil {
				return state, err
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				result := submitFleetJob(ctx, cfg, wave.ID, idx, spec, model, gw, filepath.Join(outputDir, "outputs"), client, clk)
				result = attachPreflightResult(result, preflightPlans[result.JobID])
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
		afterSnapshots := collectSnapshots(ctx, cfg, client, "after_"+wave.ID, clk)
		state.Snapshots = append(state.Snapshots, afterSnapshots...)
		afterResources := resourcesFromSnapshots(cfg, afterSnapshots)
		state.Resources = append(state.Resources, afterResources...)
		state.Failures = append(state.Failures, snapshotFailures(cfg, afterSnapshots)...)
		state.Failures = append(state.Failures, liveSnapshotMismatchFailures(cfg, afterSnapshots)...)
		state.Failures = append(state.Failures, resourceSafetyFailures(cfg, afterResources)...)
		state.Failures = append(state.Failures, livePlacementEvidenceFailures(results, state.Snapshots)...)
		state.Failures = append(state.Failures, livePreflightMismatchFailures(results)...)
		state.Events = append(state.Events, FleetEvent{At: clk.Now(), Type: "wave_done", WaveID: wave.ID})
	}
	metrics, metricFailures = collectMetrics(ctx, cfg, client)
	state.Metrics = append(state.Metrics, metrics...)
	state.Failures = append(state.Failures, metricFailures...)
	if len(state.Failures) > 0 {
		return state, fmt.Errorf("fleet benchmark completed with %d failures", len(state.Failures))
	}
	return state, nil
}

func resetBenchmarkInstances(ctx context.Context, cfg FleetBenchmarkConfig, snapshots []FleetSnapshotMark, client *http.Client, clk ports.Clock) ([]FleetEvent, []FleetFailure) {
	presetIDs := benchmarkPresetIDs(cfg)
	peers := peerByID(cfg.Peers)
	var events []FleetEvent
	var failures []FleetFailure
	for _, mark := range snapshots {
		if mark.Error != "" || mark.Snapshot.Node.ID == "" {
			continue
		}
		peer, ok := peers[mark.PeerID]
		if !ok {
			failures = append(failures, FleetFailure{NodeID: mark.PeerID, Error: "reset peer is not declared"})
			continue
		}
		for _, inst := range mark.Snapshot.Instances {
			if _, ok := presetIDs[inst.PresetID]; !ok {
				continue
			}
			if inst.InFlight > 0 || inst.Loading || inst.Pinned || inst.ReservationID != "" {
				failures = append(failures, FleetFailure{
					NodeID: mark.Snapshot.Node.ID,
					Error:  fmt.Sprintf("cannot reset benchmark instance %s preset=%s in_flight=%d loading=%t pinned=%t reservation=%s", inst.ID, inst.PresetID, inst.InFlight, inst.Loading, inst.Pinned, inst.ReservationID),
				})
				continue
			}
			if err := unloadFleetInstance(ctx, cfg, peer, inst.ID, client); err != nil {
				failures = append(failures, FleetFailure{NodeID: mark.Snapshot.Node.ID, Error: fmt.Sprintf("reset unload %s: %v", inst.ID, err)})
				continue
			}
			events = append(events, FleetEvent{
				At:       clk.Now(),
				Type:     "reset_unload",
				NodeID:   mark.Snapshot.Node.ID,
				Instance: inst.ID,
				Data: map[string]any{
					"preset_id": inst.PresetID,
				},
			})
		}
	}
	return events, failures
}

func unloadFleetInstance(ctx context.Context, cfg FleetBenchmarkConfig, peer FleetPeer, instanceID string, client *http.Client) error {
	if client == nil {
		client = http.DefaultClient
	}
	body, err := json.Marshal(map[string]string{"instance_id": instanceID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(peer.URL, "/")+"/unload", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := firstNonEmpty(peer.RPCToken, cfg.RPCToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	return nil
}

func submitFleetJob(ctx context.Context, cfg FleetBenchmarkConfig, waveID string, index int, spec FleetWaveJob, model FleetModel, gw FleetGateway, outputDir string, client *http.Client, clk ports.Clock) FleetJobResult {
	prompt, ok := promptByID(cfg.Prompts)[firstNonEmpty(spec.PromptID, model.PromptID)]
	if !ok {
		result := FleetJobResult{WaveID: waveID, JobID: firstNonEmpty(spec.ID, fmt.Sprintf("%s-%s-%d", waveID, safeName(model.ID), index+1)), ModelID: model.ID, GatewayID: gw.ID, Error: fmt.Sprintf("prompt %q is not defined", firstNonEmpty(spec.PromptID, model.PromptID)), RetryAllowed: false, ExpectedFailure: spec.ExpectedFailure}
		result.ExpectationErrors = validateFleetExpectation(spec, result)
		return result
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
		if err := json.Unmarshal([]byte(raw), &result.Trace); err != nil {
			result.Error = "decode X-Myc-Trace: " + err.Error()
			result.ExpectationErrors = validateFleetExpectation(spec, result)
			return result
		}
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

func collectMetrics(ctx context.Context, cfg FleetBenchmarkConfig, client *http.Client) ([]domain.RunMetric, []FleetFailure) {
	var out []domain.RunMetric
	var failures []FleetFailure
	required := telemetryRequired(cfg)
	for _, peer := range cfg.Peers {
		token := firstNonEmpty(peer.RPCToken, cfg.RPCToken)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peer.URL, "/")+"/telemetry/metrics", nil)
		if err != nil {
			if required {
				failures = append(failures, FleetFailure{NodeID: peer.ID, Error: "telemetry request: " + err.Error()})
			}
			continue
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			if required {
				failures = append(failures, FleetFailure{NodeID: peer.ID, Error: "telemetry request: " + err.Error()})
			}
			continue
		}
		if resp.StatusCode >= 400 {
			data, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if required {
				failures = append(failures, FleetFailure{NodeID: peer.ID, Error: "telemetry request: " + strings.TrimSpace(string(data))})
			}
			continue
		}
		var metrics []domain.RunMetric
		if err := json.NewDecoder(resp.Body).Decode(&metrics); err == nil {
			out = append(out, metrics...)
		} else if required {
			failures = append(failures, FleetFailure{NodeID: peer.ID, Error: "telemetry decode: " + err.Error()})
		}
		_ = resp.Body.Close()
	}
	return out, failures
}

func snapshotFailures(cfg FleetBenchmarkConfig, snapshots []FleetSnapshotMark) []FleetFailure {
	if !telemetryRequired(cfg) {
		return nil
	}
	var failures []FleetFailure
	for _, mark := range snapshots {
		if mark.Error != "" {
			failures = append(failures, FleetFailure{NodeID: mark.PeerID, Error: "snapshot " + mark.Stage + ": " + mark.Error})
		}
	}
	return failures
}

func liveSnapshotMismatchFailures(cfg FleetBenchmarkConfig, snapshots []FleetSnapshotMark) []FleetFailure {
	simNodes := map[string]struct{}{}
	for _, node := range cfg.Simulation.Nodes {
		simNodes[node.ID] = struct{}{}
	}
	var failures []FleetFailure
	for _, mark := range snapshots {
		if mark.Error != "" || mark.Snapshot.Node.ID == "" {
			continue
		}
		if _, ok := simNodes[mark.Snapshot.Node.ID]; !ok {
			failures = append(failures, FleetFailure{NodeID: mark.Snapshot.Node.ID, Error: "live snapshot node is not declared in simulation config"})
		}
	}
	return failures
}

func livePlacementEvidenceFailures(results []FleetJobResult, snapshots []FleetSnapshotMark) []FleetFailure {
	knownNodes := map[string]struct{}{}
	knownInstances := map[string]map[string]struct{}{}
	for _, mark := range snapshots {
		if mark.Error == "" && mark.Snapshot.Node.ID != "" {
			nodeID := mark.Snapshot.Node.ID
			knownNodes[nodeID] = struct{}{}
			if knownInstances[nodeID] == nil {
				knownInstances[nodeID] = map[string]struct{}{}
			}
			for _, inst := range mark.Snapshot.Instances {
				if inst.ID != "" {
					knownInstances[nodeID][inst.ID] = struct{}{}
				}
			}
		}
	}
	var failures []FleetFailure
	for _, result := range results {
		if result.Error != "" || result.ExpectedFailure {
			continue
		}
		if result.NodeID == "" {
			failures = append(failures, FleetFailure{JobID: result.JobID, ModelID: result.ModelID, Error: "successful request did not report X-Myc-Node"})
			continue
		}
		if _, ok := knownNodes[result.NodeID]; !ok {
			failures = append(failures, FleetFailure{JobID: result.JobID, ModelID: result.ModelID, NodeID: result.NodeID, Error: "placement node was not present in authenticated fleet snapshots"})
			continue
		}
		if result.InstanceID == "" {
			failures = append(failures, FleetFailure{JobID: result.JobID, ModelID: result.ModelID, NodeID: result.NodeID, Error: "successful request did not report X-Myc-Instance"})
		} else if _, ok := knownInstances[result.NodeID][result.InstanceID]; !ok {
			failures = append(failures, FleetFailure{JobID: result.JobID, ModelID: result.ModelID, NodeID: result.NodeID, Error: "placement instance was not present in authenticated fleet snapshots"})
		}
		if result.Backend == "" {
			failures = append(failures, FleetFailure{JobID: result.JobID, ModelID: result.ModelID, NodeID: result.NodeID, Error: "successful request did not report X-Myc-Backend"})
		}
		if result.Decision == "" {
			failures = append(failures, FleetFailure{JobID: result.JobID, ModelID: result.ModelID, NodeID: result.NodeID, Error: "successful request did not report X-Myc-Decision"})
		}
	}
	return failures
}

func livePreflightMismatchFailures(results []FleetJobResult) []FleetFailure {
	var failures []FleetFailure
	for _, result := range results {
		if result.Error != "" || result.ExpectedFailure || result.PreflightDecision == "" {
			continue
		}
		if !result.LiveMatchesPreflight {
			failures = append(failures, FleetFailure{
				JobID:   result.JobID,
				ModelID: result.ModelID,
				NodeID:  result.NodeID,
				Error:   fmt.Sprintf("live placement %s/%s did not match preflight %s/%s", result.Decision, result.NodeID, result.PreflightDecision, result.PreflightNodeID),
			})
		}
	}
	return failures
}

func preflightByJob(plans []FleetSimulationDecision) map[string]FleetSimulationDecision {
	out := map[string]FleetSimulationDecision{}
	for _, plan := range plans {
		if plan.JobID != "" {
			out[plan.JobID] = plan
		}
	}
	return out
}

func benchmarkPresetIDs(cfg FleetBenchmarkConfig) map[string]struct{} {
	out := map[string]struct{}{}
	for _, preset := range cfg.Simulation.Presets {
		if preset.ID != "" {
			out[preset.ID] = struct{}{}
		}
	}
	for _, model := range cfg.Models {
		if model.PresetID != "" {
			out[model.PresetID] = struct{}{}
		}
	}
	return out
}

func peerByID(peers []FleetPeer) map[string]FleetPeer {
	out := map[string]FleetPeer{}
	for _, peer := range peers {
		if peer.ID != "" {
			out[peer.ID] = peer
		}
	}
	return out
}

func attachPreflightResult(result FleetJobResult, plan FleetSimulationDecision) FleetJobResult {
	if plan.JobID == "" {
		return result
	}
	result.PreflightNodeID = plan.Decision.NodeID
	result.PreflightDecision = string(plan.Decision.Action)
	if result.Error != "" || result.ExpectedFailure {
		return result
	}
	result.LiveMatchesPreflight = result.Decision == result.PreflightDecision && result.NodeID == result.PreflightNodeID
	return result
}

func resourceSafetyFailures(cfg FleetBenchmarkConfig, resources []FleetResourceMark) []FleetFailure {
	minDisk := cfg.Safety.MinDiskFreeRatio
	if minDisk == 0 {
		minDisk = domain.DefaultDiskMinFreeRatio
	}
	var failures []FleetFailure
	for _, resource := range resources {
		if resource.Error != "" {
			continue
		}
		if resource.NodeID == "" {
			failures = append(failures, FleetFailure{NodeID: resource.PeerID, Error: "snapshot did not include node identity"})
			continue
		}
		if !validOOMSeverity(resource.OOMSeverity) {
			failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: fmt.Sprintf("invalid oom_severity %q", resource.OOMSeverity)})
		}
		ratio := resource.DiskMinFreeRatio
		if ratio == 0 {
			ratio = minDisk
		}
		if resource.DiskTotalMB <= 0 {
			failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: "snapshot did not include disk_total_mb"})
		} else if ratio <= 0 || ratio >= 1 {
			failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: fmt.Sprintf("invalid disk_min_free_ratio %.3f", ratio)})
		} else {
			floor := int(math.Ceil(float64(resource.DiskTotalMB) * ratio))
			if resource.DiskFreeMB <= floor {
				failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: fmt.Sprintf("disk free %dMB is at or below floor %dMB", resource.DiskFreeMB, floor)})
			}
			if resource.LargestArtifactMB > 0 && resource.DiskFreeAfterLargestArtifactMB <= floor {
				failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: fmt.Sprintf("largest artifact %dMB would leave disk free %dMB at or below floor %dMB", resource.LargestArtifactMB, resource.DiskFreeAfterLargestArtifactMB, floor)})
			}
		}
		if resource.MaxUtil <= 0 || resource.MaxUtil > 1 {
			failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: fmt.Sprintf("invalid max_util %.3f", resource.MaxUtil)})
			continue
		}
		if len(resource.AcceleratorUsage) > 0 {
			for _, usage := range resource.AcceleratorUsage {
				if usage.VRAMTotalMB <= 0 {
					failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: fmt.Sprintf("accelerator %d missing vram_total_mb", usage.Index)})
					continue
				}
				if usage.BenchmarkUsedMB > usage.UsableMB {
					failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: fmt.Sprintf("accelerator %d used %dMB exceeds benchmark limit %dMB", usage.Index, usage.BenchmarkUsedMB, usage.UsableMB)})
				}
			}
		} else {
			for _, acc := range resource.Accelerators {
				if acc.VRAMTotalMB <= 0 {
					failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: fmt.Sprintf("accelerator %d missing vram_total_mb", acc.Index)})
					continue
				}
				limit := int(float64(acc.VRAMTotalMB) * resource.MaxUtil)
				if resource.OOMSeverity == domain.OOMCatastrophic {
					limit -= int(float64(acc.VRAMTotalMB) * 0.05)
				}
				if acc.VRAMUsedMB > limit {
					failures = append(failures, FleetFailure{NodeID: resource.NodeID, Error: fmt.Sprintf("accelerator %d used %dMB exceeds benchmark limit %dMB", acc.Index, acc.VRAMUsedMB, limit)})
				}
			}
		}
	}
	return failures
}

func resourcesFromSnapshots(cfg FleetBenchmarkConfig, snapshots []FleetSnapshotMark) []FleetResourceMark {
	out := make([]FleetResourceMark, 0, len(snapshots))
	for _, mark := range snapshots {
		resource := FleetResourceMark{
			At:     mark.At,
			Stage:  mark.Stage,
			PeerID: mark.PeerID,
			Error:  mark.Error,
		}
		if mark.Error == "" {
			node := mark.Snapshot.Node
			ratio := node.DiskMinFreeRatio
			if ratio == 0 {
				ratio = cfg.Safety.MinDiskFreeRatio
			}
			if ratio == 0 {
				ratio = domain.DefaultDiskMinFreeRatio
			}
			floor := 0
			if node.DiskTotalMB > 0 {
				floor = int(math.Ceil(float64(node.DiskTotalMB) * ratio))
			}
			largestArtifact := largestArtifactForNode(cfg, node.ID)
			resource.NodeID = node.ID
			resource.DiskTotalMB = node.DiskTotalMB
			resource.DiskFreeMB = node.DiskFreeMB
			resource.DiskMinFreeRatio = node.DiskMinFreeRatio
			resource.DiskFloorMB = floor
			resource.DiskHeadroomMB = node.DiskFreeMB - floor
			resource.LargestArtifactMB = largestArtifact
			resource.DiskFreeAfterLargestArtifactMB = node.DiskFreeMB - largestArtifact
			resource.MaxUtil = node.MaxUtil
			resource.OOMSeverity = node.OOMSeverity
			resource.Accelerators = append([]domain.Accelerator(nil), node.Accelerators...)
			resource.Instances = instanceClaims(mark.Snapshot.Instances)
			resource.AcceleratorUsage = acceleratorUsage(node, mark.Snapshot.Instances)
		}
		out = append(out, resource)
	}
	return out
}

func largestArtifactForNode(cfg FleetBenchmarkConfig, nodeID string) int {
	largest := 0
	for _, preset := range cfg.Simulation.Presets {
		if preset.NodeID != "" && preset.NodeID != nodeID {
			continue
		}
		if preset.ArtifactSizeMB > largest {
			largest = preset.ArtifactSizeMB
		}
	}
	return largest
}

func instanceClaims(instances []domain.ModelInstance) []FleetInstanceClaim {
	out := make([]FleetInstanceClaim, 0, len(instances))
	for _, inst := range instances {
		out = append(out, FleetInstanceClaim{
			ID:             inst.ID,
			PresetID:       inst.PresetID,
			AcceleratorSet: append([]int(nil), inst.AcceleratorSet...),
			Claim:          inst.Claim,
			State:          inst.State,
			Priority:       inst.Priority,
		})
	}
	return out
}

func acceleratorUsage(node domain.Node, instances []domain.ModelInstance) []FleetAcceleratorUsage {
	out := make([]FleetAcceleratorUsage, 0, len(node.Accelerators))
	for _, acc := range node.Accelerators {
		reserved := reservedClaimForAccelerator(acc.Index, instances)
		margin := 0
		usable := int(float64(acc.VRAMTotalMB) * node.MaxUtil)
		if node.OOMSeverity == domain.OOMCatastrophic {
			margin = int(float64(acc.VRAMTotalMB) * 0.05)
			usable -= margin
		}
		used := acc.VRAMUsedMB
		if reserved > used {
			used = reserved
		}
		out = append(out, FleetAcceleratorUsage{
			Index:                acc.Index,
			VRAMTotalMB:          acc.VRAMTotalMB,
			SnapshotUsedMB:       acc.VRAMUsedMB,
			ReservedClaimMB:      reserved,
			BenchmarkUsedMB:      used,
			UsableMB:             usable,
			CatastrophicMarginMB: margin,
		})
	}
	return out
}

func reservedClaimForAccelerator(index int, instances []domain.ModelInstance) int {
	total := 0
	for _, inst := range instances {
		if len(inst.AcceleratorSet) == 0 {
			continue
		}
		if containsAccelerator(inst.AcceleratorSet, index) {
			claim := inst.Claim.WeightsMB + inst.Claim.KVReservedMB
			total += int(math.Ceil(float64(claim) / float64(len(inst.AcceleratorSet))))
		}
	}
	return total
}

func containsAccelerator(set []int, index int) bool {
	for _, got := range set {
		if got == index {
			return true
		}
	}
	return false
}

func writeFleetArtifacts(outputDir string, result FleetRunResult) error {
	if err := writeJSON(filepath.Join(outputDir, "config.json"), result.Manifest.Config); err != nil {
		return err
	}
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
	if err := writeJSONL(filepath.Join(outputDir, "resources.jsonl"), result.Resources); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(outputDir, "metrics.json"), result.Metrics); err != nil {
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
		first := cfg.Models[0].ID
		last := cfg.Models[len(cfg.Models)-1].ID
		firstGateway := ""
		secondGateway := ""
		if len(cfg.Gateways) > 0 {
			firstGateway = cfg.Gateways[0].ID
			secondGateway = cfg.Gateways[0].ID
		}
		if len(cfg.Gateways) > 1 {
			secondGateway = cfg.Gateways[1].ID
		}
		return []FleetWave{
			{ID: "conservative-cold", Jobs: []FleetWaveJob{{ModelID: first, GatewayID: firstGateway}}},
			{ID: "conservative-warm", Jobs: []FleetWaveJob{{ModelID: first, GatewayID: secondGateway}}},
			{ID: "conservative-fit-forced", Jobs: []FleetWaveJob{{ModelID: last, GatewayID: firstGateway}}},
		}
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
		id = fmt.Sprintf("%s-%s-%d", waveID, safeName(model.ID), idx+1)
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

func gatewayForJob(gateways []FleetGateway, byID map[string]FleetGateway, id string, index int) (FleetGateway, error) {
	if id != "" {
		if gw, ok := byID[id]; ok {
			return gw, nil
		}
		return FleetGateway{}, fmt.Errorf("unknown gateway %q", id)
	}
	return gateways[index%len(gateways)], nil
}

func validPriority(priority domain.Priority) bool {
	switch priority {
	case domain.PriorityInteractive, domain.PriorityNormal, domain.PriorityBackground:
		return true
	default:
		return false
	}
}

func validSpeedPref(speed domain.SpeedPref) bool {
	switch speed {
	case domain.SpeedThroughput, domain.SpeedLatency, domain.SpeedAuto:
		return true
	default:
		return false
	}
}

func validPreemption(preemption domain.Preemption) bool {
	switch preemption {
	case domain.PreemptInherit, domain.PreemptSoft, domain.PreemptHardForInteractive, domain.PreemptHard:
		return true
	default:
		return false
	}
}

func validNodeStatus(status domain.NodeStatus) bool {
	switch status {
	case domain.NodeReady, domain.NodeMaintenance, domain.NodeDraining, domain.NodeUnreachable:
		return true
	default:
		return false
	}
}

func validOOMSeverity(severity domain.OOMSeverity) bool {
	switch severity {
	case domain.OOMSoft, domain.OOMCatastrophic:
		return true
	default:
		return false
	}
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
