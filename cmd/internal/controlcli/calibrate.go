package controlcli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"mycelium/internal/bench"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	storesqlite "mycelium/internal/store/sqlite"
)

const calibrationPlanSchemaVersion = "mycelium.calibration_plan.v1"

type calibrationPlanArtifact struct {
	SchemaVersion       string                 `json:"schema_version"`
	CreatedAt           time.Time              `json:"created_at"`
	Profile             string                 `json:"profile"`
	Project             string                 `json:"project"`
	ConfigPath          string                 `json:"config_path"`
	DBPath              string                 `json:"db_path"`
	BenchmarkConfigPath string                 `json:"benchmark_config_path"`
	Nodes               []calibrationNodePlan  `json:"nodes"`
	Models              []calibrationModelPlan `json:"models"`
	Sweeps              []calibrationSweepPlan `json:"sweeps"`
	Safety              []string               `json:"safety"`
	Preflight           bench.FleetPreflight   `json:"preflight"`
	RunCommand          []string               `json:"run_command"`
	LiveRunCommand      []string               `json:"live_run_command,omitempty"`
	Notes               []string               `json:"notes,omitempty"`
}

type calibrationNodePlan struct {
	ID             string             `json:"id"`
	Status         domain.NodeStatus  `json:"status"`
	OOMSeverity    domain.OOMSeverity `json:"oom_severity"`
	MaxUtil        float64            `json:"max_util"`
	Accelerators   int                `json:"accelerators"`
	TotalVRAMMB    int                `json:"total_vram_mb"`
	DiskFreeMB     int                `json:"disk_free_mb,omitempty"`
	DiskTotalMB    int                `json:"disk_total_mb,omitempty"`
	SpeedClassTPS  float64            `json:"speed_class_tokens_per_sec,omitempty"`
	HasPeerAddress bool               `json:"has_peer_address"`
}

type calibrationModelPlan struct {
	ID                  string            `json:"id"`
	RequestModel        string            `json:"request_model"`
	PresetID            string            `json:"preset_id"`
	Backend             domain.Backend    `json:"backend"`
	ContextLength       int               `json:"context_length"`
	ContextRequest      int               `json:"context_request"`
	ExpectedConcurrency int               `json:"expected_concurrency"`
	Priority            domain.Priority   `json:"priority"`
	SpeedPref           domain.SpeedPref  `json:"speed_pref"`
	Preemption          domain.Preemption `json:"preemption"`
	WeightsMB           int               `json:"weights_mb"`
}

type calibrationSweepPlan struct {
	ID      string   `json:"id"`
	Kind    string   `json:"kind"`
	Models  []string `json:"models"`
	Through string   `json:"through"`
}

const calibrationReportSchemaVersion = "mycelium.calibration_report.v1"

type calibrationReportArtifact struct {
	SchemaVersion string                 `json:"schema_version"`
	CreatedAt     time.Time              `json:"created_at"`
	Plan          calibrationPlanSummary `json:"plan"`
	Run           *calibrationRunSummary `json:"run,omitempty"`
	Readiness     string                 `json:"readiness"`
	Refusals      []string               `json:"refusals,omitempty"`
	Next          []string               `json:"next,omitempty"`
}

type calibrationPlanSummary struct {
	Profile        string   `json:"profile"`
	Project        string   `json:"project"`
	ConfigPath     string   `json:"config_path"`
	DBPath         string   `json:"db_path"`
	Models         int      `json:"models"`
	Nodes          int      `json:"nodes"`
	Sweeps         int      `json:"sweeps"`
	PreflightPass  bool     `json:"preflight_passed"`
	Proofs         []string `json:"proofs,omitempty"`
	RunCommand     []string `json:"run_command,omitempty"`
	LiveRunCommand []string `json:"live_run_command,omitempty"`
}

type calibrationRunSummary struct {
	RunID         string    `json:"run_id"`
	Profile       string    `json:"profile"`
	Simulated     bool      `json:"simulated"`
	OutputDir     string    `json:"output_dir"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	PreflightPass bool      `json:"preflight_passed"`
	Proofs        int       `json:"proofs"`
	Results       int       `json:"results"`
	Failures      int       `json:"failures"`
	Events        int       `json:"events"`
	MetricRows    int       `json:"metric_rows"`
}

func runCalibrate(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce calibrate <plan|run|status|report>")
	}
	switch args[0] {
	case "plan":
		return runCalibratePlan(ctx, args[1:])
	case "run":
		return runCalibrateRun(ctx, args[1:], client)
	case "status":
		return runCalibrateStatus(args[1:])
	case "report":
		return runCalibrateReport(args[1:])
	default:
		return fmt.Errorf("usage: myce calibrate <plan|run|status|report>")
	}
}

func runCalibratePlan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("calibrate plan", flag.ContinueOnError)
	configPath := fs.String("config", defaultPeerConfigPath(), "peer config JSON")
	dbPath := fs.String("db", "", "control-plane SQLite store")
	outDir := fs.String("out", "", "output directory for calibration plan artifacts")
	profile := fs.String("profile", bench.FleetProfileConservative, "fleet benchmark profile")
	project := fs.String("project", "", "project label for synthetic calibration jobs")
	maxModels := fs.Int("max-models", 2, "maximum presets to include in the calibration plan")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *outDir == "" {
		return fmt.Errorf("--out is required")
	}
	if *maxModels <= 0 {
		return fmt.Errorf("--max-models must be positive")
	}
	cfg, artifact, err := buildCalibrationPlan(ctx, *configPath, *dbPath, *profile, *project, *maxModels, *outDir)
	if err != nil {
		return err
	}
	if err := writeCalibrationPlanArtifacts(*outDir, cfg, artifact); err != nil {
		return err
	}
	if *jsonOut {
		return writePrettyJSON(os.Stdout, artifact)
	}
	printCalibrationPlan(os.Stdout, artifact)
	return nil
}

func runCalibrateRun(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("calibrate run", flag.ContinueOnError)
	planDir := fs.String("plan", "", "directory created by myce calibrate plan")
	outDir := fs.String("out", "", "output directory for calibration run artifacts")
	simulate := fs.Bool("simulate", false, "run deterministic scheduler simulation only")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *planDir == "" {
		return fmt.Errorf("--plan is required")
	}
	if *outDir == "" {
		return fmt.Errorf("--out is required")
	}
	if !*simulate {
		return fmt.Errorf("live calibration run is not implemented; use --simulate for repo-local proof")
	}
	cfg, err := bench.LoadFleetConfig(filepath.Join(*planDir, "benchmark-config.json"))
	if err != nil {
		return err
	}
	result, err := bench.RunFleet(ctx, cfg, bench.FleetRunOptions{
		Profile:    bench.FleetProfileConservative,
		Simulate:   true,
		OutputRoot: *outDir,
		Client:     client,
		Clock:      clock.System{},
	})
	if err != nil {
		fmt.Printf("calibrate-run\t%s\tfailed\t%s\n", result.RunID, result.OutputDir)
		return err
	}
	fmt.Printf("calibrate-run\t%s\tsimulated\t%s\tproofs=%d\n", result.RunID, result.OutputDir, len(result.Preflight.Proofs))
	return nil
}

func runCalibrateStatus(args []string) error {
	fs := flag.NewFlagSet("calibrate status", flag.ContinueOnError)
	planDir := fs.String("plan", "", "directory created by myce calibrate plan")
	runDir := fs.String("run", "", "directory created by myce calibrate run, or its parent")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *planDir == "" {
		return fmt.Errorf("--plan is required")
	}
	report, err := buildCalibrationReport(*planDir, *runDir)
	if err != nil {
		return err
	}
	if *jsonOut {
		return writePrettyJSON(os.Stdout, report)
	}
	printCalibrationStatus(os.Stdout, report)
	return nil
}

func runCalibrateReport(args []string) error {
	fs := flag.NewFlagSet("calibrate report", flag.ContinueOnError)
	planDir := fs.String("plan", "", "directory created by myce calibrate plan")
	runDir := fs.String("run", "", "directory created by myce calibrate run, or its parent")
	outPath := fs.String("out", "", "output JSON report path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *planDir == "" {
		return fmt.Errorf("--plan is required")
	}
	if *outPath == "" {
		return fmt.Errorf("--out is required")
	}
	report, err := buildCalibrationReport(*planDir, *runDir)
	if err != nil {
		return err
	}
	if err := writePrivateJSON(*outPath, report); err != nil {
		return err
	}
	fmt.Printf("calibrate-report\t%s\treadiness=%s\n", *outPath, report.Readiness)
	return nil
}

func buildCalibrationPlan(ctx context.Context, configPath, dbPath, profile, project string, maxModels int, outDir string) (bench.FleetBenchmarkConfig, calibrationPlanArtifact, error) {
	loaded, err := loadOperatorConfig(configPath)
	if err != nil {
		return bench.FleetBenchmarkConfig{}, calibrationPlanArtifact{}, err
	}
	cfg := loaded.Config
	if dbPath == "" {
		dbPath = cfg.StorePath
	}
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		return bench.FleetBenchmarkConfig{}, calibrationPlanArtifact{}, err
	}
	defer store.Close()
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		return bench.FleetBenchmarkConfig{}, calibrationPlanArtifact{}, err
	}
	presets, err := store.ListPresets(ctx)
	if err != nil {
		return bench.FleetBenchmarkConfig{}, calibrationPlanArtifact{}, err
	}
	instances, err := store.ListInstances(ctx)
	if err != nil {
		return bench.FleetBenchmarkConfig{}, calibrationPlanArtifact{}, err
	}
	projects, err := store.ListProjects(ctx)
	if err != nil {
		return bench.FleetBenchmarkConfig{}, calibrationPlanArtifact{}, err
	}
	if len(presets) == 0 {
		presets = append([]domain.Preset(nil), cfg.Presets...)
	}
	if project == "" {
		project = calibrationProject(projects, cfg.Projects)
	}
	benchCfg, err := calibrationBenchmarkConfig(cfg, nodes, presets, instances, profile, project, maxModels)
	if err != nil {
		return bench.FleetBenchmarkConfig{}, calibrationPlanArtifact{}, err
	}
	preflight, err := bench.SimulateFleet(ctx, benchCfg, profile, clock.System{})
	if err != nil {
		return bench.FleetBenchmarkConfig{}, calibrationPlanArtifact{}, fmt.Errorf("calibration plan simulation failed: %w", err)
	}
	benchmarkConfigPath := filepath.Join(outDir, "benchmark-config.json")
	artifact := calibrationPlanArtifact{
		SchemaVersion:       calibrationPlanSchemaVersion,
		CreatedAt:           time.Now().UTC(),
		Profile:             profile,
		Project:             benchCfg.Project,
		ConfigPath:          loaded.Path,
		DBPath:              dbPath,
		BenchmarkConfigPath: benchmarkConfigPath,
		Nodes:               calibrationNodePlans(nodes),
		Models:              calibrationModelPlans(benchCfg.Models, presets),
		Sweeps:              calibrationSweeps(benchCfg),
		Safety: []string{
			"simulation_first",
			"no_token_values_in_plan",
			"normal_scheduler_owner_admission_path",
			"catastrophic_hosts_keep_existing_safety_caps",
		},
		Preflight: preflight,
		RunCommand: []string{
			"myce", "calibrate", "run",
			"--plan", outDir,
			"--simulate",
			"--out", filepath.Join(outDir, "runs"),
		},
		LiveRunCommand: []string{
			"myce", "benchmark", "fleet",
			"--config", filepath.Join(outDir, "benchmark-config.json"),
			"--profile", profile,
			"--out", filepath.Join(outDir, "live-runs"),
		},
		Notes: []string{
			"live calibration remains explicit; this plan does not mutate routing policy or start engines",
		},
	}
	return benchCfg, artifact, nil
}

func calibrationBenchmarkConfig(cfg operatorPeerConfig, nodes []domain.Node, presets []domain.Preset, instances []domain.ModelInstance, profile, project string, maxModels int) (bench.FleetBenchmarkConfig, error) {
	if profile == "" {
		profile = bench.FleetProfileConservative
	}
	gatewayURL := adminBaseURL(cfg.Listen)
	if gatewayURL == "" {
		return bench.FleetBenchmarkConfig{}, fmt.Errorf("peer config listen address is required for calibration gateway")
	}
	readyNodes := calibrationReadyNodes(nodes)
	if len(readyNodes) < 2 && profile == bench.FleetProfileConservative {
		return bench.FleetBenchmarkConfig{}, fmt.Errorf("conservative calibration requires at least two ready nodes for remote/multi-node proof")
	}
	eligible := calibrationEligiblePresets(presets)
	if len(eligible) < 2 && profile == bench.FleetProfileConservative {
		return bench.FleetBenchmarkConfig{}, fmt.Errorf("conservative calibration requires at least two eligible chat presets for warm and pressure proof")
	}
	if maxModels > len(eligible) {
		maxModels = len(eligible)
	}
	selected := calibrationSelectPresets(eligible, maxModels)
	simulationPresets := calibrationSimulationPresets(selected, eligible, instances)
	if project == "" {
		project = "calibration"
	}
	models := make([]bench.FleetModel, 0, len(selected))
	for i, preset := range selected {
		model := bench.FleetModel{
			ID:                  preset.ID,
			RequestModel:        preferredPresetModel(preset),
			PresetID:            preset.ID,
			PromptID:            "short",
			MaxTokens:           32,
			ContextRequest:      preset.ContextLength,
			ExpectedConcurrency: 1,
			Priority:            domain.PriorityBackground,
			SpeedPref:           domain.SpeedThroughput,
			Preemption:          domain.PreemptSoft,
		}
		if i == len(selected)-1 && len(selected) > 1 {
			model.Priority = domain.PriorityInteractive
			model.Preemption = domain.PreemptHardForInteractive
		}
		models = append(models, model)
	}
	gatewayID := firstNonEmpty(cfg.ID, "gateway")
	waves := []bench.FleetWave{
		{ID: "calibration-cold", Jobs: []bench.FleetWaveJob{{ModelID: models[0].ID, GatewayID: gatewayID}}},
		{ID: "calibration-warm", Jobs: []bench.FleetWaveJob{{ModelID: models[0].ID, GatewayID: gatewayID}}},
	}
	if len(models) > 1 {
		waves = append(waves, bench.FleetWave{ID: "calibration-pressure", Jobs: []bench.FleetWaveJob{{ModelID: models[len(models)-1].ID, GatewayID: gatewayID}}})
	}
	return bench.FleetBenchmarkConfig{
		ID:                           "calibration-" + time.Now().UTC().Format("20060102T150405Z"),
		Project:                      project,
		TrustedControlHeaderTestMode: true,
		Gateways: []bench.FleetGateway{{
			ID:     gatewayID,
			URL:    gatewayURL,
			NodeID: cfg.ID,
		}},
		Peers:   calibrationPeers(readyNodes),
		Prompts: []bench.FleetPrompt{{ID: "short", Text: "Respond with one short sentence."}},
		Models:  models,
		Waves:   waves,
		Simulation: bench.FleetSimulationConfig{
			Nodes:     readyNodes,
			Presets:   simulationPresets,
			Instances: append([]domain.ModelInstance(nil), instances...),
		},
		Safety: bench.FleetBenchmarkSafety{
			MinDiskFreeRatio:      domain.DefaultDiskMinFreeRatio,
			MaxSparkGPUMemoryUtil: 0.85,
		},
		Metadata: map[string]string{
			"generated_by": "myce calibrate plan",
			"purpose":      "auto_calibration",
		},
	}, nil
}

func calibrationSimulationPresets(selected, eligible []domain.Preset, instances []domain.ModelInstance) []domain.Preset {
	byID := map[string]domain.Preset{}
	for _, preset := range eligible {
		byID[preset.ID] = preset
	}
	outByID := map[string]domain.Preset{}
	for _, preset := range selected {
		outByID[preset.ID] = preset
	}
	for _, inst := range instances {
		if preset, ok := byID[inst.PresetID]; ok {
			outByID[preset.ID] = preset
		}
	}
	out := make([]domain.Preset, 0, len(outByID))
	for _, preset := range outByID {
		out = append(out, preset)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func calibrationProject(storeProjects, configProjects []domain.Project) string {
	projects := append([]domain.Project(nil), storeProjects...)
	if len(projects) == 0 {
		projects = append(projects, configProjects...)
	}
	sort.Slice(projects, func(i, j int) bool { return projects[i].ID < projects[j].ID })
	for _, project := range projects {
		if project.ID != "" {
			return project.ID
		}
	}
	return "calibration"
}

func calibrationReadyNodes(nodes []domain.Node) []domain.Node {
	out := append([]domain.Node(nil), nodes...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	ready := out[:0]
	for _, node := range out {
		if node.Status == "" {
			node.Status = domain.NodeReady
		}
		if node.DiskMinFreeRatio == 0 {
			node.DiskMinFreeRatio = domain.DefaultDiskMinFreeRatio
		}
		if node.Status == domain.NodeReady {
			ready = append(ready, node)
		}
	}
	return ready
}

func calibrationEligiblePresets(presets []domain.Preset) []domain.Preset {
	out := append([]domain.Preset(nil), presets...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].EstWeightsMB == out[j].EstWeightsMB {
			return out[i].ID < out[j].ID
		}
		return out[i].EstWeightsMB < out[j].EstWeightsMB
	})
	eligible := out[:0]
	for _, preset := range out {
		if preset.ID == "" || preset.Backend == "" || preset.ContextLength <= 0 || preset.EstWeightsMB <= 0 || preset.KVPerTokenMB < 0 {
			continue
		}
		if len(preset.Capabilities) > 0 && !containsCapability(preset.Capabilities, domain.CapabilityChat) {
			continue
		}
		eligible = append(eligible, preset)
	}
	return eligible
}

func calibrationSelectPresets(presets []domain.Preset, maxModels int) []domain.Preset {
	if len(presets) <= maxModels {
		return append([]domain.Preset(nil), presets...)
	}
	selected := []domain.Preset{presets[0]}
	if maxModels > 1 {
		selected = append(selected, presets[len(presets)-1])
	}
	for i := 1; len(selected) < maxModels && i < len(presets)-1; i++ {
		selected = append(selected, presets[i])
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].EstWeightsMB == selected[j].EstWeightsMB {
			return selected[i].ID < selected[j].ID
		}
		return selected[i].EstWeightsMB < selected[j].EstWeightsMB
	})
	return selected
}

func containsCapability(values []domain.Capability, want domain.Capability) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func calibrationPeers(nodes []domain.Node) []bench.FleetPeer {
	var peers []bench.FleetPeer
	for _, node := range nodes {
		if node.Address == "" {
			continue
		}
		peers = append(peers, bench.FleetPeer{ID: node.ID, URL: adminBaseURL(node.Address)})
	}
	return peers
}

func calibrationNodePlans(nodes []domain.Node) []calibrationNodePlan {
	ready := calibrationReadyNodes(nodes)
	out := make([]calibrationNodePlan, 0, len(ready))
	for _, node := range ready {
		totalVRAM := 0
		for _, acc := range node.Accelerators {
			totalVRAM += acc.VRAMTotalMB
		}
		out = append(out, calibrationNodePlan{
			ID:             node.ID,
			Status:         node.Status,
			OOMSeverity:    node.OOMSeverity,
			MaxUtil:        node.MaxUtil,
			Accelerators:   len(node.Accelerators),
			TotalVRAMMB:    totalVRAM,
			DiskFreeMB:     node.DiskFreeMB,
			DiskTotalMB:    node.DiskTotalMB,
			SpeedClassTPS:  node.SpeedClass.TokensPerSecRef,
			HasPeerAddress: node.Address != "",
		})
	}
	return out
}

func calibrationModelPlans(models []bench.FleetModel, presets []domain.Preset) []calibrationModelPlan {
	presetByID := map[string]domain.Preset{}
	for _, preset := range presets {
		presetByID[preset.ID] = preset
	}
	out := make([]calibrationModelPlan, 0, len(models))
	for _, model := range models {
		preset := presetByID[model.PresetID]
		out = append(out, calibrationModelPlan{
			ID:                  model.ID,
			RequestModel:        model.RequestModel,
			PresetID:            model.PresetID,
			Backend:             preset.Backend,
			ContextLength:       preset.ContextLength,
			ContextRequest:      model.ContextRequest,
			ExpectedConcurrency: model.ExpectedConcurrency,
			Priority:            model.Priority,
			SpeedPref:           model.SpeedPref,
			Preemption:          model.Preemption,
			WeightsMB:           preset.EstWeightsMB,
		})
	}
	return out
}

func calibrationSweeps(cfg bench.FleetBenchmarkConfig) []calibrationSweepPlan {
	var sweeps []calibrationSweepPlan
	for _, wave := range cfg.Waves {
		models := make([]string, 0, len(wave.Jobs))
		for _, job := range wave.Jobs {
			models = append(models, job.ModelID)
		}
		sweeps = append(sweeps, calibrationSweepPlan{
			ID:      wave.ID,
			Kind:    strings.TrimPrefix(wave.ID, "calibration-"),
			Models:  models,
			Through: "benchmark_fleet_simulation",
		})
	}
	return sweeps
}

func writeCalibrationPlanArtifacts(outDir string, cfg bench.FleetBenchmarkConfig, artifact calibrationPlanArtifact) error {
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		return fmt.Errorf("create calibration plan dir: %w", err)
	}
	if err := writePrivateJSON(filepath.Join(outDir, "benchmark-config.json"), cfg); err != nil {
		return err
	}
	return writePrivateJSON(filepath.Join(outDir, "calibration-plan.json"), artifact)
}

func loadCalibrationPlanArtifact(planDir string) (calibrationPlanArtifact, error) {
	var artifact calibrationPlanArtifact
	data, err := os.ReadFile(filepath.Join(planDir, "calibration-plan.json"))
	if err != nil {
		return artifact, err
	}
	if err := json.Unmarshal(data, &artifact); err != nil {
		return artifact, err
	}
	if artifact.SchemaVersion != calibrationPlanSchemaVersion {
		return artifact, fmt.Errorf("unsupported calibration plan schema %q", artifact.SchemaVersion)
	}
	return artifact, nil
}

func loadCalibrationRunResult(path string) (bench.FleetRunResult, error) {
	var result bench.FleetRunResult
	if path == "" {
		return result, nil
	}
	resultPath, err := resolveCalibrationRunResults(path)
	if err != nil {
		return result, err
	}
	data, err := os.ReadFile(resultPath)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	return result, nil
}

func resolveCalibrationRunResults(path string) (string, error) {
	direct := filepath.Join(path, "results.json")
	if _, err := os.Stat(direct); err == nil {
		return direct, nil
	}
	matches, err := filepath.Glob(filepath.Join(path, "calibration-*", "results.json"))
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no calibration results.json found under %s", path)
	}
	sort.Strings(matches)
	return matches[len(matches)-1], nil
}

func buildCalibrationReport(planDir, runDir string) (calibrationReportArtifact, error) {
	plan, err := loadCalibrationPlanArtifact(planDir)
	if err != nil {
		return calibrationReportArtifact{}, err
	}
	var run *calibrationRunSummary
	var runResult bench.FleetRunResult
	if runDir != "" {
		runResult, err = loadCalibrationRunResult(runDir)
		if err != nil {
			return calibrationReportArtifact{}, err
		}
		summary := calibrationRunSummary{
			RunID:         runResult.RunID,
			Profile:       runResult.Profile,
			Simulated:     runResult.Simulated,
			OutputDir:     runResult.OutputDir,
			StartedAt:     runResult.Manifest.StartedAt,
			FinishedAt:    runResult.Manifest.FinishedAt,
			PreflightPass: runResult.Preflight.Passed,
			Proofs:        len(runResult.Preflight.Proofs),
			Results:       len(runResult.Results),
			Failures:      len(runResult.Failures),
			Events:        len(runResult.Events),
			MetricRows:    len(runResult.Metrics),
		}
		run = &summary
	}
	report := calibrationReportArtifact{
		SchemaVersion: calibrationReportSchemaVersion,
		CreatedAt:     time.Now().UTC(),
		Plan: calibrationPlanSummary{
			Profile:        plan.Profile,
			Project:        plan.Project,
			ConfigPath:     plan.ConfigPath,
			DBPath:         plan.DBPath,
			Models:         len(plan.Models),
			Nodes:          len(plan.Nodes),
			Sweeps:         len(plan.Sweeps),
			PreflightPass:  plan.Preflight.Passed,
			Proofs:         append([]string(nil), plan.Preflight.Proofs...),
			RunCommand:     append([]string(nil), plan.RunCommand...),
			LiveRunCommand: append([]string(nil), plan.LiveRunCommand...),
		},
		Run: run,
	}
	report.Readiness, report.Refusals, report.Next = classifyCalibrationReport(report)
	return report, nil
}

func classifyCalibrationReport(report calibrationReportArtifact) (string, []string, []string) {
	if !report.Plan.PreflightPass {
		return "plan_failed", []string{"calibration plan preflight did not pass"}, []string{"fix inventory or generated benchmark config before running calibration"}
	}
	if report.Run == nil {
		return "plan_ready", []string{"no calibration run artifact supplied"}, []string{"run myce calibrate run --simulate before treating the plan as proven"}
	}
	if !report.Run.PreflightPass || report.Run.Failures > 0 {
		return "run_failed", []string{"calibration run artifact contains failures"}, []string{"inspect the run results.json and benchmark report"}
	}
	if report.Run.Simulated {
		next := []string{"live calibration remains an explicit operator-approved gate"}
		if len(report.Plan.LiveRunCommand) > 0 {
			next = append(next, strings.Join(report.Plan.LiveRunCommand, " "))
		}
		return "simulation_proven", []string{"live calibration has not run and no optimizer defaults were applied"}, next
	}
	return "live_evidence_present", nil, []string{"review live run safety before applying optimizer defaults"}
}

func writePrivateJSON(path string, value any) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
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

func printCalibrationPlan(w interface{ Write([]byte) (int, error) }, artifact calibrationPlanArtifact) {
	fmt.Fprintf(w, "calibrate-plan\tprofile=%s\tproject=%s\tconfig=%s\tmodels=%d\tnodes=%d\tproofs=%d\n",
		artifact.Profile,
		artifact.Project,
		artifact.BenchmarkConfigPath,
		len(artifact.Models),
		len(artifact.Nodes),
		len(artifact.Preflight.Proofs),
	)
	for _, proof := range artifact.Preflight.Proofs {
		fmt.Fprintf(w, "calibrate-proof\t%s\n", proof)
	}
	fmt.Fprintf(w, "calibrate-next\t%s\n", strings.Join(artifact.RunCommand, " "))
	if len(artifact.LiveRunCommand) > 0 {
		fmt.Fprintf(w, "calibrate-live-next\t%s\n", strings.Join(artifact.LiveRunCommand, " "))
	}
}

func printCalibrationStatus(w interface{ Write([]byte) (int, error) }, report calibrationReportArtifact) {
	fmt.Fprintf(w, "calibrate-status\treadiness=%s\tprofile=%s\tproject=%s\tmodels=%d\tnodes=%d\tsweeps=%d\tplan_preflight=%t\n",
		report.Readiness,
		report.Plan.Profile,
		report.Plan.Project,
		report.Plan.Models,
		report.Plan.Nodes,
		report.Plan.Sweeps,
		report.Plan.PreflightPass,
	)
	if report.Run != nil {
		fmt.Fprintf(w, "calibrate-run-status\t%s\tsimulated=%t\tpreflight=%t\tfailures=%d\tresults=%d\tmetrics=%d\n",
			report.Run.RunID,
			report.Run.Simulated,
			report.Run.PreflightPass,
			report.Run.Failures,
			report.Run.Results,
			report.Run.MetricRows,
		)
	}
	for _, refusal := range report.Refusals {
		fmt.Fprintf(w, "calibrate-refusal\t%s\n", refusal)
	}
	for _, next := range report.Next {
		fmt.Fprintf(w, "calibrate-next\t%s\n", next)
	}
}
