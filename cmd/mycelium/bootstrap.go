package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	bootstrapplan "mycelium/internal/bootstrap"
	"mycelium/internal/catalog"
	"mycelium/internal/domain"
	"mycelium/internal/engine"
	"mycelium/internal/hardware"
	"mycelium/internal/membership"
	storesqlite "mycelium/internal/store/sqlite"
)

func runBootstrap(ctx context.Context, args []string) error {
	return runBootstrapWithServiceManager(ctx, args, nil, runtime.GOOS)
}

func runBootstrapWithServiceManager(ctx context.Context, args []string, manager serviceManager, goos string) error {
	fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	joinRaw := fs.String("join", "", "join URI")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	compute := fs.String("compute", "auto", "compute mode: auto, on, off")
	configPath := fs.String("config", "", "peer config JSON path")
	apply := fs.Bool("apply", false, "write config and state")
	installService := fs.Bool("install-service", false, "install and start durable service after apply")
	doctor := fs.Bool("doctor", false, "report host and installed engine readiness")
	jsonOut := fs.Bool("json", false, "write doctor output as JSON")
	savePlan := fs.Bool("save-plan", false, "save doctor plan and engine readiness facts to the control store")
	enginesRaw := fs.String("engines", "auto", "engine list: auto, or comma-separated backends")
	allowCPUFallback := fs.Bool("allow-cpu-fallback", false, "allow explicit CPU-only engine planning")
	var modelRoots stringListFlag
	fs.Var(&modelRoots, "model-root", "local model root to include in doctor compatibility output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *doctor {
		engines, err := parseBootstrapEngines(*enginesRaw)
		if err != nil {
			return err
		}
		return runBootstrapDoctorWithOptions(ctx, bootstrapDoctorOptions{
			ConfigPath:       *configPath,
			JSON:             *jsonOut,
			SavePlan:         *savePlan,
			RequestedEngines: engines,
			AllowCPUFallback: *allowCPUFallback,
			ModelRoots:       []string(modelRoots),
		})
	}
	if *joinRaw == "" {
		return fmt.Errorf("--join is required")
	}
	join, err := parseJoinFlag(*joinRaw)
	if err != nil {
		return err
	}
	if *rpcToken == "" {
		return fmt.Errorf("--rpc-token is required for bootstrap")
	}
	path := *configPath
	if path == "" {
		path = defaultPeerConfigPath()
	}
	detector := hardware.NewDetector()
	cfg, err := generatePeerConfig(ctx, configInitOptions{
		Path:      path,
		Compute:   *compute,
		Listen:    "lan",
		Backend:   "auto",
		Detect:    detector.Detect,
		RandomHex: randomHex,
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
	})
	if err != nil {
		return err
	}
	cfg.JoinToken = join.Token
	cfg.RPCToken = *rpcToken
	cfg.SeedPeers = appendSeedPeer(cfg.SeedPeers, join.Address)
	if err := adoptBootstrapEngine(ctx, &cfg, *compute); err != nil {
		return err
	}
	if !*apply {
		fmt.Printf("bootstrap\tplan\tconfig=%s\tcompute=%t\tseeds=%d\n", path, cfg.Compute, len(cfg.SeedPeers))
		return nil
	}
	if err := savePeerConfig(path, cfg); err != nil {
		return err
	}
	store, err := storesqlite.Open(cfg.StorePath)
	if err != nil {
		return err
	}
	defer store.Close()
	if _, err := membership.NewPersistentTokenManager(ctx, cfg.JoinToken, store); err != nil {
		return err
	}
	fmt.Printf("bootstrap\tapplied\t%s\n", path)
	if *installService {
		if err := runServiceWithManager(ctx, []string{"install", "--config", path}, manager, goos); err != nil {
			return err
		}
	}
	return nil
}

type bootstrapDoctorReport struct {
	Host    domain.HostFacts       `json:"host"`
	Engines []domain.EngineProfile `json:"engines"`
	Plan    domain.BootstrapPlan   `json:"plan"`
}

type bootstrapDoctorOptions struct {
	ConfigPath       string
	JSON             bool
	SavePlan         bool
	RequestedEngines []domain.Backend
	AllowCPUFallback bool
	ModelRoots       []string
	ModelCandidates  []domain.BootstrapModelCandidate
}

func runBootstrapDoctor(ctx context.Context, configPath string, jsonOut bool) error {
	return runBootstrapDoctorWithOptions(ctx, bootstrapDoctorOptions{ConfigPath: configPath, JSON: jsonOut})
}

func runBootstrapDoctorWithOptions(ctx context.Context, opts bootstrapDoctorOptions) error {
	cfg, err := bootstrapDoctorConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	return runBootstrapDoctorWithDetectorOptions(ctx, cfg, opts, engine.NewDetector())
}

func runBootstrapDoctorWithDetector(ctx context.Context, cfg PeerConfig, jsonOut bool, detector bootstrapEngineDetector) error {
	return runBootstrapDoctorWithDetectorOptions(ctx, cfg, bootstrapDoctorOptions{JSON: jsonOut}, detector)
}

func runBootstrapDoctorWithDetectorOptions(ctx context.Context, cfg PeerConfig, opts bootstrapDoctorOptions, detector bootstrapEngineDetector) error {
	report, err := detectBootstrapEnginesWithDetectorOptions(ctx, cfg, opts, detector)
	if err != nil {
		return err
	}
	if opts.SavePlan {
		if err := saveBootstrapDoctorReport(ctx, cfg, report); err != nil {
			return err
		}
	}
	if opts.JSON {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	fmt.Printf("host\t%s\t%s\t%s\t%s\n", report.Host.NodeID, report.Host.Platform, report.Host.OOMSeverity, diskSummary(report.Host))
	for _, profile := range report.Plan.ResultingProfiles {
		fmt.Printf("engine\t%s\t%s\t%t\t%s\t%s\n", profile.Backend, profile.BinaryPath, profile.Ready, profile.Version, profile.UnreadyReason)
	}
	for _, action := range report.Plan.Actions {
		fmt.Printf("action\t%s\t%s\t%s\t%s\n", action.ID, action.Kind, action.EngineProfileID, strings.Join(action.CommandPreview, " "))
	}
	for _, incompatibility := range report.Plan.Incompatibilities {
		fmt.Printf("blocked\t%s\t%s\t%s\n", incompatibility.Backend, incompatibility.Model, incompatibility.Reason)
	}
	for _, model := range report.Plan.ModelCandidates {
		for _, backend := range model.Backends {
			fmt.Printf("model\t%s\t%s\t%s\t%s\t%t\t%s\n", model.Name, model.Format, model.Path, backend.Backend, backend.Ready, backend.Reason)
		}
	}
	return nil
}

func saveBootstrapDoctorReport(ctx context.Context, cfg PeerConfig, report bootstrapDoctorReport) error {
	store, err := storesqlite.Open(cfg.StorePath)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.SaveBootstrapPlan(ctx, report.Plan); err != nil {
		return err
	}
	for _, profile := range report.Plan.ResultingProfiles {
		if err := store.SaveEngineProfile(ctx, profile); err != nil {
			return err
		}
	}
	return nil
}

type bootstrapEngineDetector interface {
	DetectHost(context.Context, domain.Node) (domain.HostFacts, error)
	DetectEngines(context.Context, domain.HostFacts) ([]domain.EngineProfile, error)
	DetectConfiguredEngine(context.Context, domain.HostFacts, engine.Config) domain.EngineProfile
}

func bootstrapDoctorConfig(configPath string) (PeerConfig, error) {
	cfg, err := loadPeerConfig(configPath)
	if err == nil {
		return cfg, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return PeerConfig{}, err
	}
	return applyPeerConfigDefaults(PeerConfig{}), nil
}

func detectBootstrapEngines(ctx context.Context, cfg PeerConfig) (bootstrapDoctorReport, error) {
	return detectBootstrapEnginesWithDetector(ctx, cfg, engine.NewDetector())
}

func detectBootstrapEnginesWithDetector(ctx context.Context, cfg PeerConfig, detector bootstrapEngineDetector) (bootstrapDoctorReport, error) {
	return detectBootstrapEnginesWithDetectorOptions(ctx, cfg, bootstrapDoctorOptions{}, detector)
}

func detectBootstrapEnginesWithDetectorOptions(ctx context.Context, cfg PeerConfig, opts bootstrapDoctorOptions, detector bootstrapEngineDetector) (bootstrapDoctorReport, error) {
	host, err := detector.DetectHost(ctx, engineSeedNode(cfg))
	if err != nil {
		return bootstrapDoctorReport{}, err
	}
	profiles, err := detector.DetectEngines(ctx, host)
	if err != nil {
		return bootstrapDoctorReport{}, err
	}
	configured := detector.DetectConfiguredEngine(ctx, host, engineConfigFromCompute(cfg.ComputeConfig))
	profiles = mergeEngineProfiles(configured, profiles)
	models, err := discoverBootstrapModelCandidates(ctx, host, profiles, opts.ModelRoots)
	if err != nil {
		return bootstrapDoctorReport{}, err
	}
	models = append(models, opts.ModelCandidates...)
	plan, err := (bootstrapplan.Planner{}).PlanBootstrap(ctx, domain.BootstrapRequest{
		RequestedEngines: opts.RequestedEngines,
		AllowCPUFallback: opts.AllowCPUFallback,
		ModelCandidates:  models,
	}, host, profiles)
	if err != nil {
		return bootstrapDoctorReport{}, err
	}
	return bootstrapDoctorReport{Host: host, Engines: profiles, Plan: plan}, nil
}

func adoptBootstrapEngine(ctx context.Context, cfg *PeerConfig, computeMode string) error {
	return adoptBootstrapEngineWithDetector(ctx, cfg, computeMode, engine.NewDetector())
}

func adoptBootstrapEngineWithDetector(ctx context.Context, cfg *PeerConfig, computeMode string, detector bootstrapEngineDetector) error {
	if !cfg.Compute {
		return nil
	}
	report, err := detectBootstrapEnginesWithDetectorOptions(ctx, *cfg, bootstrapDoctorOptions{RequestedEngines: []domain.Backend{cfg.ComputeConfig.Backend}}, detector)
	if err != nil {
		if computeMode == "on" {
			return err
		}
		cfg.Compute = false
		return nil
	}
	cfg.EngineProfiles = append([]domain.EngineProfile(nil), report.Plan.ResultingProfiles...)
	chosen, ok := chooseReadyEngine(report.Plan.ResultingProfiles, cfg.ComputeConfig.Backend)
	if !ok {
		if computeMode == "on" {
			return fmt.Errorf("no ready engine profile for backend %s", cfg.ComputeConfig.Backend)
		}
		cfg.Compute = false
		return nil
	}
	cfg.ComputeConfig.Backend = chosen.Backend
	cfg.ComputeConfig.BackendBinary = chosen.BinaryPath
	if len(chosen.Args) > 0 {
		cfg.ComputeConfig.CustomArgs = append([]string(nil), chosen.Args...)
	}
	if chosen.HealthPath != "" {
		cfg.ComputeConfig.HealthPath = chosen.HealthPath
	}
	return nil
}

func parseBootstrapEngines(raw string) ([]domain.Backend, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "auto" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	engines := make([]domain.Backend, 0, len(parts))
	for _, part := range parts {
		backend := domain.Backend(strings.TrimSpace(part))
		switch backend {
		case domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendVLLM, domain.BackendOpenVINO, domain.BackendSGLang, domain.BackendCustom:
			engines = append(engines, backend)
		default:
			return nil, fmt.Errorf("unknown bootstrap engine %q", part)
		}
	}
	return engines, nil
}

func discoverBootstrapModelCandidates(ctx context.Context, host domain.HostFacts, profiles []domain.EngineProfile, roots []string) ([]domain.BootstrapModelCandidate, error) {
	if len(roots) == 0 {
		return nil, nil
	}
	candidates, err := catalog.DiscoverCandidates(ctx, catalog.DiscoveryRequest{HostID: host.NodeID, Roots: roots, Engines: profiles})
	if err != nil {
		return nil, err
	}
	out := make([]domain.BootstrapModelCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		backends := make([]domain.BootstrapBackendCompatibility, 0, len(candidate.Backends))
		for _, backend := range candidate.Backends {
			backends = append(backends, domain.BootstrapBackendCompatibility{Backend: backend.Backend, Ready: backend.Ready, Reason: backend.Reason})
		}
		out = append(out, domain.BootstrapModelCandidate{
			HostID:        candidate.HostID,
			Name:          candidate.Name,
			Path:          candidate.Path,
			Format:        candidate.Format,
			SizeMB:        candidate.SizeMB,
			ContextLength: candidate.ContextLength,
			Capabilities:  append([]domain.Capability(nil), candidate.Capabilities...),
			Metadata:      cloneStringMap(candidate.Metadata),
			Backends:      backends,
		})
	}
	return out, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type stringListFlag []string

func (f *stringListFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringListFlag) Set(value string) error {
	if value == "" {
		return fmt.Errorf("value is required")
	}
	*f = append(*f, value)
	return nil
}

func engineSeedNode(cfg PeerConfig) domain.Node {
	return domain.Node{
		ID:               cfg.ComputeConfig.ID,
		Name:             cfg.ComputeConfig.Name,
		Address:          cfg.Listen,
		MaxUtil:          cfg.ComputeConfig.MaxUtil,
		DiskTotalMB:      cfg.ComputeConfig.DiskTotalMB,
		DiskFreeMB:       cfg.ComputeConfig.DiskFreeMB,
		DiskMinFreeRatio: cfg.ComputeConfig.DiskMinFreeRatio,
		Status:           domain.NodeReady,
	}
}

func engineConfigFromCompute(cfg ComputeConfig) engine.Config {
	return engine.Config{
		Backend:          cfg.Backend,
		BackendBinary:    computeBackendBinary(cfg, engineDefaultBinary(cfg.Backend)),
		CustomArgs:       append([]string(nil), cfg.CustomArgs...),
		HealthPath:       cfg.HealthPath,
		MaxUtil:          cfg.MaxUtil,
		DiskMinFreeRatio: cfg.DiskMinFreeRatio,
	}
}

func engineDefaultBinary(backend domain.Backend) string {
	switch backend {
	case domain.BackendMLX:
		return "mlx_lm.server"
	case domain.BackendVLLM:
		return "vllm"
	case domain.BackendOpenVINO:
		return "openvino-genai-openai"
	case domain.BackendLlamaCpp:
		return "llama-server"
	default:
		return ""
	}
}

func mergeEngineProfiles(configured domain.EngineProfile, profiles []domain.EngineProfile) []domain.EngineProfile {
	out := []domain.EngineProfile{configured}
	for _, profile := range profiles {
		if profile.Backend == configured.Backend {
			continue
		}
		out = append(out, profile)
	}
	return out
}

func chooseReadyEngine(profiles []domain.EngineProfile, preferred domain.Backend) (domain.EngineProfile, bool) {
	for _, profile := range profiles {
		if profile.Ready && profile.Backend == preferred {
			return profile, true
		}
	}
	for _, profile := range profiles {
		if profile.Ready {
			return profile, true
		}
	}
	return domain.EngineProfile{}, false
}

func diskSummary(host domain.HostFacts) string {
	if host.DiskTotalMB == 0 {
		return "disk=unknown"
	}
	return fmt.Sprintf("disk_free=%dMB/%dMB", host.DiskFreeMB, host.DiskTotalMB)
}
