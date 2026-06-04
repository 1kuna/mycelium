package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"

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
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *doctor {
		return runBootstrapDoctor(ctx, *configPath, *jsonOut)
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
}

func runBootstrapDoctor(ctx context.Context, configPath string, jsonOut bool) error {
	cfg, err := bootstrapDoctorConfig(configPath)
	if err != nil {
		return err
	}
	return runBootstrapDoctorWithDetector(ctx, cfg, jsonOut, engine.NewDetector())
}

type bootstrapEngineDetector interface {
	DetectHost(context.Context, domain.Node) (domain.HostFacts, error)
	DetectEngines(context.Context, domain.HostFacts) ([]domain.EngineProfile, error)
	DetectConfiguredEngine(context.Context, domain.HostFacts, engine.Config) domain.EngineProfile
}

func runBootstrapDoctorWithDetector(ctx context.Context, cfg PeerConfig, jsonOut bool, detector bootstrapEngineDetector) error {
	report, err := detectBootstrapEnginesWithDetector(ctx, cfg, detector)
	if err != nil {
		return err
	}
	if jsonOut {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	fmt.Printf("host\t%s\t%s\t%s\t%s\n", report.Host.NodeID, report.Host.Platform, report.Host.OOMSeverity, diskSummary(report.Host))
	for _, profile := range report.Engines {
		fmt.Printf("engine\t%s\t%s\t%t\t%s\t%s\n", profile.Backend, profile.BinaryPath, profile.Ready, profile.Version, profile.UnreadyReason)
	}
	return nil
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
	return bootstrapDoctorReport{Host: host, Engines: profiles}, nil
}

func adoptBootstrapEngine(ctx context.Context, cfg *PeerConfig, computeMode string) error {
	return adoptBootstrapEngineWithDetector(ctx, cfg, computeMode, engine.NewDetector())
}

func adoptBootstrapEngineWithDetector(ctx context.Context, cfg *PeerConfig, computeMode string, detector bootstrapEngineDetector) error {
	if !cfg.Compute {
		return nil
	}
	report, err := detectBootstrapEnginesWithDetector(ctx, *cfg, detector)
	if err != nil {
		if computeMode == "on" {
			return err
		}
		cfg.Compute = false
		return nil
	}
	cfg.EngineProfiles = append([]domain.EngineProfile(nil), report.Engines...)
	chosen, ok := chooseReadyEngine(report.Engines, cfg.ComputeConfig.Backend)
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
