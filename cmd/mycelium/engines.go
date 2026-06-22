package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/engine"
	"mycelium/internal/enginecatalog"
	"mycelium/internal/enginecompat"
	storesqlite "mycelium/internal/store/sqlite"
)

func runEngines(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("engines subcommand is required: list, doctor, preflight, install-plan, apply, or reload")
	}
	switch args[0] {
	case "list":
		return runEnginesList(ctx, args[1:])
	case "doctor":
		return runEnginesDoctor(ctx, args[1:])
	case "preflight":
		return runEnginesPreflight(ctx, args[1:])
	case "install-plan":
		return runEnginesInstallPlan(ctx, args[1:])
	case "apply":
		return runEnginesApply(ctx, args[1:])
	case "reload":
		return runEnginesReload(ctx, args[1:])
	default:
		return fmt.Errorf("unknown engines subcommand %q", args[0])
	}
}

func runEnginesList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("engines list", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	dbPath := fs.String("db", "", "control store path")
	jsonOut := fs.Bool("json", false, "write JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := openEngineStore(*configPath, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	profiles, err := store.ListEngineProfiles(ctx)
	if err != nil {
		return err
	}
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].ID < profiles[j].ID })
	if *jsonOut {
		data, err := json.MarshalIndent(profiles, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	for _, profile := range profiles {
		fmt.Printf("engine\t%s\t%s\t%t\t%s\t%s\t%s\t%s\n",
			profile.ID,
			profile.Backend,
			profile.Ready,
			profile.ArtifactPlatform,
			profile.Version,
			strings.Join(profile.SupportedModels, ","),
			profile.UnreadyReason)
	}
	return nil
}

func runEnginesDoctor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("engines doctor", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	dbPath := fs.String("db", "", "control store path")
	peer := fs.String("peer", "local", "peer to inspect; only local is supported by this command")
	jsonOut := fs.Bool("json", false, "write JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peer != "local" {
		return fmt.Errorf("engines doctor --peer currently supports only local")
	}
	store, err := openEngineStore(*configPath, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	plans, err := store.ListBootstrapPlans(ctx)
	if err != nil {
		return err
	}
	if len(plans) == 0 {
		if *jsonOut {
			fmt.Println(`{"plans":[]}`)
			return nil
		}
		fmt.Println("engines\tdoctor\tno saved bootstrap plan; run mycelium bootstrap --doctor --save-plan")
		return nil
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].CreatedAt.After(plans[j].CreatedAt) })
	plan := plans[0]
	if *jsonOut {
		data, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	fmt.Printf("doctor\t%s\t%s\t%s\t%s\n", plan.ID, plan.Host.NodeID, plan.Host.Platform, plan.CreatedAt.Format("2006-01-02T15:04:05Z07:00"))
	for _, profile := range sortedProfiles(plan.ResultingProfiles) {
		fmt.Printf("engine\t%s\t%s\t%t\t%s\t%s\n", profile.ID, profile.Backend, profile.Ready, profile.ArtifactPlatform, profile.UnreadyReason)
	}
	for _, action := range plan.Actions {
		fmt.Printf("action\t%s\t%s\t%s\t%s\n", action.ID, action.Kind, action.EngineProfileID, strings.Join(action.CommandPreview, " "))
	}
	for _, incompatibility := range plan.Incompatibilities {
		fmt.Printf("blocked\t%s\t%s\t%s\n", incompatibility.Backend, incompatibility.Model, incompatibility.Reason)
	}
	for _, model := range plan.ModelCandidates {
		for _, backend := range model.Backends {
			fmt.Printf("model\t%s\t%s\t%s\t%s\t%t\t%s\n", model.Name, model.Format, model.Path, backend.Backend, backend.Ready, backend.Reason)
		}
	}
	return nil
}

func runEnginesPreflight(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("engines preflight", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	dbPath := fs.String("db", "", "control store path")
	backendRaw := fs.String("backend", "", "backend to preflight")
	model := fs.String("model", "", "model or preset name for display")
	modelFormat := fs.String("model-format", "", "model format to include in compatibility-key matching")
	hostPlatform := fs.String("host-platform", "", "host platform os/arch, for example linux/amd64")
	acceleratorVendor := fs.String("accelerator-vendor", "", "accelerator vendor to include in compatibility-key matching")
	acceleratorRuntime := fs.String("accelerator-runtime", "", "accelerator runtime to include in compatibility-key matching")
	driverVersion := fs.String("driver-version", "", "driver/runtime version to include in compatibility-key matching")
	strict := fs.Bool("strict", false, "fail if no saved ready engine profile exists")
	jsonOut := fs.Bool("json", false, "write JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	backend, err := normalizeBackend(*backendRaw)
	if err != nil {
		return err
	}
	if backend == "" {
		return fmt.Errorf("engines preflight --backend is required")
	}
	store, err := openEngineStore(*configPath, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	mode := domain.EngineReadinessLegacyAllow
	if *strict {
		mode = domain.EngineReadinessStrict
	}
	name := *model
	if name == "" {
		name = string(backend)
	}
	target, err := preflightCompatibilityKey(ctx, store, backend, *modelFormat, *hostPlatform, *acceleratorVendor, *acceleratorRuntime, *driverVersion)
	if err != nil {
		return err
	}
	checker := engine.NewReadinessCheckerForKey(store, mode, target)
	check, err := checker.CheckEngineReadiness(ctx, domain.Node{}, domain.Preset{ID: name, Backend: backend})
	if err != nil {
		return err
	}
	if *jsonOut {
		data, err := json.MarshalIndent(check, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	fmt.Printf("preflight\t%s\t%s\t%s\t%s\t%s\n", name, backend, check.Status, check.ProfileID, check.Reason)
	return nil
}

func runEnginesInstallPlan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("engines install-plan", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	dbPath := fs.String("db", "", "control store path")
	backendRaw := fs.String("backend", "", "backend to plan")
	hostRaw := fs.String("host", "", "saved host id, saved platform, or os/arch platform")
	acceleratorVendor := fs.String("accelerator-vendor", "", "accelerator vendor when --host is an os/arch platform")
	allowCPUFallback := fs.Bool("allow-cpu-fallback", false, "allow explicit CPU fallback plans")
	jsonOut := fs.Bool("json", false, "write JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	backend, err := normalizeBackend(*backendRaw)
	if err != nil {
		return err
	}
	if backend == "" {
		return fmt.Errorf("engines install-plan --backend is required")
	}
	store, err := openEngineStore(*configPath, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	host, err := installPlanHost(ctx, store, *hostRaw, *acceleratorVendor)
	if err != nil {
		return err
	}
	plan, err := enginecatalog.PlanInstall(enginecatalog.InstallPlanRequest{Backend: backend, Host: host, AllowCPUFallback: *allowCPUFallback})
	if err != nil {
		return err
	}
	if *jsonOut {
		data, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	printEngineInstallPlan(plan)
	return nil
}

func installPlanHost(ctx context.Context, store *storesqlite.Store, hostRaw, acceleratorVendor string) (domain.HostFacts, error) {
	hostRaw = strings.TrimSpace(hostRaw)
	plans, err := store.ListBootstrapPlans(ctx)
	if err != nil {
		return domain.HostFacts{}, err
	}
	if len(plans) > 0 {
		sort.Slice(plans, func(i, j int) bool { return plans[i].CreatedAt.After(plans[j].CreatedAt) })
		if hostRaw == "" || hostRaw == "latest" || hostRaw == "local" {
			return plans[0].Host, nil
		}
		for _, plan := range plans {
			if plan.ID == hostRaw || plan.Host.NodeID == hostRaw || plan.Host.Platform == hostRaw {
				return plan.Host, nil
			}
		}
	}
	if hostRaw == "" {
		return domain.HostFacts{}, fmt.Errorf("engines install-plan --host is required when no saved bootstrap plan exists")
	}
	osName, arch, err := splitHostPlatform(hostRaw)
	if err != nil {
		return domain.HostFacts{}, fmt.Errorf("unknown saved host %q and not an os/arch platform: %w", hostRaw, err)
	}
	host := domain.HostFacts{OS: osName, Arch: arch, Platform: hostRaw}
	if acceleratorVendor != "" {
		host.Accelerators = []domain.Accelerator{{Vendor: acceleratorVendor}}
	}
	return host, nil
}

func printEngineInstallPlan(plan enginecatalog.InstallPlan) {
	fmt.Printf("install-plan\t%s\t%s\t%s\t%s\t%s\tapproval=%t\tdry_run=%t\trollback=%s\t%s\n",
		plan.Backend,
		plan.EngineFamily,
		plan.HostID,
		plan.HostPlatform,
		plan.Status,
		plan.RequiresApproval,
		plan.DryRunOnly,
		plan.Rollback,
		plan.Reason)
	for _, action := range plan.Actions {
		fmt.Printf("action\t%s\t%s\tapproval=%t\tdry_run=%t\tmanual=%t\tplatform=%s\tpackage=%s\tpath=%s\tcommand=%s\t%s\n",
			action.ID,
			action.Kind,
			action.RequiresApproval,
			action.DryRunOnly,
			action.Manual,
			action.Platform,
			action.Package,
			action.ManagedPath,
			strings.Join(action.CommandPreview, " "),
			action.Reason)
	}
	for _, risk := range plan.Risks {
		fmt.Printf("risk\t%s\n", risk)
	}
	for _, note := range plan.Notes {
		fmt.Printf("note\t%s\n", note)
	}
}

type engineReloadReport struct {
	DryRun  bool                 `json:"dry_run"`
	Presets []domain.Preset      `json:"presets,omitempty"`
	Blocked []engineApplyBlocked `json:"blocked,omitempty"`
}

func runEnginesReload(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("engines reload", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	dbPath := fs.String("db", "", "control store path")
	dryRun := fs.Bool("dry-run", false, "preview refreshed runtime preset visibility without mutating a running peer")
	jsonOut := fs.Bool("json", false, "write JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*dryRun {
		return fmt.Errorf("dynamic engine registry reload is not live-enabled yet; run engines reload --dry-run to preview refreshed candidate visibility without restarting")
	}
	store, err := openEngineStore(*configPath, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	loaded, err := loadRuntimePresets(ctx, store)
	if err != nil {
		return err
	}
	report := engineReloadReport{DryRun: true, Presets: loaded.Presets, Blocked: loaded.Blocked}
	if *jsonOut {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	fmt.Printf("reload\tdry-run\tpresets=%d\tblocked=%d\n", len(report.Presets), len(report.Blocked))
	for _, preset := range report.Presets {
		fmt.Printf("preset\t%s\t%s\t%s\t%s\t%s\n", preset.ID, preset.Backend, preset.ModelRef, preset.EngineReadiness, preset.EngineReadinessReason)
	}
	for _, blocked := range report.Blocked {
		fmt.Printf("blocked\t%s\t%s\t%s\t%s\n", blocked.Kind, blocked.ID, blocked.Backend, blocked.Reason)
	}
	return nil
}

func preflightCompatibilityKey(ctx context.Context, store *storesqlite.Store, backend domain.Backend, modelFormat, hostPlatform, acceleratorVendor, acceleratorRuntime, driverVersion string) (domain.CompatibilityKey, error) {
	key := domain.CompatibilityKey{
		Backend:            backend,
		ModelFormat:        modelFormat,
		AcceleratorVendor:  acceleratorVendor,
		AcceleratorRuntime: acceleratorRuntime,
		DriverVersion:      driverVersion,
	}
	if hostPlatform != "" {
		osName, arch, err := splitHostPlatform(hostPlatform)
		if err != nil {
			return domain.CompatibilityKey{}, err
		}
		key.OS = osName
		key.CPUArch = arch
	}
	plan, ok, err := latestBootstrapPlan(ctx, store)
	if err != nil {
		return domain.CompatibilityKey{}, err
	}
	if !ok {
		return key, nil
	}
	for _, profile := range plan.ResultingProfiles {
		if profile.Backend == backend && profile.CompatibilityKey != (domain.CompatibilityKey{}) {
			seeded := profile.CompatibilityKey
			seeded.ModelFormat = modelFormat
			if key.OS != "" {
				seeded.OS = key.OS
			}
			if key.CPUArch != "" {
				seeded.CPUArch = key.CPUArch
			}
			if key.AcceleratorVendor != "" {
				seeded.AcceleratorVendor = key.AcceleratorVendor
			}
			if key.AcceleratorRuntime != "" {
				seeded.AcceleratorRuntime = key.AcceleratorRuntime
			}
			if key.DriverVersion != "" {
				seeded.DriverVersion = key.DriverVersion
			}
			return seeded, nil
		}
	}
	host := plan.Host
	osName, arch, err := splitHostPlatform(host.Platform)
	if err != nil && host.OS == "" && host.Arch == "" {
		return domain.CompatibilityKey{}, err
	}
	if key.OS == "" {
		key.OS = host.OS
		if key.OS == "" {
			key.OS = osName
		}
	}
	if key.CPUArch == "" {
		key.CPUArch = host.Arch
		if key.CPUArch == "" {
			key.CPUArch = arch
		}
	}
	if key.AcceleratorVendor == "" && len(host.Accelerators) == 1 {
		key.AcceleratorVendor = host.Accelerators[0].Vendor
	}
	if key.DriverVersion == "" {
		key.DriverVersion = host.DriverFacts[key.AcceleratorVendor+".driver"]
	}
	return key, nil
}

func splitHostPlatform(platform string) (string, string, error) {
	if platform == "" {
		return "", "", nil
	}
	parts := strings.SplitN(platform, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("host platform must be os/arch, got %q", platform)
	}
	return parts[0], parts[1], nil
}

type engineApplyOptions struct {
	ConfigPath        string
	DBPath            string
	PlanID            string
	Write             bool
	JSON              bool
	PostWriteValidate func(string) error
}

type engineApplyReport struct {
	PlanID           string                 `json:"plan_id"`
	ConfigPath       string                 `json:"config_path"`
	Write            bool                   `json:"write"`
	BackupPath       string                 `json:"backup_path,omitempty"`
	ReadyEngines     []domain.EngineProfile `json:"ready_engines,omitempty"`
	GeneratedPresets []domain.Preset        `json:"generated_presets,omitempty"`
	Blocked          []engineApplyBlocked   `json:"blocked,omitempty"`
	Verification     []engineApplyEvidence  `json:"verification,omitempty"`
}

type engineApplyBlocked struct {
	Kind    string         `json:"kind"`
	ID      string         `json:"id,omitempty"`
	Backend domain.Backend `json:"backend,omitempty"`
	Reason  string         `json:"reason"`
}

type engineApplyEvidence struct {
	Kind    string         `json:"kind"`
	ID      string         `json:"id,omitempty"`
	Backend domain.Backend `json:"backend,omitempty"`
	Status  string         `json:"status"`
	Reason  string         `json:"reason"`
}

func runEnginesApply(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("engines apply", flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	dbPath := fs.String("db", "", "control store path")
	planID := fs.String("plan", "latest", "saved bootstrap plan id, or latest")
	write := fs.Bool("write", false, "materialize generated profiles into peer config and control store")
	jsonOut := fs.Bool("json", false, "write JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return runEnginesApplyWithOptions(ctx, engineApplyOptions{
		ConfigPath: *configPath,
		DBPath:     *dbPath,
		PlanID:     *planID,
		Write:      *write,
		JSON:       *jsonOut,
	})
}

func runEnginesApplyWithOptions(ctx context.Context, opts engineApplyOptions) error {
	configPath := opts.ConfigPath
	if configPath == "" {
		configPath = defaultPeerConfigPath()
	}
	cfg, err := bootstrapDoctorConfig(configPath)
	if err != nil {
		return err
	}
	storePath := opts.DBPath
	if storePath == "" {
		storePath = cfg.StorePath
	}
	store, err := storesqlite.Open(storePath)
	if err != nil {
		return err
	}
	defer store.Close()
	plan, err := applyBootstrapPlan(ctx, store, opts.PlanID)
	if err != nil {
		return err
	}
	report, nextConfig, err := buildEngineApplyReport(ctx, store, cfg, configPath, plan, opts.Write)
	if err != nil {
		return err
	}
	if opts.Write {
		backup, err := savePeerConfigAtomic(configPath, nextConfig, backupStampForPlan(plan), opts.PostWriteValidate)
		if err != nil {
			return err
		}
		report.BackupPath = backup
		for _, preset := range report.GeneratedPresets {
			if err := store.SavePreset(ctx, preset); err != nil {
				return err
			}
		}
		report.Verification = append(report.Verification, verifyGeneratedPresetVisibility(ctx, store, report.GeneratedPresets)...)
	}
	if opts.JSON {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	printEngineApplyReport(report)
	return nil
}

func applyBootstrapPlan(ctx context.Context, store *storesqlite.Store, id string) (domain.BootstrapPlan, error) {
	if id != "" && id != "latest" {
		return store.BootstrapPlan(ctx, id)
	}
	plans, err := store.ListBootstrapPlans(ctx)
	if err != nil {
		return domain.BootstrapPlan{}, err
	}
	if len(plans) == 0 {
		return domain.BootstrapPlan{}, fmt.Errorf("no saved bootstrap plan; run mycelium bootstrap --doctor --save-plan")
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].CreatedAt.After(plans[j].CreatedAt) })
	return plans[0], nil
}

func buildEngineApplyReport(ctx context.Context, store *storesqlite.Store, cfg PeerConfig, configPath string, plan domain.BootstrapPlan, write bool) (engineApplyReport, PeerConfig, error) {
	savedProfiles, err := store.ListEngineProfiles(ctx)
	if err != nil {
		return engineApplyReport{}, PeerConfig{}, err
	}
	savedByID := engineProfilesByID(savedProfiles)
	report := engineApplyReport{PlanID: plan.ID, ConfigPath: configPath, Write: write}
	for _, planned := range sortedProfiles(plan.ResultingProfiles) {
		saved, ok := savedByID[planned.ID]
		if !ok {
			report.Blocked = append(report.Blocked, engineApplyBlocked{Kind: "engine", ID: planned.ID, Backend: planned.Backend, Reason: "engine profile is present in plan but not saved in registry"})
			continue
		}
		if !saved.Ready {
			reason := saved.UnreadyReason
			if reason == "" {
				reason = "saved engine profile is not ready"
			}
			report.Blocked = append(report.Blocked, engineApplyBlocked{Kind: "engine", ID: saved.ID, Backend: saved.Backend, Reason: reason})
			continue
		}
		if _, ok, reason := enginecompat.ProfileMatchesHost(saved, plan.Host, ""); !ok {
			report.Blocked = append(report.Blocked, engineApplyBlocked{Kind: "engine", ID: saved.ID, Backend: saved.Backend, Reason: reason})
			continue
		}
		report.ReadyEngines = append(report.ReadyEngines, markGeneratedEngineProfile(saved))
	}
	if len(report.ReadyEngines) == 0 {
		return report, cfg, fmt.Errorf("no saved ready engine profiles in bootstrap plan %q", plan.ID)
	}
	nextConfig := materializeEngineProfilesIntoConfig(cfg, report.ReadyEngines)
	existingPresets, err := store.ListPresets(ctx)
	if err != nil {
		return engineApplyReport{}, PeerConfig{}, err
	}
	generated, blocked := generatedPresetsFromPlan(plan, report.ReadyEngines, existingPresets)
	report.GeneratedPresets = generated
	report.Blocked = append(report.Blocked, blocked...)
	nextConfig.Presets = mergeGeneratedPresets(nextConfig.Presets, generated)
	report.Verification = append(report.Verification, verifyEngineProfiles(report.ReadyEngines)...)
	report.Verification = append(report.Verification, verifyGeneratedPresets(generated, report.ReadyEngines, write)...)
	return report, nextConfig, nil
}

func markGeneratedEngineProfile(profile domain.EngineProfile) domain.EngineProfile {
	profile.ManagedBy = "bootstrap"
	return profile
}

func materializeEngineProfilesIntoConfig(cfg PeerConfig, profiles []domain.EngineProfile) PeerConfig {
	next := cfg
	ready := sortedProfiles(profiles)
	next.Compute = true
	next.EngineProfiles = append([]domain.EngineProfile(nil), ready...)
	runtimes := make([]BackendRuntimeConfig, 0, len(ready))
	for _, profile := range ready {
		runtimes = append(runtimes, runtimeConfigFromEngineProfile(profile))
	}
	next.ComputeConfig.Backends = runtimes
	chosen := chooseConfigPrimaryEngine(ready, cfg.ComputeConfig.Backend)
	next.ComputeConfig.Backend = chosen.Backend
	next.ComputeConfig.BackendBinary = chosen.BinaryPath
	next.ComputeConfig.CustomArgs = append([]string(nil), chosen.Args...)
	next.ComputeConfig.HealthPath = chosen.HealthPath
	return next
}

func runtimeConfigFromEngineProfile(profile domain.EngineProfile) BackendRuntimeConfig {
	return BackendRuntimeConfig{
		Backend:       profile.Backend,
		BackendBinary: profile.BinaryPath,
		CustomArgs:    append([]string(nil), profile.Args...),
		HealthPath:    profile.HealthPath,
	}
}

func chooseConfigPrimaryEngine(profiles []domain.EngineProfile, preferred domain.Backend) domain.EngineProfile {
	for _, profile := range profiles {
		if profile.Backend == preferred {
			return profile
		}
	}
	return profiles[0]
}

func generatedPresetsFromPlan(plan domain.BootstrapPlan, readyProfiles []domain.EngineProfile, existing []domain.Preset) ([]domain.Preset, []engineApplyBlocked) {
	readyByBackend := engineProfilesByBackend(readyProfiles)
	existingByID := presetsByID(existing)
	var generated []domain.Preset
	var blocked []engineApplyBlocked
	for _, candidate := range plan.ModelCandidates {
		if len(candidate.Backends) == 0 {
			blocked = append(blocked, blockedModelCandidate(candidate)...)
			continue
		}
		backendSpecificID := readyBackendCount(candidate, readyByBackend) > 1
		for _, compat := range candidate.Backends {
			profile, ok, reason := readyProfileForCompatibility(plan, candidate, compat, readyByBackend)
			if !ok {
				blocked = append(blocked, engineApplyBlocked{Kind: "model", ID: candidate.Name, Backend: compat.Backend, Reason: reason})
				continue
			}
			preset, err := generatedPresetFromCandidate(plan.ID, candidate, profile, backendSpecificID)
			if err != nil {
				blocked = append(blocked, engineApplyBlocked{Kind: "model", ID: candidate.Name, Backend: compat.Backend, Reason: err.Error()})
				continue
			}
			if existingPreset, ok := existingByID[preset.ID]; ok && existingPreset.GeneratedBy != "bootstrap" {
				blocked = append(blocked, engineApplyBlocked{Kind: "model", ID: preset.ID, Backend: compat.Backend, Reason: "existing preset is not bootstrap-generated"})
				continue
			}
			generated = append(generated, preset)
		}
	}
	sort.Slice(generated, func(i, j int) bool { return generated[i].ID < generated[j].ID })
	return generated, blocked
}

func readyProfileForCompatibility(plan domain.BootstrapPlan, candidate domain.BootstrapModelCandidate, compat domain.BootstrapBackendCompatibility, ready map[domain.Backend]domain.EngineProfile) (domain.EngineProfile, bool, string) {
	if !compat.Ready {
		reason := compat.Reason
		if reason == "" {
			reason = "backend compatibility is not ready"
		}
		return domain.EngineProfile{}, false, reason
	}
	profile, ok := ready[compat.Backend]
	if !ok {
		return domain.EngineProfile{}, false, "no saved ready engine profile for backend"
	}
	if compat.EngineProfileID != "" && compat.EngineProfileID != profile.ID {
		return domain.EngineProfile{}, false, fmt.Sprintf("compatibility row references engine profile %q but ready profile is %q", compat.EngineProfileID, profile.ID)
	}
	if compat.CompatibilityKey != (domain.CompatibilityKey{}) {
		if _, ok, reason := enginecompat.ProfileMatchesKey(profile, compat.CompatibilityKey); !ok {
			return domain.EngineProfile{}, false, reason
		}
		return profile, true, ""
	}
	if _, ok, reason := enginecompat.ProfileMatchesHost(profile, plan.Host, candidate.Format); !ok {
		return domain.EngineProfile{}, false, reason
	}
	return profile, true, ""
}

func readyBackendCount(candidate domain.BootstrapModelCandidate, ready map[domain.Backend]domain.EngineProfile) int {
	count := 0
	for _, compat := range candidate.Backends {
		if compat.Ready {
			if _, ok := ready[compat.Backend]; ok {
				count++
			}
		}
	}
	return count
}

func blockedModelCandidate(candidate domain.BootstrapModelCandidate) []engineApplyBlocked {
	if len(candidate.Backends) == 0 {
		return []engineApplyBlocked{{Kind: "model", ID: candidate.Name, Reason: "model candidate has no backend compatibility rows"}}
	}
	blocked := make([]engineApplyBlocked, 0, len(candidate.Backends))
	for _, compat := range candidate.Backends {
		reason := compat.Reason
		if reason == "" {
			reason = "backend compatibility is not ready"
		}
		blocked = append(blocked, engineApplyBlocked{Kind: "model", ID: candidate.Name, Backend: compat.Backend, Reason: reason})
	}
	return blocked
}

func generatedPresetFromCandidate(planID string, candidate domain.BootstrapModelCandidate, profile domain.EngineProfile, backendSpecificID bool) (domain.Preset, error) {
	if candidate.Name == "" {
		return domain.Preset{}, fmt.Errorf("model candidate is missing name")
	}
	if candidate.Path == "" {
		return domain.Preset{}, fmt.Errorf("model candidate %q is missing path", candidate.Name)
	}
	if candidate.SizeMB <= 0 {
		return domain.Preset{}, fmt.Errorf("model candidate %q is missing size_mb", candidate.Name)
	}
	if candidate.ContextLength <= 0 {
		return domain.Preset{}, fmt.Errorf("model candidate %q is missing context_length", candidate.Name)
	}
	kv, err := modelCandidateKVPerToken(candidate)
	if err != nil {
		return domain.Preset{}, err
	}
	id := candidate.Name
	if backendSpecificID {
		id = id + "-" + string(profile.Backend)
	}
	preset := domain.Preset{
		ID:                    id,
		ModelRef:              candidate.Path,
		Backend:               profile.Backend,
		ContextLength:         candidate.ContextLength,
		Capabilities:          append([]domain.Capability(nil), candidate.Capabilities...),
		ArtifactSizeMB:        candidate.SizeMB,
		ModelFormat:           candidate.Format,
		EstWeightsMB:          candidate.SizeMB,
		KVPerTokenMB:          kv,
		NodeID:                candidate.HostID,
		GeneratedBy:           "bootstrap",
		GeneratedFrom:         planID,
		EngineProfileID:       profile.ID,
		EngineReadiness:       domain.EngineReadinessReadyProfile,
		EngineReadinessReason: "saved ready engine profile",
	}
	if preset.ModelRef != preset.ID {
		preset.Aliases = []string{candidate.Name}
	}
	return preset, nil
}

func modelCandidateKVPerToken(candidate domain.BootstrapModelCandidate) (float64, error) {
	if candidate.Metadata == nil {
		return 0, fmt.Errorf("model candidate %q is missing kv_per_token_mb metadata", candidate.Name)
	}
	raw := strings.TrimSpace(candidate.Metadata["kv_per_token_mb"])
	if raw == "" {
		return 0, fmt.Errorf("model candidate %q is missing kv_per_token_mb metadata", candidate.Name)
	}
	kv, err := strconv.ParseFloat(raw, 64)
	if err != nil || kv < 0 {
		return 0, fmt.Errorf("model candidate %q has invalid kv_per_token_mb metadata", candidate.Name)
	}
	return kv, nil
}

func verifyEngineProfiles(profiles []domain.EngineProfile) []engineApplyEvidence {
	evidence := make([]engineApplyEvidence, 0, len(profiles)*3)
	for _, profile := range profiles {
		status := "pass"
		reason := "profile has id and backend"
		if profile.ID == "" || profile.Backend == "" {
			status = "fail"
			reason = "profile id and backend are required"
		}
		evidence = append(evidence, engineApplyEvidence{Kind: "engine_profile_fields", ID: profile.ID, Backend: profile.Backend, Status: status, Reason: reason})

		switch {
		case profile.BinaryPath == "":
			evidence = append(evidence, engineApplyEvidence{Kind: "engine_binary", ID: profile.ID, Backend: profile.Backend, Status: "unknown", Reason: "binary path is empty; runtime may resolve from PATH"})
		case strings.ContainsRune(profile.BinaryPath, os.PathSeparator):
			if info, err := os.Stat(profile.BinaryPath); err != nil {
				evidence = append(evidence, engineApplyEvidence{Kind: "engine_binary", ID: profile.ID, Backend: profile.Backend, Status: "fail", Reason: err.Error()})
			} else if info.IsDir() {
				evidence = append(evidence, engineApplyEvidence{Kind: "engine_binary", ID: profile.ID, Backend: profile.Backend, Status: "fail", Reason: "binary path is a directory"})
			} else {
				evidence = append(evidence, engineApplyEvidence{Kind: "engine_binary", ID: profile.ID, Backend: profile.Backend, Status: "pass", Reason: "binary path exists"})
			}
		default:
			evidence = append(evidence, engineApplyEvidence{Kind: "engine_binary", ID: profile.ID, Backend: profile.Backend, Status: "unknown", Reason: "binary path has no directory; PATH lookup not executed during read-only apply verification"})
		}

		if profile.Version == "" {
			evidence = append(evidence, engineApplyEvidence{Kind: "engine_version", ID: profile.ID, Backend: profile.Backend, Status: "unknown", Reason: "version not checked without executing backend binary"})
		} else {
			evidence = append(evidence, engineApplyEvidence{Kind: "engine_version", ID: profile.ID, Backend: profile.Backend, Status: "pass", Reason: "version already recorded: " + profile.Version})
		}
	}
	return evidence
}

func verifyGeneratedPresets(presets []domain.Preset, profiles []domain.EngineProfile, write bool) []engineApplyEvidence {
	profileByID := engineProfilesByID(profiles)
	evidence := make([]engineApplyEvidence, 0, len(presets)*2)
	for _, preset := range presets {
		status := "pass"
		reason := "preset has scheduler resource metadata"
		if preset.EstWeightsMB <= 0 || preset.ContextLength <= 0 || preset.KVPerTokenMB < 0 {
			status = "fail"
			reason = "preset is missing valid resource metadata"
		}
		evidence = append(evidence, engineApplyEvidence{Kind: "generated_preset_fields", ID: preset.ID, Backend: preset.Backend, Status: status, Reason: reason})

		profile, ok := profileByID[preset.EngineProfileID]
		status = "pass"
		reason = "profile does not restrict model formats"
		if !ok {
			status = "fail"
			reason = "referenced engine profile is not in the apply set"
		} else if preset.GeneratedBy != "bootstrap" {
			status = "fail"
			reason = "preset provenance is not bootstrap"
		} else if len(profile.SupportedModels) > 0 && preset.ModelFormat == "" {
			status = "unknown"
			reason = "preset model format is empty; compatibility came from the saved bootstrap plan"
		} else if len(profile.SupportedModels) > 0 && !supportedModelFormat(profile.SupportedModels, preset.ModelFormat) {
			status = "fail"
			reason = fmt.Sprintf("profile does not advertise model format %q", preset.ModelFormat)
		} else if len(profile.SupportedModels) > 0 {
			reason = "preset model format is supported by engine profile"
		}
		evidence = append(evidence, engineApplyEvidence{Kind: "model_format_claim", ID: preset.ID, Backend: preset.Backend, Status: status, Reason: reason})

		if !write {
			evidence = append(evidence, engineApplyEvidence{Kind: "store_preset_visibility", ID: preset.ID, Backend: preset.Backend, Status: "unknown", Reason: "preview mode does not write generated presets"})
		}
	}
	return evidence
}

func supportedModelFormat(formats []string, format string) bool {
	for _, value := range formats {
		if value == format {
			return true
		}
	}
	return false
}

func verifyGeneratedPresetVisibility(ctx context.Context, store *storesqlite.Store, presets []domain.Preset) []engineApplyEvidence {
	evidence := make([]engineApplyEvidence, 0, len(presets))
	for _, preset := range presets {
		stored, err := store.Preset(ctx, preset.ID)
		if err != nil {
			evidence = append(evidence, engineApplyEvidence{Kind: "store_preset_visibility", ID: preset.ID, Backend: preset.Backend, Status: "fail", Reason: err.Error()})
			continue
		}
		if stored.GeneratedBy != "bootstrap" || stored.EngineProfileID != preset.EngineProfileID {
			evidence = append(evidence, engineApplyEvidence{Kind: "store_preset_visibility", ID: preset.ID, Backend: preset.Backend, Status: "fail", Reason: "stored preset provenance does not match generated preset"})
			continue
		}
		evidence = append(evidence, engineApplyEvidence{Kind: "store_preset_visibility", ID: preset.ID, Backend: preset.Backend, Status: "pass", Reason: "generated preset is visible in control store"})
	}
	return evidence
}

func mergeGeneratedPresets(existing, generated []domain.Preset) []domain.Preset {
	if len(generated) == 0 {
		return append([]domain.Preset(nil), existing...)
	}
	out := append([]domain.Preset(nil), existing...)
	index := map[string]int{}
	for i, preset := range out {
		index[preset.ID] = i
	}
	for _, preset := range generated {
		if i, ok := index[preset.ID]; ok {
			out[i] = preset
			continue
		}
		index[preset.ID] = len(out)
		out = append(out, preset)
	}
	return out
}

func engineProfilesByID(profiles []domain.EngineProfile) map[string]domain.EngineProfile {
	out := map[string]domain.EngineProfile{}
	for _, profile := range profiles {
		if profile.ID != "" {
			out[profile.ID] = profile
		}
	}
	return out
}

func engineProfilesByBackend(profiles []domain.EngineProfile) map[domain.Backend]domain.EngineProfile {
	out := map[domain.Backend]domain.EngineProfile{}
	for _, profile := range profiles {
		if profile.Backend != "" {
			out[profile.Backend] = profile
		}
	}
	return out
}

func presetsByID(presets []domain.Preset) map[string]domain.Preset {
	out := map[string]domain.Preset{}
	for _, preset := range presets {
		if preset.ID != "" {
			out[preset.ID] = preset
		}
	}
	return out
}

func printEngineApplyReport(report engineApplyReport) {
	mode := "preview"
	if report.Write {
		mode = "written"
	}
	fmt.Printf("apply\t%s\t%s\tengines=%d\tpresets=%d\tblocked=%d\tverification=%d\n", mode, report.PlanID, len(report.ReadyEngines), len(report.GeneratedPresets), len(report.Blocked), len(report.Verification))
	if report.BackupPath != "" {
		fmt.Printf("backup\t%s\n", report.BackupPath)
	}
	for _, profile := range report.ReadyEngines {
		fmt.Printf("engine\t%s\t%s\t%s\t%s\n", profile.ID, profile.Backend, profile.ManagedBy, profile.BinaryPath)
	}
	for _, preset := range report.GeneratedPresets {
		fmt.Printf("preset\t%s\t%s\t%s\t%s\n", preset.ID, preset.Backend, preset.ModelRef, preset.EngineProfileID)
	}
	for _, blocked := range report.Blocked {
		fmt.Printf("blocked\t%s\t%s\t%s\t%s\n", blocked.Kind, blocked.ID, blocked.Backend, blocked.Reason)
	}
	for _, evidence := range report.Verification {
		fmt.Printf("verify\t%s\t%s\t%s\t%s\t%s\n", evidence.Kind, evidence.ID, evidence.Backend, evidence.Status, evidence.Reason)
	}
}

func openEngineStore(configPath, dbPath string) (*storesqlite.Store, error) {
	if dbPath != "" {
		return storesqlite.Open(dbPath)
	}
	cfg, err := bootstrapDoctorConfig(configPath)
	if err != nil {
		return nil, err
	}
	return storesqlite.Open(cfg.StorePath)
}

func sortedProfiles(profiles []domain.EngineProfile) []domain.EngineProfile {
	out := append([]domain.EngineProfile(nil), profiles...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
