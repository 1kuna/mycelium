package bootstrap

import (
	"context"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract"
)

func TestPlannerConformance(t *testing.T) {
	host := domain.HostFacts{NodeID: "node-a", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	profile := readyProfile(domain.BackendVLLM, "linux/amd64")
	contract.RunBootstrapPlannerConformance(t, "planner", func() ports.BootstrapPlanner {
		return Planner{Now: func() time.Time { return time.Unix(10, 0).UTC() }}
	}, host, profile)
}

func TestPlannerAdoptsExistingAppleSiliconEnginesAndRejectsDockerlessFallbacks(t *testing.T) {
	plan := mustPlan(t, domain.BootstrapRequest{}, domain.HostFacts{
		NodeID:          "mac",
		OS:              "darwin",
		Arch:            "arm64",
		PackageManagers: []string{"brew"},
		Accelerators:    []domain.Accelerator{{Vendor: "apple", UnifiedMemory: true}},
	}, []domain.EngineProfile{
		readyProfile(domain.BackendLlamaCpp, "darwin/arm64"),
		readyProfile(domain.BackendMLX, "darwin/arm64"),
	})
	if !hasAction(plan, "adopt-llamacpp", "adopt_engine") || !hasAction(plan, "adopt-mlx", "adopt_engine") {
		t.Fatalf("apple silicon plan actions = %+v", plan.Actions)
	}
	if hasBackend(plan.ResultingProfiles, domain.BackendVLLM) {
		t.Fatalf("mac plan included vllm: %+v", plan.ResultingProfiles)
	}
}

func TestPlannerKeepsIntelMacOffMLXAndCPUUnlessOptedIn(t *testing.T) {
	plan := mustPlan(t, domain.BootstrapRequest{RequestedEngines: []domain.Backend{domain.BackendLlamaCpp, domain.BackendMLX}}, domain.HostFacts{
		NodeID: "intel-mac",
		OS:     "darwin",
		Arch:   "amd64",
	}, nil)
	if !hasIncompatibility(plan, domain.BackendLlamaCpp, "cpu fallback") {
		t.Fatalf("intel mac cpu fallback not blocked: %+v", plan.Incompatibilities)
	}
	if !hasIncompatibility(plan, domain.BackendMLX, "darwin/arm64") {
		t.Fatalf("intel mac mlx not blocked: %+v", plan.Incompatibilities)
	}
	optIn := mustPlan(t, domain.BootstrapRequest{RequestedEngines: []domain.Backend{domain.BackendLlamaCpp}, AllowCPUFallback: true}, domain.HostFacts{
		NodeID: "intel-mac",
		OS:     "darwin",
		Arch:   "amd64",
	}, nil)
	if !hasAction(optIn, "plan-llamacpp", "install_runtime") {
		t.Fatalf("cpu opt-in plan actions = %+v", optIn.Actions)
	}
}

func TestPlannerSplitsLinuxNVIDIAArchitecturesAndSafeCaps(t *testing.T) {
	spark := mustPlan(t, domain.BootstrapRequest{}, domain.HostFacts{
		NodeID:           "spark",
		OS:               "linux",
		Arch:             "arm64",
		ContainerRuntime: "docker",
		OOMSeverity:      domain.OOMCatastrophic,
		Accelerators:     []domain.Accelerator{{Vendor: "nvidia", Kind: "gb10"}},
	}, []domain.EngineProfile{
		{ID: "bad-vllm", Backend: domain.BackendVLLM, Ready: true, SupportedPlatforms: []string{"linux/amd64"}, ArtifactPlatform: "linux/amd64", Safety: domain.EngineSafety{OOMSeverity: domain.OOMCatastrophic, VLLMGPUUtilization: 0.80}},
		readyProfile(domain.BackendSGLang, "linux/arm64"),
	})
	if !hasIncompatibility(spark, domain.BackendVLLM, "artifact platform linux/amd64") {
		t.Fatalf("spark accepted x86 vllm: %+v", spark.Incompatibilities)
	}
	if !hasAction(spark, "adopt-sglang", "adopt_engine") {
		t.Fatalf("spark did not adopt arm64 sglang: %+v", spark.Actions)
	}

	unsafe := mustPlan(t, domain.BootstrapRequest{RequestedEngines: []domain.Backend{domain.BackendVLLM}}, domain.HostFacts{
		NodeID:       "spark",
		OS:           "linux",
		Arch:         "arm64",
		OOMSeverity:  domain.OOMCatastrophic,
		Accelerators: []domain.Accelerator{{Vendor: "nvidia"}},
	}, []domain.EngineProfile{
		{ID: "unsafe-vllm", Backend: domain.BackendVLLM, Ready: true, SupportedPlatforms: []string{"linux/arm64"}, ArtifactPlatform: "linux/arm64", Safety: domain.EngineSafety{OOMSeverity: domain.OOMCatastrophic, VLLMGPUUtilization: 0.90}},
	})
	if !hasIncompatibility(unsafe, domain.BackendVLLM, "<= 0.85") {
		t.Fatalf("unsafe spark cap accepted: %+v", unsafe.Incompatibilities)
	}
}

func TestPlannerCoversB70OpenVINOAndDiskFloor(t *testing.T) {
	plan := mustPlan(t, domain.BootstrapRequest{}, domain.HostFacts{
		NodeID:           "b70",
		OS:               "linux",
		Arch:             "amd64",
		DiskTotalMB:      1000,
		DiskFreeMB:       100,
		DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
		Accelerators:     []domain.Accelerator{{Vendor: "intel", Kind: "arc-pro-b70"}},
	}, []domain.EngineProfile{
		readyProfile(domain.BackendOpenVINO, "linux/amd64"),
		readyProfile(domain.BackendLlamaCpp, "linux/amd64"),
	})
	if !hasIncompatibility(plan, domain.BackendOpenVINO, "disk free") || !hasIncompatibility(plan, domain.BackendLlamaCpp, "disk free") {
		t.Fatalf("b70 low disk not blocked: %+v", plan.Incompatibilities)
	}
	if len(plan.Actions) != 0 {
		t.Fatalf("low disk plan had actions: %+v", plan.Actions)
	}
}

func TestPlannerReportsLinuxAMDAndModelCompatibility(t *testing.T) {
	plan := mustPlan(t, domain.BootstrapRequest{
		ModelCandidates: []domain.BootstrapModelCandidate{{
			Name:   "gemma",
			Path:   "/models/gemma",
			Format: "openvino-ir",
			Backends: []domain.BootstrapBackendCompatibility{{
				Backend: domain.BackendOpenVINO,
			}},
		}},
	}, domain.HostFacts{
		NodeID:       "amd",
		OS:           "linux",
		Arch:         "amd64",
		Accelerators: []domain.Accelerator{{Vendor: "amd"}},
	}, nil)
	if !hasAction(plan, "plan-llamacpp", "install_runtime") || !hasAction(plan, "plan-vllm", "install_runtime") {
		t.Fatalf("amd plan actions = %+v", plan.Actions)
	}
	if !hasIncompatibility(plan, domain.BackendOpenVINO, "runtime is not ready") {
		t.Fatalf("model incompatibility missing: %+v", plan.Incompatibilities)
	}
}

func TestPlannerRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (Planner{Now: func() time.Time { return time.Unix(10, 0).UTC() }}).PlanBootstrap(ctx, domain.BootstrapRequest{}, domain.HostFacts{
		OS:   "linux",
		Arch: "amd64",
	}, nil)
	if err == nil || err != context.Canceled {
		t.Fatalf("PlanBootstrap error = %v, want context.Canceled", err)
	}
}

func TestPlannerNormalizesPlatformAndRequestedEngines(t *testing.T) {
	plan := mustPlan(t, domain.BootstrapRequest{
		RequestedEngines: []domain.Backend{"", domain.BackendLlamaCpp, domain.BackendLlamaCpp, domain.BackendCustom},
		AllowCPUFallback: true,
	}, domain.HostFacts{
		NodeID:           "cpu-linux",
		Platform:         "linux/amd64",
		PackageManagers:  []string{"apt"},
		ContainerRuntime: "podman",
	}, nil)
	if plan.Host.OS != "linux" || plan.Host.Arch != "amd64" || plan.Host.DiskMinFreeRatio != domain.DefaultDiskMinFreeRatio {
		t.Fatalf("normalized host = %+v", plan.Host)
	}
	if len(plan.RequestedEngines) != 2 || plan.RequestedEngines[0] != domain.BackendLlamaCpp || plan.RequestedEngines[1] != domain.BackendCustom {
		t.Fatalf("requested engines = %+v", plan.RequestedEngines)
	}
	if !hasAction(plan, "plan-llamacpp", "install_runtime") || !hasAction(plan, "plan-custom", "write_wrapper") {
		t.Fatalf("cpu linux plan actions = %+v", plan.Actions)
	}
}

func TestPlannerProducesEngineSpecificInstallActions(t *testing.T) {
	mac := mustPlan(t, domain.BootstrapRequest{
		RequestedEngines: []domain.Backend{domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendCustom},
	}, domain.HostFacts{
		NodeID:          "mac",
		OS:              "darwin",
		Arch:            "arm64",
		PackageManagers: []string{"brew"},
	}, nil)
	requireAction(t, mac, "plan-llamacpp", "install_package", []string{"brew", "install", "llama.cpp"})
	requireAction(t, mac, "plan-mlx", "create_venv", []string{"python3", "-m", "venv", "~/.mycelium/engines/mlx-lm"})
	requireAction(t, mac, "plan-custom", "write_wrapper", nil)

	nvidia := mustPlan(t, domain.BootstrapRequest{
		RequestedEngines: []domain.Backend{domain.BackendVLLM, domain.BackendSGLang},
	}, domain.HostFacts{
		NodeID:           "gpu",
		OS:               "linux",
		Arch:             "amd64",
		ContainerRuntime: "docker",
		Accelerators:     []domain.Accelerator{{Vendor: "nvidia"}},
	}, nil)
	requireAction(t, nvidia, "plan-vllm", "pull_image", []string{"docker", "pull", "platform-specific-vllm-image"})
	requireAction(t, nvidia, "plan-sglang", "pull_image", []string{"docker", "pull", "platform-specific-sglang-image"})

	intel := mustPlan(t, domain.BootstrapRequest{
		RequestedEngines: []domain.Backend{domain.BackendOpenVINO},
	}, domain.HostFacts{
		NodeID:       "b70",
		OS:           "linux",
		Arch:         "amd64",
		Accelerators: []domain.Accelerator{{Vendor: "intel"}},
	}, nil)
	requireAction(t, intel, "plan-openvino", "create_venv", []string{"python3", "-m", "venv", "~/.mycelium/engines/openvino-genai"})
}

func TestPlannerRejectsUnsupportedProfilePlatform(t *testing.T) {
	plan := mustPlan(t, domain.BootstrapRequest{RequestedEngines: []domain.Backend{domain.BackendVLLM}}, domain.HostFacts{
		NodeID:       "nvidia-x64",
		OS:           "linux",
		Arch:         "amd64",
		Accelerators: []domain.Accelerator{{Vendor: "nvidia"}},
	}, []domain.EngineProfile{{
		ID:                 "arm-only-vllm",
		Backend:            domain.BackendVLLM,
		Ready:              true,
		SupportedModels:    []string{"hf-transformers"},
		SupportedPlatforms: []string{"linux/arm64"},
	}})
	if !hasIncompatibility(plan, domain.BackendVLLM, "profile artifact does not support host platform linux/amd64") {
		t.Fatalf("platform incompatibility missing: %+v", plan.Incompatibilities)
	}
}

func TestPlannerHandlesVLLMGPUCapParsingAndModelAnnotation(t *testing.T) {
	host := domain.HostFacts{
		NodeID:       "spark",
		OS:           "linux",
		Arch:         "arm64",
		OOMSeverity:  domain.OOMCatastrophic,
		Accelerators: []domain.Accelerator{{Vendor: "nvidia"}},
	}
	safeProfile := domain.EngineProfile{
		ID:                 "spark-vllm",
		Backend:            domain.BackendVLLM,
		Ready:              true,
		SupportedModels:    []string{"hf-transformers"},
		SupportedPlatforms: []string{"linux/arm64"},
		ArtifactPlatform:   "linux/arm64",
		Args:               []string{"--gpu-memory-utilization", "0.70"},
	}
	plan := mustPlan(t, domain.BootstrapRequest{
		RequestedEngines: []domain.Backend{domain.BackendVLLM},
		ModelCandidates: []domain.BootstrapModelCandidate{{
			Name:   "qwen",
			Format: "hf-transformers",
			Backends: []domain.BootstrapBackendCompatibility{{
				Backend: domain.BackendVLLM,
			}},
		}},
	}, host, []domain.EngineProfile{safeProfile})
	if !hasAction(plan, "adopt-vllm", "adopt_engine") {
		t.Fatalf("safe vllm was not adopted: %+v", plan.Actions)
	}
	if len(plan.ModelCandidates) != 1 || len(plan.ModelCandidates[0].Backends) != 1 {
		t.Fatalf("model candidates = %+v", plan.ModelCandidates)
	}
	compat := plan.ModelCandidates[0].Backends[0]
	if !compat.Ready || compat.EngineProfileID != "spark-vllm" || compat.CompatibilityKey.ModelFormat != "hf-transformers" {
		t.Fatalf("annotated model compatibility = %+v", compat)
	}
	profile := requireProfile(t, plan, domain.BackendVLLM)
	if profile.Safety.VLLMGPUUtilization != 0.70 {
		t.Fatalf("parsed vllm gpu cap = %+v", profile.Safety)
	}

	missingCap := mustPlan(t, domain.BootstrapRequest{RequestedEngines: []domain.Backend{domain.BackendVLLM}}, host, []domain.EngineProfile{{
		ID:                 "spark-vllm",
		Backend:            domain.BackendVLLM,
		Ready:              true,
		SupportedModels:    []string{"hf-transformers"},
		SupportedPlatforms: []string{"linux/arm64"},
		ArtifactPlatform:   "linux/arm64",
		Args:               []string{"--gpu-memory-utilization=not-a-number"},
	}})
	if !hasIncompatibility(missingCap, domain.BackendVLLM, "requires explicit gpu memory utilization cap") {
		t.Fatalf("bad cap was accepted: %+v", missingCap.Incompatibilities)
	}
}

func TestPlannerUsesModelBackendReasonWhenRuntimeReady(t *testing.T) {
	plan := mustPlan(t, domain.BootstrapRequest{
		RequestedEngines: []domain.Backend{domain.BackendLlamaCpp},
		ModelCandidates: []domain.BootstrapModelCandidate{{
			Name:   "vision",
			Format: "gguf",
			Backends: []domain.BootstrapBackendCompatibility{{
				Backend: domain.BackendLlamaCpp,
				Reason:  "mmproj artifact missing",
			}},
		}},
	}, domain.HostFacts{
		NodeID:       "mac",
		OS:           "darwin",
		Arch:         "arm64",
		Accelerators: []domain.Accelerator{{Vendor: "apple"}},
	}, []domain.EngineProfile{readyProfile(domain.BackendLlamaCpp, "darwin/arm64")})
	if !hasIncompatibility(plan, domain.BackendLlamaCpp, "mmproj artifact missing") {
		t.Fatalf("model backend reason not preserved: %+v", plan.Incompatibilities)
	}
	if plan.ModelCandidates[0].Backends[0].Ready {
		t.Fatalf("model backend reason should keep candidate unready: %+v", plan.ModelCandidates)
	}
}

func mustPlan(t *testing.T, req domain.BootstrapRequest, host domain.HostFacts, profiles []domain.EngineProfile) domain.BootstrapPlan {
	t.Helper()
	plan, err := (Planner{Now: func() time.Time { return time.Unix(10, 0).UTC() }}).PlanBootstrap(context.Background(), req, host, profiles)
	if err != nil {
		t.Fatalf("PlanBootstrap: %v", err)
	}
	return plan
}

func readyProfile(backend domain.Backend, platform string) domain.EngineProfile {
	return domain.EngineProfile{
		ID:                 "engine-" + string(backend),
		Backend:            backend,
		BinaryPath:         "/bin/" + string(backend),
		Ready:              true,
		SupportedModels:    supportedFormats(backend),
		SupportedPlatforms: []string{platform},
		ArtifactPlatform:   platform,
		Safety:             domain.EngineSafety{VLLMGPUUtilization: 0.80},
	}
}

func hasAction(plan domain.BootstrapPlan, id, kind string) bool {
	for _, action := range plan.Actions {
		if action.ID == id && action.Kind == kind {
			return true
		}
	}
	return false
}

func hasBackend(profiles []domain.EngineProfile, backend domain.Backend) bool {
	for _, profile := range profiles {
		if profile.Backend == backend {
			return true
		}
	}
	return false
}

func hasIncompatibility(plan domain.BootstrapPlan, backend domain.Backend, reason string) bool {
	for _, inc := range plan.Incompatibilities {
		if inc.Backend == backend && strings.Contains(inc.Reason, reason) {
			return true
		}
	}
	return false
}

func requireAction(t *testing.T, plan domain.BootstrapPlan, id, kind string, command []string) {
	t.Helper()
	for _, action := range plan.Actions {
		if action.ID != id {
			continue
		}
		if action.Kind != kind {
			t.Fatalf("action %s kind = %s, want %s action=%+v", id, action.Kind, kind, action)
		}
		if command != nil && strings.Join(action.CommandPreview, "\x00") != strings.Join(command, "\x00") {
			t.Fatalf("action %s command = %+v, want %+v", id, action.CommandPreview, command)
		}
		return
	}
	t.Fatalf("missing action %s in %+v", id, plan.Actions)
}

func requireProfile(t *testing.T, plan domain.BootstrapPlan, backend domain.Backend) domain.EngineProfile {
	t.Helper()
	for _, profile := range plan.ResultingProfiles {
		if profile.Backend == backend {
			return profile
		}
	}
	t.Fatalf("missing profile %s in %+v", backend, plan.ResultingProfiles)
	return domain.EngineProfile{}
}
