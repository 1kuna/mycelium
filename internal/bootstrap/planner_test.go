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
