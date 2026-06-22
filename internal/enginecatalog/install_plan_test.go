package enginecatalog

import (
	"strings"
	"testing"

	"mycelium/internal/domain"
)

func TestPlanInstallCoversSupportedEnginePlatformMatrix(t *testing.T) {
	tests := []struct {
		name    string
		backend domain.Backend
		host    domain.HostFacts
		want    string
		action  string
		risk    string
		note    string
	}{
		{
			name:    "apple silicon llamacpp metal",
			backend: domain.BackendLlamaCpp,
			host:    domain.HostFacts{NodeID: "mac", OS: "darwin", Arch: "arm64", Platform: "darwin/arm64", Accelerators: []domain.Accelerator{{Vendor: "apple"}}},
			want:    InstallPlanAvailable,
			action:  "install-homebrew-llamacpp",
			note:    "native macOS path",
		},
		{
			name:    "spark vllm arm64",
			backend: domain.BackendVLLM,
			host:    domain.HostFacts{NodeID: "spark", OS: "linux", Arch: "arm64", Platform: "linux/arm64", OOMSeverity: domain.OOMCatastrophic, Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}},
			want:    InstallPlanAvailable,
			action:  "resolve-platform-vllm",
			risk:    "Spark-class catastrophic hosts",
		},
		{
			name:    "nvidia x86 vllm",
			backend: domain.BackendVLLM,
			host:    domain.HostFacts{NodeID: "nvidia-x64", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}},
			want:    InstallPlanAvailable,
			action:  "resolve-platform-vllm",
			risk:    "CUDA and CPU architecture",
		},
		{
			name:    "nvidia sglang planning",
			backend: domain.BackendSGLang,
			host:    domain.HostFacts{NodeID: "spark", OS: "linux", Arch: "arm64", Platform: "linux/arm64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}},
			want:    InstallPlanAvailable,
			action:  "write-sglang-profile-after-adapter",
			risk:    "backend adapter",
		},
		{
			name:    "b70 llamacpp sycl",
			backend: domain.BackendLlamaCpp,
			host:    domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}},
			want:    InstallPlanAvailable,
			action:  "install-oneapi-sycl-llamacpp",
			note:    "B70 OpenVINO/SYCL adoption proof",
		},
		{
			name:    "b70 openvino",
			backend: domain.BackendOpenVINO,
			host:    domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}},
			want:    InstallPlanAvailable,
			action:  "choose-openvino-serving",
			risk:    "GenAI vs OVMS",
		},
		{
			name:    "apple silicon mlx",
			backend: domain.BackendMLX,
			host:    domain.HostFacts{NodeID: "mac", OS: "darwin", Arch: "arm64", Platform: "darwin/arm64", Accelerators: []domain.Accelerator{{Vendor: "apple"}}},
			want:    InstallPlanAvailable,
			action:  "create-mlx-venv",
			risk:    "Apple Silicon only",
		},
		{
			name:    "amd llamacpp rocm contract",
			backend: domain.BackendLlamaCpp,
			host:    domain.HostFacts{NodeID: "amd", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "amd"}}},
			want:    InstallPlanAvailable,
			action:  "install-llamacpp-rocm",
			risk:    "contract-level",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := PlanInstall(InstallPlanRequest{Backend: tt.backend, Host: tt.host})
			if err != nil {
				t.Fatalf("PlanInstall: %v", err)
			}
			if plan.Status != tt.want || !plan.RequiresApproval || !plan.DryRunOnly || plan.Rollback != "manual" {
				t.Fatalf("plan = %+v", plan)
			}
			if tt.action != "" && !hasAction(plan, tt.action) {
				t.Fatalf("missing action %q in %+v", tt.action, plan.Actions)
			}
			if tt.risk != "" && !containsString(plan.Risks, tt.risk) {
				t.Fatalf("missing risk %q in %+v", tt.risk, plan.Risks)
			}
			if tt.note != "" && !containsString(plan.Notes, tt.note) {
				t.Fatalf("missing note %q in %+v", tt.note, plan.Notes)
			}
		})
	}
}

func TestPlanInstallRejectsUnsupportedOrUnsafePaths(t *testing.T) {
	intelMac := domain.HostFacts{NodeID: "intel-mac", OS: "darwin", Arch: "amd64", Platform: "darwin/amd64"}
	plan, err := PlanInstall(InstallPlanRequest{Backend: domain.BackendMLX, Host: intelMac})
	if err != nil {
		t.Fatalf("PlanInstall mlx intel mac: %v", err)
	}
	if plan.Status != InstallPlanUnsupported || !strings.Contains(plan.Reason, "Intel macOS") {
		t.Fatalf("intel mac mlx plan = %+v", plan)
	}
	plan, err = PlanInstall(InstallPlanRequest{Backend: domain.BackendLlamaCpp, Host: intelMac})
	if err != nil {
		t.Fatalf("PlanInstall llamacpp intel mac: %v", err)
	}
	if plan.Status != InstallPlanUnsupported || !strings.Contains(plan.Reason, "CPU fallback requires explicit opt-in") {
		t.Fatalf("intel mac llamacpp plan = %+v", plan)
	}
	plan, err = PlanInstall(InstallPlanRequest{Backend: domain.BackendLlamaCpp, Host: intelMac, AllowCPUFallback: true})
	if err != nil {
		t.Fatalf("PlanInstall cpu fallback: %v", err)
	}
	if plan.Status != InstallPlanAvailable || !hasAction(plan, "install-homebrew-llamacpp-cpu") {
		t.Fatalf("cpu fallback plan = %+v", plan)
	}
	lowDiskB70 := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", DiskFreeMB: 10, DiskTotalMB: 100, Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	plan, err = PlanInstall(InstallPlanRequest{Backend: domain.BackendOpenVINO, Host: lowDiskB70})
	if err != nil {
		t.Fatalf("PlanInstall low disk: %v", err)
	}
	if plan.Status != InstallPlanBlocked || !strings.Contains(plan.Reason, "disk floor") {
		t.Fatalf("low disk plan = %+v", plan)
	}
}

func TestPlanInstallCustomIsManualOnly(t *testing.T) {
	plan, err := PlanInstall(InstallPlanRequest{
		Backend: domain.BackendCustom,
		Host:    domain.HostFacts{NodeID: "host", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}},
	})
	if err != nil {
		t.Fatalf("PlanInstall custom: %v", err)
	}
	if plan.Status != InstallPlanManual || len(plan.Actions) != 1 || !plan.Actions[0].Manual || !strings.Contains(plan.Reason, "no generic installer") {
		t.Fatalf("custom plan = %+v", plan)
	}
}

func hasAction(plan InstallPlan, id string) bool {
	for _, action := range plan.Actions {
		if action.ID == id && action.RequiresApproval || action.ID == id && action.DryRunOnly {
			return true
		}
	}
	return false
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
