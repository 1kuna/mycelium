package enginecatalog

import (
	"fmt"
	"strings"

	"mycelium/internal/domain"
)

const (
	InstallPlanAvailable   = "install_plan_available"
	InstallPlanManual      = "manual_profile_required"
	InstallPlanUnsupported = "not_supported"
	InstallPlanBlocked     = "blocked"
)

type InstallPlanRequest struct {
	Backend          domain.Backend
	Host             domain.HostFacts
	AllowCPUFallback bool
}

type InstallPlan struct {
	Backend          domain.Backend  `json:"backend"`
	EngineFamily     string          `json:"engine_family,omitempty"`
	HostID           string          `json:"host_id,omitempty"`
	HostPlatform     string          `json:"host_platform,omitempty"`
	Accelerator      string          `json:"accelerator,omitempty"`
	Status           string          `json:"status"`
	Reason           string          `json:"reason,omitempty"`
	RequiresApproval bool            `json:"requires_approval"`
	DryRunOnly       bool            `json:"dry_run_only"`
	Rollback         string          `json:"rollback"`
	Actions          []InstallAction `json:"actions,omitempty"`
	Risks            []string        `json:"risks,omitempty"`
	Notes            []string        `json:"notes,omitempty"`
}

type InstallAction struct {
	ID               string   `json:"id"`
	Kind             string   `json:"kind"`
	CommandPreview   []string `json:"command_preview,omitempty"`
	Package          string   `json:"package,omitempty"`
	Image            string   `json:"image,omitempty"`
	ManagedPath      string   `json:"managed_path,omitempty"`
	Platform         string   `json:"platform,omitempty"`
	RequiresApproval bool     `json:"requires_approval"`
	DryRunOnly       bool     `json:"dry_run_only"`
	Manual           bool     `json:"manual"`
	Reason           string   `json:"reason,omitempty"`
}

func PlanInstall(req InstallPlanRequest) (InstallPlan, error) {
	if req.Backend == "" {
		return InstallPlan{}, fmt.Errorf("backend is required")
	}
	hostID := req.Host.NodeID
	if hostID == "" {
		hostID = hostPlatform(req.Host)
	}
	plan := InstallPlan{
		Backend:          req.Backend,
		HostID:           hostID,
		HostPlatform:     hostPlatform(req.Host),
		Accelerator:      hostVendor(req.Host),
		RequiresApproval: true,
		DryRunOnly:       true,
		Rollback:         "manual",
	}
	if req.Backend == domain.BackendCustom {
		plan.EngineFamily = "custom/native process"
		plan.Status = InstallPlanManual
		plan.Reason = "custom/native engines have no generic installer; provide an explicit binary/profile and health path"
		plan.Actions = []InstallAction{manualAction("custom-profile", "operator supplies backend binary, launch args, health path, and rollback notes")}
		return plan, nil
	}
	capability, ok := capabilityByBackend(req.Backend)
	if !ok {
		return InstallPlan{}, fmt.Errorf("unknown backend %q", req.Backend)
	}
	plan.EngineFamily = capability.Family
	if blocked, reason := diskFloorBlocked(req.Host); blocked {
		plan.Status = InstallPlanBlocked
		plan.Reason = reason
		plan.Actions = []InstallAction{{ID: "disk-floor", Kind: "safety_check", DryRunOnly: true, Reason: reason}}
		return plan, nil
	}
	support := capability.AssessHost(req.Host)
	if req.Backend == domain.BackendLlamaCpp && isIntelMac(req.Host) && req.AllowCPUFallback {
		support = HostSupport{Supported: true, Runtime: "cpu", Reason: "explicit CPU fallback requested for Intel macOS"}
	}
	if !support.Supported {
		plan.Status = InstallPlanUnsupported
		plan.Reason = support.Reason
		plan.Actions = []InstallAction{{ID: "unsupported", Kind: "safety_check", DryRunOnly: true, Reason: support.Reason}}
		return plan, nil
	}
	plan.Status = InstallPlanAvailable
	plan.Reason = support.Reason
	plan.Actions, plan.Risks, plan.Notes = actionsFor(req.Backend, req.Host, support.Runtime, req.AllowCPUFallback)
	return plan, nil
}

func actionsFor(backend domain.Backend, host domain.HostFacts, runtime string, allowCPU bool) ([]InstallAction, []string, []string) {
	osName, arch := hostOSArch(host)
	vendor := hostVendor(host)
	platform := osName + "/" + arch
	switch backend {
	case domain.BackendLlamaCpp:
		switch {
		case osName == "darwin" && arch == "arm64":
			return []InstallAction{
				detectAction("detect-llama-server", "llama-server"),
				packageAction("install-homebrew-llamacpp", "brew", "llama.cpp", platform),
				verifyAction("verify-llamacpp", "llama-server --version"),
				profileAction("write-llamacpp-metal-profile", platform),
			}, nil, []string{"native macOS path; Docker is not planned for Mac compute"}
		case isIntelMac(host) && allowCPU:
			return []InstallAction{
				detectAction("detect-llama-server", "llama-server"),
				packageAction("install-homebrew-llamacpp-cpu", "brew", "llama.cpp", platform),
				verifyAction("verify-llamacpp-cpu", "llama-server --version"),
				profileAction("write-llamacpp-cpu-profile", platform),
			}, []string{"CPU fallback is low-throughput and must remain explicit"}, nil
		case osName == "linux" && vendor == "nvidia":
			return []InstallAction{
				detectAction("detect-llamacpp-cuda-wrapper", "llama-server"),
				manualPackageAction("install-llamacpp-cuda", "platform-native CUDA llama.cpp build or verified container wrapper", platform),
				verifyAction("verify-llamacpp-cuda", "llama-server --version"),
				profileAction("write-llamacpp-cuda-profile", platform),
			}, []string{"CUDA build/image must match host architecture and driver runtime"}, nil
		case osName == "linux" && vendor == "intel":
			return []InstallAction{
				detectAction("detect-llamacpp-sycl-wrapper", "llama-server"),
				manualPackageAction("install-oneapi-sycl-llamacpp", "Intel oneAPI/SYCL runtime plus llama.cpp SYCL wrapper", platform),
				verifyAction("verify-llamacpp-sycl", "llama-server --version"),
				profileAction("write-llamacpp-sycl-profile", platform),
			}, []string{"B70-style SYCL wrappers need explicit runtime library path evidence"}, []string{"B70 OpenVINO/SYCL adoption proof exists as host evidence, but this plan remains non-mutating"}
		case osName == "linux" && vendor == "amd":
			return []InstallAction{
				detectAction("detect-llamacpp-rocm-wrapper", "llama-server"),
				manualPackageAction("install-llamacpp-rocm", "ROCm-compatible llama.cpp build", platform),
				verifyAction("verify-llamacpp-rocm", "llama-server --version"),
			}, []string{"ROCm support is contract-level until verified on local hardware"}, nil
		}
	case domain.BackendVLLM:
		if osName == "linux" && vendor == "nvidia" {
			cap := "0.85"
			if arch == "amd64" {
				cap = "host-configured safe cap"
			}
			return []InstallAction{
				detectAction("detect-vllm-wrapper", "vllm"),
				manualPackageAction("resolve-platform-vllm", "platform-safe vLLM wheel or container image", platform),
				verifyAction("verify-cuda-runtime", "python -m vllm.entrypoints.openai.api_server --help"),
				profileAction("write-vllm-profile-with-gpu-cap-"+strings.ReplaceAll(cap, ".", "_"), platform),
			}, []string{"wheel/image must match CUDA and CPU architecture", "Spark-class catastrophic hosts must keep vLLM gpu memory utilization <= 0.85"}, nil
		}
	case domain.BackendSGLang:
		if osName == "linux" && vendor == "nvidia" {
			return []InstallAction{
				detectAction("detect-sglang-wrapper", "python -m sglang.launch_server"),
				manualPackageAction("resolve-platform-sglang", "platform-safe SGLang wheel or container image", platform),
				profileAction("write-sglang-profile-after-adapter", platform),
			}, []string{"SGLang remains planned until a backend adapter/profile is verified"}, nil
		}
	case domain.BackendMLX:
		return []InstallAction{
			detectAction("detect-mlx-lm", "python -m mlx_lm.server --help"),
			venvAction("create-mlx-venv", "~/.mycelium/engines/mlx-lm", platform),
			packageAction("install-mlx-lm", "pip", "mlx-lm", platform),
			verifyAction("verify-mlx-lm", "python -m mlx_lm.server --help"),
			profileAction("write-mlx-profile", platform),
		}, []string{"MLX is Apple Silicon only in the supported product path"}, nil
	case domain.BackendOpenVINO:
		return []InstallAction{
			detectAction("detect-openvino-runtime", "python -c 'import openvino'"),
			venvAction("create-openvino-venv", "~/.mycelium/engines/openvino", platform),
			packageAction("install-openvino-genai", "pip", "openvino-genai", platform),
			manualPackageAction("choose-openvino-serving", "OpenVINO GenAI OpenAI wrapper or OVMS profile, depending on model family support", platform),
			verifyAction("verify-openvino", "openvino runtime version probe"),
			profileAction("write-openvino-profile", platform),
		}, []string{"GenAI vs OVMS serving path must be selected per model family; rollback is manual until apply design is complete"}, []string{"B70 Gemma/OpenVINO path is a concrete proof case, not a generic auto-upgrade policy"}
	}
	return nil, nil, nil
}

func detectAction(id, command string) InstallAction {
	return InstallAction{ID: id, Kind: "detect_existing", CommandPreview: strings.Fields(command), DryRunOnly: true}
}

func packageAction(id, tool, pkg, platform string) InstallAction {
	return InstallAction{ID: id, Kind: "install_package", CommandPreview: []string{tool, "install", pkg}, Package: pkg, Platform: platform, RequiresApproval: true, DryRunOnly: true}
}

func manualPackageAction(id, pkg, platform string) InstallAction {
	return InstallAction{ID: id, Kind: "plan_runtime_source", Package: pkg, Platform: platform, RequiresApproval: true, DryRunOnly: true, Manual: true}
}

func venvAction(id, path, platform string) InstallAction {
	return InstallAction{ID: id, Kind: "create_venv", ManagedPath: path, Platform: platform, RequiresApproval: true, DryRunOnly: true}
}

func verifyAction(id, command string) InstallAction {
	return InstallAction{ID: id, Kind: "verify", CommandPreview: strings.Fields(command), DryRunOnly: true, Reason: "verification is planned but not executed by install-plan"}
}

func profileAction(id, platform string) InstallAction {
	return InstallAction{ID: id, Kind: "write_profile", Platform: platform, RequiresApproval: true, DryRunOnly: true}
}

func manualAction(id, reason string) InstallAction {
	return InstallAction{ID: id, Kind: "manual", Manual: true, DryRunOnly: true, RequiresApproval: true, Reason: reason}
}

func capabilityByBackend(backend domain.Backend) (Capability, bool) {
	for _, capability := range Default() {
		if capability.Backend == backend {
			return capability, true
		}
	}
	return Capability{}, false
}

func hostPlatform(host domain.HostFacts) string {
	if host.Platform != "" {
		return host.Platform
	}
	osName, arch := hostOSArch(host)
	if osName == "" && arch == "" {
		return ""
	}
	return osName + "/" + arch
}

func isIntelMac(host domain.HostFacts) bool {
	osName, arch := hostOSArch(host)
	return osName == "darwin" && arch == "amd64"
}

func diskFloorBlocked(host domain.HostFacts) (bool, string) {
	if host.DiskTotalMB <= 0 || host.DiskFreeMB < 0 {
		return false, ""
	}
	floor := host.DiskMinFreeRatio
	if floor <= 0 {
		floor = domain.DefaultDiskMinFreeRatio
	}
	ratio := float64(host.DiskFreeMB) / float64(host.DiskTotalMB)
	if ratio >= floor {
		return false, ""
	}
	return true, fmt.Sprintf("disk floor blocked: free ratio %.3f below required %.3f", ratio, floor)
}
