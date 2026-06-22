package bootstrap

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/enginecompat"
	"mycelium/internal/ports"
)

const sparkSafeVLLMGPUUtil = 0.85

type Planner struct {
	Now func() time.Time
}

func (p Planner) PlanBootstrap(ctx context.Context, req domain.BootstrapRequest, host domain.HostFacts, detections []domain.EngineProfile) (domain.BootstrapPlan, error) {
	if err := ctx.Err(); err != nil {
		return domain.BootstrapPlan{}, err
	}
	host = normalizeHost(host)
	now := p.now()
	requested := requestedEngines(req.RequestedEngines, host, req.AllowCPUFallback)
	plan := domain.BootstrapPlan{
		ID:               req.ID,
		CreatedAt:        now,
		Host:             host,
		RequestedEngines: append([]domain.Backend(nil), requested...),
		ModelCandidates:  append([]domain.BootstrapModelCandidate(nil), req.ModelCandidates...),
	}
	if plan.ID == "" {
		plan.ID = "bootstrap-" + strings.ReplaceAll(host.Platform, "/", "-")
	}
	byBackend := engineMap(detections)
	diskBlocked := diskBelowFloor(host)
	if diskBlocked {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf("disk free %dMB is below floor %.2f of %dMB", host.DiskFreeMB, diskFloor(host), host.DiskTotalMB))
	}
	for _, backend := range requested {
		profile := byBackend[backend]
		if profile.Backend == "" {
			profile = syntheticProfile(host, backend)
		}
		profile = normalizeProfile(host, profile)
		supported, reason := supportsBackend(host, backend, req.AllowCPUFallback)
		if !supported {
			profile.Ready = false
			profile.UnreadyReason = reason
			plan.ResultingProfiles = append(plan.ResultingProfiles, profile)
			plan.Incompatibilities = append(plan.Incompatibilities, domain.BootstrapIncompatibility{Backend: backend, Reason: reason})
			continue
		}
		if profile.ArtifactPlatform != "" && profile.ArtifactPlatform != host.Platform {
			reason := fmt.Sprintf("artifact platform %s does not match host platform %s", profile.ArtifactPlatform, host.Platform)
			profile.Ready = false
			profile.UnreadyReason = reason
			plan.ResultingProfiles = append(plan.ResultingProfiles, profile)
			plan.Incompatibilities = append(plan.Incompatibilities, domain.BootstrapIncompatibility{Backend: backend, Reason: reason})
			continue
		}
		if !platformAllowed(host.Platform, profile.SupportedPlatforms) {
			reason := fmt.Sprintf("profile artifact does not support host platform %s", host.Platform)
			profile.Ready = false
			profile.UnreadyReason = reason
			plan.ResultingProfiles = append(plan.ResultingProfiles, profile)
			plan.Incompatibilities = append(plan.Incompatibilities, domain.BootstrapIncompatibility{Backend: backend, Reason: reason})
			continue
		}
		if reason := unsafeProfileReason(host, profile); reason != "" {
			profile.Ready = false
			profile.UnreadyReason = reason
			plan.ResultingProfiles = append(plan.ResultingProfiles, profile)
			plan.Incompatibilities = append(plan.Incompatibilities, domain.BootstrapIncompatibility{Backend: backend, Reason: reason})
			continue
		}
		if diskBlocked {
			reason := "disk free is below configured floor"
			profile.Ready = false
			profile.UnreadyReason = reason
			plan.ResultingProfiles = append(plan.ResultingProfiles, profile)
			plan.Incompatibilities = append(plan.Incompatibilities, domain.BootstrapIncompatibility{Backend: backend, Reason: reason})
			continue
		}
		plan.ResultingProfiles = append(plan.ResultingProfiles, profile)
		if profile.Ready {
			plan.Actions = append(plan.Actions, domain.BootstrapAction{
				ID:              "adopt-" + string(backend),
				Kind:            "adopt_engine",
				EngineProfileID: profile.ID,
				Platform:        host.Platform,
				Reason:          "existing engine is usable",
			})
			continue
		}
		plan.Actions = append(plan.Actions, installAction(host, profile))
	}
	annotateModelCompatibility(&plan)
	sortPlan(&plan)
	return plan, nil
}

func (p Planner) now() time.Time {
	if p.Now == nil {
		return time.Now().UTC()
	}
	return p.Now().UTC()
}

func normalizeHost(host domain.HostFacts) domain.HostFacts {
	if host.Platform == "" && host.OS != "" && host.Arch != "" {
		host.Platform = host.OS + "/" + host.Arch
	}
	if host.Platform != "" && (host.OS == "" || host.Arch == "") {
		parts := strings.SplitN(host.Platform, "/", 2)
		if len(parts) == 2 {
			if host.OS == "" {
				host.OS = parts[0]
			}
			if host.Arch == "" {
				host.Arch = parts[1]
			}
		}
	}
	if host.DiskMinFreeRatio == 0 {
		host.DiskMinFreeRatio = domain.DefaultDiskMinFreeRatio
	}
	return host
}

func requestedEngines(requested []domain.Backend, host domain.HostFacts, allowCPU bool) []domain.Backend {
	if len(requested) == 0 {
		return platformDefaults(host, allowCPU)
	}
	out := make([]domain.Backend, 0, len(requested))
	seen := map[domain.Backend]struct{}{}
	for _, backend := range requested {
		if backend == "" {
			continue
		}
		if _, ok := seen[backend]; ok {
			continue
		}
		seen[backend] = struct{}{}
		out = append(out, backend)
	}
	return out
}

func platformDefaults(host domain.HostFacts, allowCPU bool) []domain.Backend {
	switch host.OS {
	case "darwin":
		if host.Arch == "arm64" {
			return []domain.Backend{domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendCustom}
		}
		if host.Arch == "amd64" {
			if allowCPU {
				return []domain.Backend{domain.BackendLlamaCpp, domain.BackendCustom}
			}
			return []domain.Backend{domain.BackendCustom}
		}
	case "linux":
		switch acceleratorVendor(host) {
		case "nvidia":
			return []domain.Backend{domain.BackendVLLM, domain.BackendSGLang, domain.BackendLlamaCpp, domain.BackendCustom}
		case "intel":
			return []domain.Backend{domain.BackendLlamaCpp, domain.BackendVLLM, domain.BackendOpenVINO, domain.BackendCustom}
		case "amd":
			return []domain.Backend{domain.BackendLlamaCpp, domain.BackendVLLM, domain.BackendCustom}
		}
		if allowCPU {
			return []domain.Backend{domain.BackendLlamaCpp, domain.BackendCustom}
		}
	}
	return []domain.Backend{domain.BackendCustom}
}

func supportsBackend(host domain.HostFacts, backend domain.Backend, allowCPU bool) (bool, string) {
	switch host.OS {
	case "darwin":
		if host.Arch == "arm64" {
			switch backend {
			case domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendCustom:
				return true, ""
			}
		}
		if host.Arch == "amd64" {
			switch backend {
			case domain.BackendMLX:
				return false, "mlx is supported only on darwin/arm64"
			case domain.BackendLlamaCpp:
				if allowCPU || len(host.Accelerators) > 0 {
					return true, ""
				}
				return false, "cpu fallback requires explicit opt-in"
			case domain.BackendCustom:
				return true, ""
			}
		}
	case "linux":
		if backend == domain.BackendCustom {
			return true, ""
		}
		switch acceleratorVendor(host) {
		case "nvidia":
			switch backend {
			case domain.BackendVLLM, domain.BackendSGLang, domain.BackendLlamaCpp, domain.BackendCustom:
				return true, ""
			}
		case "intel":
			switch backend {
			case domain.BackendLlamaCpp, domain.BackendVLLM, domain.BackendOpenVINO, domain.BackendCustom:
				return true, ""
			}
		case "amd":
			switch backend {
			case domain.BackendLlamaCpp, domain.BackendVLLM, domain.BackendCustom:
				return true, ""
			}
		default:
			if allowCPU {
				switch backend {
				case domain.BackendLlamaCpp, domain.BackendCustom:
					return true, ""
				}
			}
		}
	}
	return false, fmt.Sprintf("%s is unsupported on %s", backend, host.Platform)
}

func syntheticProfile(host domain.HostFacts, backend domain.Backend) domain.EngineProfile {
	profile := domain.EngineProfile{
		ID:                 "engine-" + string(backend),
		Backend:            backend,
		DisplayName:        displayName(backend),
		ManagedBy:          string(domain.EngineSourceManaged),
		BinaryPath:         defaultBinary(backend),
		HealthPath:         "/health",
		SupportedModels:    supportedFormats(backend),
		SupportedPlatforms: supportedPlatforms(host, backend),
		ArtifactPlatform:   host.Platform,
		MaxUtilDefault:     defaultMaxUtil(host, backend),
		DiskMinFreeRatio:   diskFloor(host),
		Safety:             domain.EngineSafety{OOMSeverity: host.OOMSeverity},
		UnreadyReason:      "engine is not installed or configured",
	}
	profile.CompatibilityKey = enginecompat.HostProfileKey(host, profile, "")
	return profile
}

func normalizeProfile(host domain.HostFacts, profile domain.EngineProfile) domain.EngineProfile {
	if profile.ID == "" {
		profile.ID = "engine-" + string(profile.Backend)
	}
	if profile.DisplayName == "" {
		profile.DisplayName = displayName(profile.Backend)
	}
	if profile.ManagedBy == "" {
		profile.ManagedBy = string(domain.EngineSourceSystem)
	}
	if len(profile.SupportedModels) == 0 {
		profile.SupportedModels = supportedFormats(profile.Backend)
	}
	if len(profile.SupportedPlatforms) == 0 {
		profile.SupportedPlatforms = supportedPlatforms(host, profile.Backend)
	}
	if profile.ArtifactPlatform == "" {
		profile.ArtifactPlatform = host.Platform
	}
	if profile.MaxUtilDefault == 0 {
		profile.MaxUtilDefault = defaultMaxUtil(host, profile.Backend)
	}
	if profile.DiskMinFreeRatio == 0 {
		profile.DiskMinFreeRatio = diskFloor(host)
	}
	if profile.Safety.OOMSeverity == "" {
		profile.Safety.OOMSeverity = host.OOMSeverity
	}
	if profile.Backend == domain.BackendVLLM && profile.Safety.VLLMGPUUtilization == 0 {
		profile.Safety.VLLMGPUUtilization = vllmGPUUtilization(profile.Args)
	}
	profile.CompatibilityKey = enginecompat.HostProfileKey(host, profile, "")
	return profile
}

func installAction(host domain.HostFacts, profile domain.EngineProfile) domain.BootstrapAction {
	action := domain.BootstrapAction{
		ID:              "plan-" + string(profile.Backend),
		Kind:            "install_runtime",
		EngineProfileID: profile.ID,
		Platform:        host.Platform,
		Reason:          profile.UnreadyReason,
	}
	switch profile.Backend {
	case domain.BackendLlamaCpp:
		if host.OS == "darwin" && hasPackageManager(host, "brew") {
			action.Kind = "install_package"
			action.CommandPreview = []string{"brew", "install", "llama.cpp"}
		}
	case domain.BackendMLX:
		action.Kind = "create_venv"
		action.CommandPreview = []string{"python3", "-m", "venv", "~/.mycelium/engines/mlx-lm"}
	case domain.BackendVLLM:
		if host.ContainerRuntime != "" {
			action.Kind = "pull_image"
			action.CommandPreview = []string{host.ContainerRuntime, "pull", "platform-specific-vllm-image"}
		}
	case domain.BackendSGLang:
		if host.ContainerRuntime != "" {
			action.Kind = "pull_image"
			action.CommandPreview = []string{host.ContainerRuntime, "pull", "platform-specific-sglang-image"}
		}
	case domain.BackendOpenVINO:
		action.Kind = "create_venv"
		action.CommandPreview = []string{"python3", "-m", "venv", "~/.mycelium/engines/openvino-genai"}
	case domain.BackendCustom:
		action.Kind = "write_wrapper"
	}
	return action
}

func unsafeProfileReason(host domain.HostFacts, profile domain.EngineProfile) string {
	if profile.Backend != domain.BackendVLLM || host.OOMSeverity != domain.OOMCatastrophic {
		return ""
	}
	if profile.Safety.VLLMGPUUtilization == 0 {
		return "catastrophic vllm host requires explicit gpu memory utilization cap"
	}
	if profile.Safety.VLLMGPUUtilization > sparkSafeVLLMGPUUtil {
		return "catastrophic vllm host requires gpu memory utilization <= 0.85"
	}
	return ""
}

func annotateModelCompatibility(plan *domain.BootstrapPlan) {
	ready := map[domain.Backend]bool{}
	reasons := map[domain.Backend]string{}
	for _, profile := range plan.ResultingProfiles {
		ready[profile.Backend] = profile.Ready
		reasons[profile.Backend] = profile.UnreadyReason
	}
	for i := range plan.ModelCandidates {
		for j := range plan.ModelCandidates[i].Backends {
			backend := plan.ModelCandidates[i].Backends[j].Backend
			if profile, ok := engineMap(plan.ResultingProfiles)[backend]; ok {
				compat := enginecompat.HostProfileKey(plan.Host, profile, plan.ModelCandidates[i].Format)
				plan.ModelCandidates[i].Backends[j].EngineProfileID = profile.ID
				plan.ModelCandidates[i].Backends[j].CompatibilityKey = compat
			}
			if ready[backend] && plan.ModelCandidates[i].Backends[j].Reason == "" {
				plan.ModelCandidates[i].Backends[j].Ready = true
				continue
			}
			plan.ModelCandidates[i].Backends[j].Ready = false
			if plan.ModelCandidates[i].Backends[j].Reason == "" {
				if reason := reasons[backend]; reason != "" {
					plan.ModelCandidates[i].Backends[j].Reason = reason
				} else {
					plan.ModelCandidates[i].Backends[j].Reason = string(backend) + " runtime is not ready"
				}
			}
			plan.Incompatibilities = append(plan.Incompatibilities, domain.BootstrapIncompatibility{
				Backend: backend,
				Model:   plan.ModelCandidates[i].Name,
				Reason:  plan.ModelCandidates[i].Backends[j].Reason,
			})
		}
	}
}

func diskBelowFloor(host domain.HostFacts) bool {
	if host.DiskTotalMB <= 0 {
		return false
	}
	return float64(host.DiskFreeMB) < float64(host.DiskTotalMB)*diskFloor(host)
}

func diskFloor(host domain.HostFacts) float64 {
	if host.DiskMinFreeRatio > 0 {
		return host.DiskMinFreeRatio
	}
	return domain.DefaultDiskMinFreeRatio
}

func defaultMaxUtil(host domain.HostFacts, backend domain.Backend) float64 {
	if backend == domain.BackendVLLM && host.OOMSeverity == domain.OOMCatastrophic {
		return sparkSafeVLLMGPUUtil
	}
	return 0.90
}

func supportedPlatforms(host domain.HostFacts, backend domain.Backend) []string {
	switch backend {
	case domain.BackendMLX:
		return []string{"darwin/arm64"}
	case domain.BackendVLLM, domain.BackendSGLang:
		return []string{"linux/amd64", "linux/arm64"}
	case domain.BackendLlamaCpp, domain.BackendOpenVINO, domain.BackendCustom:
		return []string{host.Platform}
	default:
		return nil
	}
}

func supportedFormats(backend domain.Backend) []string {
	switch backend {
	case domain.BackendLlamaCpp:
		return []string{"gguf"}
	case domain.BackendVLLM, domain.BackendSGLang:
		return []string{"hf-transformers"}
	case domain.BackendMLX:
		return []string{"mlx", "hf-transformers"}
	case domain.BackendOpenVINO:
		return []string{"openvino-ir"}
	default:
		return nil
	}
}

func defaultBinary(backend domain.Backend) string {
	switch backend {
	case domain.BackendLlamaCpp:
		return "llama-server"
	case domain.BackendMLX:
		return "mlx_lm.server"
	case domain.BackendVLLM:
		return "vllm"
	case domain.BackendSGLang:
		return "sglang"
	case domain.BackendOpenVINO:
		return "openvino-genai-openai"
	default:
		return ""
	}
}

func displayName(backend domain.Backend) string {
	switch backend {
	case domain.BackendLlamaCpp:
		return "llama.cpp"
	case domain.BackendMLX:
		return "MLX"
	case domain.BackendVLLM:
		return "vLLM"
	case domain.BackendSGLang:
		return "SGLang"
	case domain.BackendOpenVINO:
		return "OpenVINO GenAI"
	default:
		return string(backend)
	}
}

func acceleratorVendor(host domain.HostFacts) string {
	for _, acc := range host.Accelerators {
		vendor := strings.ToLower(acc.Vendor)
		if vendor != "" {
			return vendor
		}
	}
	return ""
}

func hasPackageManager(host domain.HostFacts, name string) bool {
	for _, manager := range host.PackageManagers {
		if manager == name {
			return true
		}
	}
	return false
}

func vllmGPUUtilization(args []string) float64 {
	for i, arg := range args {
		if strings.HasPrefix(arg, "--gpu-memory-utilization=") {
			var value float64
			if _, err := fmt.Sscanf(strings.TrimPrefix(arg, "--gpu-memory-utilization="), "%f", &value); err == nil {
				return value
			}
			return 0
		}
		if arg == "--gpu-memory-utilization" && i+1 < len(args) {
			var value float64
			if _, err := fmt.Sscanf(args[i+1], "%f", &value); err == nil {
				return value
			}
			return 0
		}
	}
	return 0
}

func platformAllowed(platform string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, value := range allowed {
		if value == platform {
			return true
		}
	}
	return false
}

func engineMap(profiles []domain.EngineProfile) map[domain.Backend]domain.EngineProfile {
	out := map[domain.Backend]domain.EngineProfile{}
	for _, profile := range profiles {
		if profile.Backend == "" {
			continue
		}
		out[profile.Backend] = profile
	}
	return out
}

func sortPlan(plan *domain.BootstrapPlan) {
	sort.Slice(plan.Actions, func(i, j int) bool { return plan.Actions[i].ID < plan.Actions[j].ID })
	sort.Slice(plan.ResultingProfiles, func(i, j int) bool { return plan.ResultingProfiles[i].ID < plan.ResultingProfiles[j].ID })
	sort.Slice(plan.Incompatibilities, func(i, j int) bool {
		if plan.Incompatibilities[i].Model != plan.Incompatibilities[j].Model {
			return plan.Incompatibilities[i].Model < plan.Incompatibilities[j].Model
		}
		return plan.Incompatibilities[i].Backend < plan.Incompatibilities[j].Backend
	})
}

var _ ports.BootstrapPlanner = Planner{}
