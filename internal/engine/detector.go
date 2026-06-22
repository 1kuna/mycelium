package engine

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/enginecompat"
	"mycelium/internal/hardware"
	"mycelium/internal/ports"
)

const SparkSafeVLLMGPUUtilization = 0.85

type Detector struct {
	GOOS     string
	GOARCH   string
	Command  func(context.Context, string, ...string) ([]byte, error)
	Hardware ports.HardwareDetector
	Clock    ports.Clock
}

type Config struct {
	Backend          domain.Backend
	BackendBinary    string
	CustomArgs       []string
	HealthPath       string
	MaxUtil          float64
	DiskMinFreeRatio float64
}

func NewDetector() Detector {
	return Detector{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Command: runCommand, Hardware: hardware.NewDetector(), Clock: clock.System{}}
}

func (d Detector) DetectHost(ctx context.Context, seed domain.Node) (domain.HostFacts, error) {
	goos := d.GOOS
	if goos == "" {
		goos = runtime.GOOS
	}
	goarch := d.GOARCH
	if goarch == "" {
		goarch = runtime.GOARCH
	}
	detector := d.Hardware
	if detector == nil {
		hw := hardware.NewDetector()
		detector = hw
	}
	node, err := detector.Detect(ctx, seed)
	if err != nil {
		return domain.HostFacts{}, err
	}
	return domain.HostFacts{
		NodeID:           node.ID,
		OS:               goos,
		Arch:             goarch,
		Platform:         goos + "/" + goarch,
		PackageManagers:  d.detectPackageManagers(ctx),
		ContainerRuntime: d.detectContainerRuntime(ctx),
		Accelerators:     append([]domain.Accelerator(nil), node.Accelerators...),
		DriverFacts:      driverFacts(node),
		TotalMemoryMB:    largestMemory(node),
		DiskFreeMB:       node.DiskFreeMB,
		DiskTotalMB:      node.DiskTotalMB,
		DiskMinFreeRatio: node.DiskMinFreeRatio,
		OOMSeverity:      node.OOMSeverity,
	}, nil
}

func (d Detector) DetectEngines(ctx context.Context, host domain.HostFacts) ([]domain.EngineProfile, error) {
	now := d.now()
	profiles := []domain.EngineProfile{
		d.detectProfile(ctx, host, domain.BackendLlamaCpp, "llama.cpp", "llama-server", []string{"gguf"}, []string{host.Platform}, now),
		d.detectProfile(ctx, host, domain.BackendMLX, "MLX", "mlx_lm.server", []string{"mlx", "hf-transformers"}, []string{"darwin/arm64"}, now),
		d.detectProfile(ctx, host, domain.BackendVLLM, "vLLM", "vllm", []string{"hf-transformers"}, []string{"linux/amd64", "linux/arm64"}, now),
		d.detectProfile(ctx, host, domain.BackendSGLang, "SGLang", "sglang", []string{"hf-transformers"}, []string{"linux/amd64", "linux/arm64"}, now),
		d.detectProfile(ctx, host, domain.BackendOpenVINO, "OpenVINO GenAI", "openvino-genai-openai", []string{"openvino-ir"}, []string{"linux/amd64", "linux/arm64", "darwin/arm64", "darwin/amd64"}, now),
	}
	sort.Slice(profiles, func(i, j int) bool { return profiles[i].ID < profiles[j].ID })
	return profiles, nil
}

func (d Detector) DetectConfiguredEngine(ctx context.Context, host domain.HostFacts, cfg Config) domain.EngineProfile {
	if cfg.Backend == "" {
		return domain.EngineProfile{Ready: false, UnreadyReason: "configured backend is required"}
	}
	display := string(cfg.Backend)
	binary := cfg.BackendBinary
	if binary == "" {
		binary = defaultBinary(cfg.Backend)
	}
	profile := d.detectProfile(ctx, host, cfg.Backend, display, binary, nil, supportedPlatformsForBackend(cfg.Backend, host.Platform), d.now())
	profile.Args = append([]string(nil), cfg.CustomArgs...)
	profile.HealthPath = cfg.HealthPath
	profile.MaxUtilDefault = cfg.MaxUtil
	profile.DiskMinFreeRatio = cfg.DiskMinFreeRatio
	if cfg.Backend == domain.BackendVLLM && host.OOMSeverity == domain.OOMCatastrophic && !hasVLLMUtil(profile.Args) {
		profile.Args = append(profile.Args, "--gpu-memory-utilization", "0.85")
		profile.Safety.VLLMGPUUtilization = SparkSafeVLLMGPUUtilization
	}
	return profile
}

func (d Detector) detectProfile(ctx context.Context, host domain.HostFacts, backend domain.Backend, name, binary string, formats, platforms []string, now time.Time) domain.EngineProfile {
	profile := domain.EngineProfile{
		ID:                 "engine-" + string(backend),
		Backend:            backend,
		DisplayName:        name,
		ManagedBy:          "system",
		BinaryPath:         binary,
		HealthPath:         "/health",
		SupportedModels:    append([]string(nil), formats...),
		SupportedPlatforms: append([]string(nil), platforms...),
		ArtifactPlatform:   host.Platform,
		MaxUtilDefault:     0.90,
		DiskMinFreeRatio:   domain.DefaultDiskMinFreeRatio,
		Safety:             domain.EngineSafety{OOMSeverity: host.OOMSeverity},
		VerifiedAt:         now,
	}
	profile.CompatibilityKey = enginecompat.HostProfileKey(host, profile, "")
	if !platformAllowed(host.Platform, profile.SupportedPlatforms) {
		profile.UnreadyReason = "unsupported platform " + host.Platform
		return profile
	}
	path, err := d.resolveBinary(ctx, binary)
	if err != nil {
		profile.UnreadyReason = err.Error()
		return profile
	}
	profile.BinaryPath = path
	version, err := d.probeExecutable(ctx, path)
	if err != nil {
		profile.UnreadyReason = err.Error()
		return profile
	}
	profile.Version = version
	profile.CompatibilityKey = enginecompat.HostProfileKey(host, profile, "")
	profile.Ready = true
	if backend == domain.BackendVLLM && host.OOMSeverity == domain.OOMCatastrophic {
		profile.Args = []string{"--gpu-memory-utilization", "0.85"}
		profile.Safety.VLLMGPUUtilization = SparkSafeVLLMGPUUtilization
	}
	return profile
}

func (d Detector) detectPackageManagers(ctx context.Context) []string {
	var found []string
	for _, name := range []string{"brew", "apt", "dnf", "pacman", "docker", "uv", "pipx"} {
		if _, err := d.resolveBinary(ctx, name); err == nil {
			found = append(found, name)
		}
	}
	return found
}

func (d Detector) detectContainerRuntime(ctx context.Context) string {
	for _, name := range []string{"docker", "podman"} {
		if _, err := d.resolveBinary(ctx, name); err == nil {
			return name
		}
	}
	return ""
}

func (d Detector) resolveBinary(ctx context.Context, binary string) (string, error) {
	if binary == "" {
		return "", fmt.Errorf("engine binary is required")
	}
	if strings.Contains(binary, "/") {
		return binary, nil
	}
	out, err := d.command(ctx, "which", binary)
	if err != nil {
		return "", fmt.Errorf("%s not found", binary)
	}
	path := strings.TrimSpace(string(out))
	if path == "" {
		return "", fmt.Errorf("%s not found", binary)
	}
	return path, nil
}

func (d Detector) probeExecutable(ctx context.Context, binary string) (string, error) {
	out, err := d.command(ctx, binary, "--version")
	if err == nil {
		version := strings.TrimSpace(string(out))
		if version == "" {
			return "unknown", nil
		}
		return version, nil
	}
	if _, helpErr := d.command(ctx, binary, "--help"); helpErr != nil {
		return "", fmt.Errorf("%s probe failed: %w", binary, err)
	}
	return "unknown", nil
}

func (d Detector) command(ctx context.Context, name string, args ...string) ([]byte, error) {
	if d.Command != nil {
		return d.Command(ctx, name, args...)
	}
	return runCommand(ctx, name, args...)
}

func (d Detector) now() time.Time {
	if d.Clock == nil {
		return clock.System{}.Now().UTC()
	}
	return d.Clock.Now().UTC()
}

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.Output()
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

func supportedPlatformsForBackend(backend domain.Backend, hostPlatform string) []string {
	switch backend {
	case domain.BackendMLX:
		return []string{"darwin/arm64"}
	case domain.BackendVLLM, domain.BackendSGLang:
		return []string{"linux/amd64", "linux/arm64"}
	case domain.BackendLlamaCpp, domain.BackendOpenVINO, domain.BackendCustom:
		return []string{hostPlatform}
	default:
		return nil
	}
}

func defaultBinary(backend domain.Backend) string {
	switch backend {
	case domain.BackendMLX:
		return "mlx_lm.server"
	case domain.BackendVLLM:
		return "vllm"
	case domain.BackendSGLang:
		return "sglang"
	case domain.BackendOpenVINO:
		return "openvino-genai-openai"
	case domain.BackendLlamaCpp:
		return "llama-server"
	default:
		return ""
	}
}

func driverFacts(node domain.Node) map[string]string {
	out := map[string]string{}
	for _, acc := range node.Accelerators {
		if acc.ComputeCapability != "" {
			out[acc.Vendor+".compute_capability"] = acc.ComputeCapability
		}
		if acc.ArchFamily != "" {
			out[acc.Vendor+".arch_family"] = acc.ArchFamily
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func largestMemory(node domain.Node) int {
	total := 0
	for _, acc := range node.Accelerators {
		if acc.VRAMTotalMB > total {
			total = acc.VRAMTotalMB
		}
	}
	return total
}

func hasVLLMUtil(args []string) bool {
	for i, arg := range args {
		if strings.HasPrefix(arg, "--gpu-memory-utilization=") {
			return true
		}
		if arg == "--gpu-memory-utilization" && i+1 < len(args) {
			return true
		}
	}
	return false
}

var _ ports.HostDetector = Detector{}
var _ ports.EngineDetector = Detector{}
