package enginecatalog

import (
	"fmt"
	"sort"
	"strings"

	"mycelium/internal/catalog"
	"mycelium/internal/domain"
)

const (
	FormatMLX         = "mlx"
	FormatSafetensors = "safetensors"
)

type Capability struct {
	Backend    domain.Backend `json:"backend"`
	Family     string         `json:"family"`
	Formats    []string       `json:"formats"`
	Multimodal string         `json:"multimodal,omitempty"`
	Rules      []PlatformRule `json:"rules"`
}

type PlatformRule struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	Vendor    string `json:"accelerator_vendor"`
	Runtime   string `json:"accelerator_runtime"`
	Supported bool   `json:"supported"`
	Reason    string `json:"reason"`
}

type HostSupport struct {
	Supported bool
	Runtime   string
	Reason    string
}

func Default() []Capability {
	return []Capability{
		{
			Backend:    domain.BackendLlamaCpp,
			Family:     "llama.cpp",
			Formats:    []string{catalog.FormatGGUF},
			Multimodal: "GGUF vision requires a matching mmproj/projector artifact; text GGUF alone is not proof of vision support",
			Rules: []PlatformRule{
				{OS: "darwin", Arch: "arm64", Vendor: "apple", Runtime: "metal", Supported: true, Reason: "llama.cpp Metal on Apple Silicon"},
				{OS: "darwin", Arch: "amd64", Vendor: "cpu", Runtime: "cpu", Reason: "Intel Mac CPU fallback requires explicit opt-in; MLX is Apple Silicon only"},
				{OS: "linux", Arch: "arm64", Vendor: "nvidia", Runtime: "cuda", Supported: true, Reason: "llama.cpp CUDA with an ARM64-compatible build"},
				{OS: "linux", Arch: "amd64", Vendor: "nvidia", Runtime: "cuda", Supported: true, Reason: "llama.cpp CUDA with an x86_64-compatible build"},
				{OS: "linux", Arch: "amd64", Vendor: "intel", Runtime: "sycl", Supported: true, Reason: "SYCL llama.cpp on Intel Arc/Level Zero hosts"},
				{OS: "linux", Arch: "amd64", Vendor: "amd", Runtime: "rocm", Supported: true, Reason: "ROCm-compatible llama.cpp profile, subject to local verification"},
			},
		},
		{
			Backend:    domain.BackendVLLM,
			Family:     "vLLM",
			Formats:    []string{catalog.FormatHFTransformers, FormatSafetensors},
			Multimodal: "multimodal support is model/backend dependent and must be proven by the ready profile or route contract",
			Rules: []PlatformRule{
				{OS: "linux", Arch: "arm64", Vendor: "nvidia", Runtime: "cuda", Supported: true, Reason: "vLLM on Linux NVIDIA ARM64 requires a platform-safe wheel or container image"},
				{OS: "linux", Arch: "amd64", Vendor: "nvidia", Runtime: "cuda", Supported: true, Reason: "vLLM on Linux NVIDIA x86_64 requires matching CUDA wheel/image constraints"},
				{OS: "linux", Arch: "amd64", Vendor: "intel", Runtime: "level-zero", Reason: "Intel XPU/vLLM-compatible wrappers must be detected and saved before they are considered supported"},
				{OS: "linux", Arch: "amd64", Vendor: "amd", Runtime: "rocm", Reason: "ROCm vLLM combinations require a saved verified profile before advisor treats them as supported"},
			},
		},
		{
			Backend:    domain.BackendSGLang,
			Family:     "SGLang",
			Formats:    []string{catalog.FormatHFTransformers, FormatSafetensors},
			Multimodal: "multimodal support is model/backend dependent; SGLang remains planned until an adapter/profile is ready",
			Rules: []PlatformRule{
				{OS: "linux", Arch: "arm64", Vendor: "nvidia", Runtime: "cuda", Supported: true, Reason: "SGLang planning support on Linux NVIDIA ARM64; adapter/profile must exist before launch"},
				{OS: "linux", Arch: "amd64", Vendor: "nvidia", Runtime: "cuda", Supported: true, Reason: "SGLang planning support on Linux NVIDIA x86_64; adapter/profile must exist before launch"},
			},
		},
		{
			Backend:    domain.BackendMLX,
			Family:     "MLX",
			Formats:    []string{FormatMLX},
			Multimodal: "model-family support is determined by the MLX artifact and server profile",
			Rules: []PlatformRule{
				{OS: "darwin", Arch: "arm64", Vendor: "apple", Runtime: "metal", Supported: true, Reason: "MLX on macOS Apple Silicon with an MLX artifact"},
				{OS: "darwin", Arch: "amd64", Vendor: "cpu", Runtime: "cpu", Reason: "MLX is not supported on Intel macOS"},
			},
		},
		{
			Backend:    domain.BackendOpenVINO,
			Family:     "OpenVINO",
			Formats:    []string{catalog.FormatOpenVINOIR},
			Multimodal: "model-family support is explicit in the OpenVINO model metadata and serving wrapper",
			Rules: []PlatformRule{
				{OS: "linux", Arch: "amd64", Vendor: "intel", Runtime: "openvino", Supported: true, Reason: "OpenVINO IR/model folder on Linux Intel CPU/GPU runtime"},
				{OS: "linux", Arch: "amd64", Vendor: "cpu", Runtime: "openvino", Supported: true, Reason: "OpenVINO CPU runtime requires explicit OpenVINO setup and verification"},
			},
		},
	}
}

func (c Capability) AssessHost(host domain.HostFacts) HostSupport {
	osName, arch := hostOSArch(host)
	vendor := hostVendor(host)
	for _, rule := range c.Rules {
		if rule.OS == osName && rule.Arch == arch && rule.Vendor == vendor {
			return HostSupport{Supported: rule.Supported, Runtime: rule.Runtime, Reason: rule.Reason}
		}
	}
	return HostSupport{
		Reason: fmt.Sprintf("%s is not supported for %s/%s with %s accelerator by the engine capability catalog", c.Family, osName, arch, vendor),
	}
}

func (c Capability) SupportsFormat(format string) bool {
	for _, supported := range c.Formats {
		if FormatsCompatible(supported, format) {
			return true
		}
	}
	return false
}

func (c Capability) NeededFormat() string {
	formats := append([]string(nil), c.Formats...)
	sort.Strings(formats)
	return strings.Join(formats, ",")
}

func FormatsCompatible(supported, artifact string) bool {
	supported = NormalizeFormat(supported)
	artifact = NormalizeFormat(artifact)
	if supported == artifact {
		return true
	}
	if supported == catalog.FormatHFTransformers && artifact == FormatSafetensors {
		return true
	}
	if supported == FormatSafetensors && artifact == catalog.FormatHFTransformers {
		return true
	}
	return false
}

func NormalizeFormat(format string) string {
	return strings.ToLower(strings.TrimSpace(format))
}

func hostOSArch(host domain.HostFacts) (string, string) {
	osName := host.OS
	arch := host.Arch
	if (osName == "" || arch == "") && host.Platform != "" {
		parts := strings.SplitN(host.Platform, "/", 2)
		if len(parts) == 2 {
			if osName == "" {
				osName = parts[0]
			}
			if arch == "" {
				arch = parts[1]
			}
		}
	}
	return osName, arch
}

func hostVendor(host domain.HostFacts) string {
	if len(host.Accelerators) == 0 {
		return "cpu"
	}
	seen := map[string]struct{}{}
	for _, acc := range host.Accelerators {
		vendor := strings.TrimSpace(acc.Vendor)
		if vendor != "" {
			seen[vendor] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return "cpu"
	}
	if len(seen) > 1 {
		return "mixed"
	}
	for vendor := range seen {
		return vendor
	}
	return "cpu"
}
