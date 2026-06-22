package enginecompat

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"mycelium/internal/domain"
)

func HostProfileKey(host domain.HostFacts, profile domain.EngineProfile, modelFormat string) domain.CompatibilityKey {
	osName, arch := splitPlatform(host.OS, host.Arch, host.Platform)
	vendor := acceleratorVendor(host.Accelerators)
	return domain.CompatibilityKey{
		OS:                 osName,
		CPUArch:            arch,
		AcceleratorVendor:  vendor,
		AcceleratorRuntime: acceleratorRuntime(osName, vendor, profile.Backend),
		DriverVersion:      driverVersion(host.DriverFacts, vendor),
		EngineDistribution: engineDistribution(profile),
		EngineVersion:      profile.Version,
		Backend:            profile.Backend,
		ModelFormat:        modelFormat,
	}
}

func NodeProfileKey(node domain.Node, profile domain.EngineProfile, modelFormat string) domain.CompatibilityKey {
	vendor := acceleratorVendor(node.Accelerators)
	return domain.CompatibilityKey{
		OS:                 node.OS,
		CPUArch:            node.Arch,
		AcceleratorVendor:  vendor,
		AcceleratorRuntime: acceleratorRuntime(node.OS, vendor, profile.Backend),
		EngineDistribution: engineDistribution(profile),
		EngineVersion:      profile.Version,
		Backend:            profile.Backend,
		ModelFormat:        modelFormat,
	}
}

func ProfileMatchesHost(profile domain.EngineProfile, host domain.HostFacts, modelFormat string) (domain.CompatibilityKey, bool, string) {
	return profileMatchesKey(profile, HostProfileKey(host, profile, modelFormat))
}

func ProfileMatchesNode(profile domain.EngineProfile, node domain.Node, modelFormat string) (domain.CompatibilityKey, bool, string) {
	return profileMatchesKey(profile, NodeProfileKey(node, profile, modelFormat))
}

func ProfileMatchesKey(profile domain.EngineProfile, target domain.CompatibilityKey) (domain.CompatibilityKey, bool, string) {
	if target.EngineDistribution == "" {
		target.EngineDistribution = engineDistribution(profile)
	}
	if target.EngineVersion == "" {
		target.EngineVersion = profile.Version
	}
	if target.Backend == "" {
		target.Backend = profile.Backend
	}
	return profileMatchesKey(profile, target)
}

func KeyComplete(key domain.CompatibilityKey) bool {
	return key.OS != "" &&
		key.CPUArch != "" &&
		key.AcceleratorVendor != "" &&
		key.AcceleratorRuntime != "" &&
		key.EngineDistribution != "" &&
		key.Backend != ""
}

func IncompleteReason(profile domain.EngineProfile) string {
	if profile.CompatibilityKey == (domain.CompatibilityKey{}) {
		return fmt.Sprintf("compatibility_key_incomplete: engine profile %q has no saved compatibility key", profile.ID)
	}
	var missing []string
	key := profile.CompatibilityKey
	if key.OS == "" {
		missing = append(missing, "os")
	}
	if key.CPUArch == "" {
		missing = append(missing, "cpu_arch")
	}
	if key.AcceleratorVendor == "" {
		missing = append(missing, "accelerator_vendor")
	}
	if key.AcceleratorRuntime == "" {
		missing = append(missing, "accelerator_runtime")
	}
	if key.EngineDistribution == "" {
		missing = append(missing, "engine_distribution")
	}
	if key.Backend == "" {
		missing = append(missing, "backend")
	}
	return fmt.Sprintf("compatibility_key_incomplete: engine profile %q missing %s", profile.ID, strings.Join(missing, ","))
}

func profileMatchesKey(profile domain.EngineProfile, target domain.CompatibilityKey) (domain.CompatibilityKey, bool, string) {
	key := profile.CompatibilityKey
	if !KeyComplete(key) {
		return target, false, IncompleteReason(profile)
	}
	if target.OS == "" || target.CPUArch == "" || target.AcceleratorVendor == "" || target.AcceleratorRuntime == "" {
		return target, false, "compatibility_key_incomplete: target host compatibility facts are incomplete"
	}
	for _, check := range []struct {
		field string
		have  string
		want  string
	}{
		{"os", target.OS, key.OS},
		{"cpu_arch", target.CPUArch, key.CPUArch},
		{"accelerator_vendor", target.AcceleratorVendor, key.AcceleratorVendor},
		{"accelerator_runtime", target.AcceleratorRuntime, key.AcceleratorRuntime},
		{"driver_version", target.DriverVersion, key.DriverVersion},
		{"engine_distribution", target.EngineDistribution, key.EngineDistribution},
		{"engine_version", target.EngineVersion, key.EngineVersion},
		{"backend", string(target.Backend), string(key.Backend)},
		{"model_format", target.ModelFormat, key.ModelFormat},
	} {
		if check.want != "" && check.have != "" && check.have != check.want {
			return target, false, fmt.Sprintf("compatibility_key_mismatch: %s saved=%q target=%q", check.field, check.want, check.have)
		}
	}
	return target, true, ""
}

func splitPlatform(osName, arch, platform string) (string, string) {
	if (osName == "" || arch == "") && platform != "" {
		parts := strings.SplitN(platform, "/", 2)
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

func acceleratorVendor(accelerators []domain.Accelerator) string {
	if len(accelerators) == 0 {
		return "cpu"
	}
	seen := map[string]struct{}{}
	for _, acc := range accelerators {
		vendor := strings.TrimSpace(acc.Vendor)
		if vendor == "" {
			continue
		}
		seen[vendor] = struct{}{}
	}
	if len(seen) == 0 {
		return "cpu"
	}
	if len(seen) == 1 {
		for vendor := range seen {
			return vendor
		}
	}
	return "mixed"
}

func acceleratorRuntime(osName, vendor string, backend domain.Backend) string {
	switch vendor {
	case "apple":
		if osName == "darwin" {
			return "metal"
		}
	case "nvidia":
		return "cuda"
	case "intel":
		switch backend {
		case domain.BackendLlamaCpp:
			return "sycl"
		case domain.BackendVLLM:
			return "level-zero"
		case domain.BackendOpenVINO:
			return "openvino"
		default:
			return "level-zero"
		}
	case "amd":
		return "rocm"
	case "cpu":
		return "cpu"
	case "mixed":
		return "mixed"
	}
	return ""
}

func driverVersion(facts map[string]string, vendor string) string {
	for _, key := range []string{
		vendor + ".driver_version",
		vendor + ".driver",
		vendor + ".runtime_version",
		"driver_version",
		"runtime_version",
	} {
		if value := strings.TrimSpace(facts[key]); value != "" {
			return value
		}
	}
	return ""
}

func engineDistribution(profile domain.EngineProfile) string {
	var parts []string
	if profile.DisplayName != "" {
		parts = append(parts, profile.DisplayName)
	}
	if profile.BinaryPath != "" {
		parts = append(parts, filepath.Base(profile.BinaryPath))
	}
	if len(parts) == 0 && profile.Backend != "" {
		parts = append(parts, string(profile.Backend))
	}
	sort.Strings(parts)
	return strings.Join(parts, ":")
}
