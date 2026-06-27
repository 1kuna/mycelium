package enginecompat

import (
	"strings"
	"testing"

	"mycelium/internal/domain"
)

func TestHostAndNodeProfileKeysInferCompatibilityFacts(t *testing.T) {
	profile := domain.EngineProfile{
		ID:          "vllm-spark",
		Backend:     domain.BackendVLLM,
		DisplayName: "vLLM",
		BinaryPath:  "/opt/mycelium/bin/vllm-wrapper",
		Version:     "0.10.0",
	}
	hostKey := HostProfileKey(domain.HostFacts{
		Platform: "linux/arm64",
		Accelerators: []domain.Accelerator{{
			Vendor: "nvidia",
		}},
		DriverFacts: map[string]string{
			"nvidia.driver": "570.1",
		},
	}, profile, "safetensors")
	if hostKey.OS != "linux" || hostKey.CPUArch != "arm64" || hostKey.AcceleratorVendor != "nvidia" || hostKey.AcceleratorRuntime != "cuda" {
		t.Fatalf("host key hardware facts = %+v", hostKey)
	}
	if hostKey.DriverVersion != "570.1" || hostKey.EngineDistribution != "vLLM:vllm-wrapper" || hostKey.EngineVersion != "0.10.0" || hostKey.Backend != domain.BackendVLLM || hostKey.ModelFormat != "safetensors" {
		t.Fatalf("host key engine facts = %+v", hostKey)
	}

	nodeKey := NodeProfileKey(domain.Node{
		OS:   "linux",
		Arch: "amd64",
		Accelerators: []domain.Accelerator{{
			Vendor: "intel",
		}},
	}, domain.EngineProfile{Backend: domain.BackendOpenVINO, BinaryPath: "/usr/bin/openvino"}, "openvino-ir")
	if nodeKey.AcceleratorVendor != "intel" || nodeKey.AcceleratorRuntime != "openvino" || nodeKey.EngineDistribution != "openvino" {
		t.Fatalf("node key = %+v", nodeKey)
	}
}

func TestProfileMatchingReportsIncompleteMismatchAndDefaults(t *testing.T) {
	profile := domain.EngineProfile{ID: "empty"}
	if KeyComplete(profile.CompatibilityKey) {
		t.Fatal("empty compatibility key reported complete")
	}
	if _, ok, reason := ProfileMatchesKey(profile, domain.CompatibilityKey{OS: "linux"}); ok || !strings.Contains(reason, "has no saved compatibility key") {
		t.Fatalf("empty profile match = ok %v reason %q", ok, reason)
	}

	profile = domain.EngineProfile{
		ID:          "llamacpp-metal",
		Backend:     domain.BackendLlamaCpp,
		DisplayName: "llama.cpp",
		BinaryPath:  "/opt/bin/llama-server",
		Version:     "b5000",
		CompatibilityKey: domain.CompatibilityKey{
			OS:                 "darwin",
			CPUArch:            "arm64",
			AcceleratorVendor:  "apple",
			AcceleratorRuntime: "metal",
			EngineDistribution: "llama-server:llama.cpp",
			EngineVersion:      "b5000",
			Backend:            domain.BackendLlamaCpp,
			ModelFormat:        "gguf",
		},
	}
	target, ok, reason := ProfileMatchesKey(profile, domain.CompatibilityKey{
		OS:                 "darwin",
		CPUArch:            "arm64",
		AcceleratorVendor:  "apple",
		AcceleratorRuntime: "metal",
		ModelFormat:        "gguf",
	})
	if !ok || reason != "" {
		t.Fatalf("profile should match target=%+v ok=%v reason=%q", target, ok, reason)
	}
	if target.EngineDistribution != "llama-server:llama.cpp" || target.EngineVersion != "b5000" || target.Backend != domain.BackendLlamaCpp {
		t.Fatalf("target defaults were not filled = %+v", target)
	}

	_, ok, reason = ProfileMatchesKey(profile, domain.CompatibilityKey{
		OS:                 "darwin",
		CPUArch:            "amd64",
		AcceleratorVendor:  "apple",
		AcceleratorRuntime: "metal",
		EngineDistribution: "llama-server:llama.cpp",
		Backend:            domain.BackendLlamaCpp,
		ModelFormat:        "gguf",
	})
	if ok || !strings.Contains(reason, `cpu_arch saved="arm64" target="amd64"`) {
		t.Fatalf("mismatch ok=%v reason=%q", ok, reason)
	}

	incompleteTarget := domain.CompatibilityKey{OS: "darwin", CPUArch: "arm64"}
	if _, ok, reason := ProfileMatchesKey(profile, incompleteTarget); ok || !strings.Contains(reason, "target host compatibility facts are incomplete") {
		t.Fatalf("incomplete target ok=%v reason=%q", ok, reason)
	}
}

func TestIncompleteReasonListsMissingSavedKeyFields(t *testing.T) {
	profile := domain.EngineProfile{
		ID: "partial",
		CompatibilityKey: domain.CompatibilityKey{
			OS:                 "linux",
			CPUArch:            "amd64",
			AcceleratorRuntime: "cuda",
			EngineDistribution: "vllm",
		},
	}
	reason := IncompleteReason(profile)
	for _, want := range []string{"partial", "accelerator_vendor", "backend"} {
		if !strings.Contains(reason, want) {
			t.Fatalf("reason %q missing %q", reason, want)
		}
	}
	if strings.Contains(reason, "os") || strings.Contains(reason, "cpu_arch") {
		t.Fatalf("reason listed present fields: %q", reason)
	}
}

func TestProfileMatchesHostAndNodeUseDerivedRuntimeFacts(t *testing.T) {
	profile := domain.EngineProfile{
		ID:      "openvino",
		Backend: domain.BackendOpenVINO,
		Version: "2026.0",
		CompatibilityKey: domain.CompatibilityKey{
			OS:                 "linux",
			CPUArch:            "amd64",
			AcceleratorVendor:  "cpu",
			AcceleratorRuntime: "cpu",
			EngineDistribution: "openvino",
			EngineVersion:      "2026.0",
			Backend:            domain.BackendOpenVINO,
			ModelFormat:        "openvino-ir",
		},
	}
	if _, ok, reason := ProfileMatchesHost(profile, domain.HostFacts{Platform: "linux/amd64"}, "openvino-ir"); !ok || reason != "" {
		t.Fatalf("host match ok=%v reason=%q", ok, reason)
	}
	if _, ok, reason := ProfileMatchesNode(profile, domain.Node{OS: "linux", Arch: "amd64"}, "openvino-ir"); !ok || reason != "" {
		t.Fatalf("node match ok=%v reason=%q", ok, reason)
	}
}

func TestRuntimeAndDistributionFallbacks(t *testing.T) {
	for _, tt := range []struct {
		name    string
		host    domain.HostFacts
		backend domain.Backend
		want    string
	}{
		{name: "apple metal", host: domain.HostFacts{OS: "darwin", Arch: "arm64", Accelerators: []domain.Accelerator{{Vendor: "apple"}}}, backend: domain.BackendMLX, want: "metal"},
		{name: "intel vllm", host: domain.HostFacts{OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}, backend: domain.BackendVLLM, want: "level-zero"},
		{name: "intel llamacpp", host: domain.HostFacts{OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}, backend: domain.BackendLlamaCpp, want: "sycl"},
		{name: "amd rocm", host: domain.HostFacts{OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "amd"}}}, backend: domain.BackendLlamaCpp, want: "rocm"},
		{name: "mixed", host: domain.HostFacts{OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "amd"}, {Vendor: "nvidia"}}}, backend: domain.BackendLlamaCpp, want: "mixed"},
		{name: "blank vendor cpu", host: domain.HostFacts{OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: " "}}}, backend: domain.BackendOpenVINO, want: "cpu"},
		{name: "unknown vendor", host: domain.HostFacts{OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "mystery"}}}, backend: domain.BackendCustom, want: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key := HostProfileKey(tt.host, domain.EngineProfile{Backend: tt.backend}, "")
			if key.AcceleratorRuntime != tt.want {
				t.Fatalf("runtime = %q, want %q in key %+v", key.AcceleratorRuntime, tt.want, key)
			}
		})
	}

	key := HostProfileKey(domain.HostFacts{OS: "linux", Arch: "amd64", DriverFacts: map[string]string{"runtime_version": "1.2.3"}}, domain.EngineProfile{Backend: domain.BackendCustom}, "")
	if key.DriverVersion != "1.2.3" || key.EngineDistribution != "custom" {
		t.Fatalf("fallback key = %+v", key)
	}
}
