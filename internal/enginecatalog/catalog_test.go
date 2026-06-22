package enginecatalog

import (
	"strings"
	"testing"

	"mycelium/internal/catalog"
	"mycelium/internal/domain"
)

func TestDefaultCatalogCoversDoc04EngineFamilies(t *testing.T) {
	caps := Default()
	requireCapability(t, caps, domain.BackendLlamaCpp, []string{catalog.FormatGGUF})
	requireCapability(t, caps, domain.BackendVLLM, []string{catalog.FormatHFTransformers, FormatSafetensors})
	requireCapability(t, caps, domain.BackendSGLang, []string{catalog.FormatHFTransformers, FormatSafetensors})
	requireCapability(t, caps, domain.BackendMLX, []string{FormatMLX})
	requireCapability(t, caps, domain.BackendOpenVINO, []string{catalog.FormatOpenVINOIR})
	if _, ok := findCapability(caps, domain.BackendCustom); ok {
		t.Fatal("custom/native process should not be assumed by the capability catalog")
	}
}

func TestCapabilityHostAssessmentKeepsPlatformConstraintsExplicit(t *testing.T) {
	caps := Default()
	mlx := mustCapability(t, caps, domain.BackendMLX)
	if support := mlx.AssessHost(domain.HostFacts{OS: "darwin", Arch: "arm64", Platform: "darwin/arm64", Accelerators: []domain.Accelerator{{Vendor: "apple"}}}); !support.Supported {
		t.Fatalf("Apple Silicon MLX support = %+v", support)
	}
	if support := mlx.AssessHost(domain.HostFacts{OS: "darwin", Arch: "amd64", Platform: "darwin/amd64"}); support.Supported || !strings.Contains(support.Reason, "Intel macOS") {
		t.Fatalf("Intel Mac MLX support = %+v", support)
	}
	vllm := mustCapability(t, caps, domain.BackendVLLM)
	if support := vllm.AssessHost(domain.HostFacts{OS: "linux", Arch: "arm64", Platform: "linux/arm64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}); !support.Supported || !strings.Contains(support.Reason, "ARM64") {
		t.Fatalf("Spark vLLM support = %+v", support)
	}
	if support := vllm.AssessHost(domain.HostFacts{OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}); support.Supported || !strings.Contains(support.Reason, "Intel XPU") {
		t.Fatalf("Intel vLLM support = %+v", support)
	}
	openvino := mustCapability(t, caps, domain.BackendOpenVINO)
	if support := openvino.AssessHost(domain.HostFacts{OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}); !support.Supported {
		t.Fatalf("B70 OpenVINO support = %+v", support)
	}
}

func requireCapability(t *testing.T, caps []Capability, backend domain.Backend, formats []string) {
	t.Helper()
	capability := mustCapability(t, caps, backend)
	for _, format := range formats {
		if !capability.SupportsFormat(format) {
			t.Fatalf("%s does not support %s: %+v", backend, format, capability)
		}
	}
}

func mustCapability(t *testing.T, caps []Capability, backend domain.Backend) Capability {
	t.Helper()
	capability, ok := findCapability(caps, backend)
	if !ok {
		t.Fatalf("missing capability for %s", backend)
	}
	return capability
}

func findCapability(caps []Capability, backend domain.Backend) (Capability, bool) {
	for _, capability := range caps {
		if capability.Backend == backend {
			return capability, true
		}
	}
	return Capability{}, false
}
