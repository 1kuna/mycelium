package modelcompat

import (
	"strings"
	"testing"
	"time"

	"mycelium/internal/catalog"
	"mycelium/internal/domain"
	"mycelium/internal/enginecompat"
)

func TestAdvisorReportsHostBackendArtifactCompatibility(t *testing.T) {
	macHost := domain.HostFacts{NodeID: "mac", OS: "darwin", Arch: "arm64", Platform: "darwin/arm64", Accelerators: []domain.Accelerator{{Vendor: "apple"}}}
	b70Host := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	sparkHost := domain.HostFacts{NodeID: "spark", OS: "linux", Arch: "arm64", Platform: "linux/arm64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	macMLX := keyedProfile(domain.EngineProfile{ID: "mac-mlx", Backend: domain.BackendMLX, DisplayName: "MLX", BinaryPath: "mlx-server", Version: "1", Ready: true, SupportedModels: []string{"mlx"}}, macHost, "")
	b70OV := keyedProfile(domain.EngineProfile{ID: "b70-openvino", Backend: domain.BackendOpenVINO, DisplayName: "OpenVINO", BinaryPath: "openvino-server", Version: "1", Ready: true, SupportedModels: []string{catalog.FormatOpenVINOIR}}, b70Host, "")
	sparkVLLM := keyedProfile(domain.EngineProfile{ID: "spark-vllm", Backend: domain.BackendVLLM, DisplayName: "vLLM", BinaryPath: "vllm", Version: "1", Ready: true, SupportedModels: []string{catalog.FormatHFTransformers}}, sparkHost, "")

	report, err := Advise(Request{
		Model: "logical/gemma",
		Artifacts: []Artifact{
			{Ref: "hf://logical/gemma", Format: "safetensors"},
			{Ref: "/models/gemma-mlx", Format: "mlx"},
			{Ref: "/models/gemma-ov", Format: catalog.FormatOpenVINOIR},
			{Ref: "/models/gemma.gguf", Format: catalog.FormatGGUF},
		},
		Plans: []domain.BootstrapPlan{
			{ID: "plan-mac", CreatedAt: time.Unix(1, 0).UTC(), Host: macHost, ResultingProfiles: []domain.EngineProfile{macMLX}},
			{ID: "plan-b70", CreatedAt: time.Unix(1, 0).UTC(), Host: b70Host, ResultingProfiles: []domain.EngineProfile{b70OV}},
			{ID: "plan-spark", CreatedAt: time.Unix(1, 0).UTC(), Host: sparkHost, ResultingProfiles: []domain.EngineProfile{sparkVLLM}},
		},
		Profiles: []domain.EngineProfile{macMLX, b70OV, sparkVLLM},
		LegacyPresets: []domain.Preset{{
			ID:       "manual",
			NodeID:   "legacy-manual",
			Backend:  domain.BackendLlamaCpp,
			ModelRef: "/manual/model",
		}},
	})
	if err != nil {
		t.Fatalf("Advise: %v", err)
	}
	if report.Model != "logical/gemma" || len(report.Artifacts) != 4 {
		t.Fatalf("report header = %+v", report)
	}
	requireRow(t, report.Rows, "mac", "/models/gemma-mlx", domain.BackendMLX, StatusCanRunNow, "")
	requireRow(t, report.Rows, "b70", "/models/gemma-ov", domain.BackendOpenVINO, StatusCanRunNow, "")
	requireRow(t, report.Rows, "spark", "hf://logical/gemma", domain.BackendVLLM, StatusCanRunNow, "")
	row := requireRow(t, report.Rows, "mac", "/models/gemma.gguf", domain.BackendMLX, StatusNeedsArtifactFormat, "mlx")
	if !strings.Contains(row.Reason, "mlx required") {
		t.Fatalf("mac gguf advice = %+v", row)
	}
	row = requireRow(t, report.Rows, "legacy-manual", "/models/gemma-mlx", domain.BackendLlamaCpp, StatusLegacyConfigUnverified, "")
	if !strings.Contains(row.Reason, "manual preset") {
		t.Fatalf("legacy row = %+v", row)
	}
	if got := artifactByRef(report.Artifacts, "/models/gemma-ov").Scope; got != "host_specific" {
		t.Fatalf("openvino artifact scope = %q", got)
	}
	if got := artifactByRef(report.Artifacts, "/models/gemma.gguf").Scope; got != "shared_where_backend_matches" {
		t.Fatalf("gguf artifact scope = %q", got)
	}
}

func TestAdvisorRejectsUnreadyAndMismatchedProfiles(t *testing.T) {
	host := domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}
	unready := keyedProfile(domain.EngineProfile{ID: "openvino", Backend: domain.BackendOpenVINO, DisplayName: "OpenVINO", BinaryPath: "ov", Ready: false, UnreadyReason: "verification failed", SupportedModels: []string{catalog.FormatOpenVINOIR}}, host, "")
	sparkHost := domain.HostFacts{NodeID: "spark", OS: "linux", Arch: "arm64", Platform: "linux/arm64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}
	mismatched := keyedProfile(domain.EngineProfile{ID: "spark-vllm", Backend: domain.BackendVLLM, DisplayName: "vLLM", BinaryPath: "vllm", Ready: true, SupportedModels: []string{catalog.FormatHFTransformers}}, sparkHost, "")
	report, err := Advise(Request{
		Model:     "logical/model",
		Artifacts: []Artifact{{Ref: "/models/model-ov", Format: catalog.FormatOpenVINOIR}, {Ref: "hf://logical/model", Format: catalog.FormatHFTransformers}},
		Plans: []domain.BootstrapPlan{{
			ID:                "plan-b70",
			CreatedAt:         time.Unix(1, 0).UTC(),
			Host:              host,
			ResultingProfiles: []domain.EngineProfile{unready, mismatched},
		}},
		Profiles: []domain.EngineProfile{unready, mismatched},
	})
	if err != nil {
		t.Fatalf("Advise: %v", err)
	}
	row := requireRow(t, report.Rows, "b70", "/models/model-ov", domain.BackendOpenVINO, StatusBlocked, "")
	if !strings.Contains(row.Reason, "verification failed") {
		t.Fatalf("unready row = %+v", row)
	}
	row = requireRow(t, report.Rows, "b70", "hf://logical/model", domain.BackendVLLM, StatusBlocked, "")
	if !strings.Contains(row.Reason, "compatibility_key_mismatch") {
		t.Fatalf("mismatch row = %+v", row)
	}
}

func TestAdvisorRequiresDeclaredSupportedFormats(t *testing.T) {
	host := domain.HostFacts{NodeID: "mac", OS: "darwin", Arch: "arm64", Platform: "darwin/arm64", Accelerators: []domain.Accelerator{{Vendor: "apple"}}}
	profile := keyedProfile(domain.EngineProfile{ID: "mlx", Backend: domain.BackendMLX, DisplayName: "MLX", BinaryPath: "mlx", Ready: true}, host, "")
	report, err := Advise(Request{
		Model:     "logical/model",
		Artifacts: []Artifact{{Ref: "/models/model", Format: "mlx"}},
		Plans:     []domain.BootstrapPlan{{ID: "plan", CreatedAt: time.Unix(1, 0).UTC(), Host: host, ResultingProfiles: []domain.EngineProfile{profile}}},
		Profiles:  []domain.EngineProfile{profile},
	})
	if err != nil {
		t.Fatalf("Advise: %v", err)
	}
	row := requireRow(t, report.Rows, "mac", "/models/model", domain.BackendMLX, StatusNeedsArtifactFormat, "mlx")
	if !strings.Contains(row.Reason, "does not declare supported model formats") {
		t.Fatalf("row = %+v", row)
	}
}

func TestAdvisorCapabilityCatalogCoversMajorEngineFamilies(t *testing.T) {
	plans := []domain.BootstrapPlan{
		{ID: "mac-as", CreatedAt: time.Unix(1, 0).UTC(), Host: domain.HostFacts{NodeID: "mac-as", OS: "darwin", Arch: "arm64", Platform: "darwin/arm64", Accelerators: []domain.Accelerator{{Vendor: "apple"}}}},
		{ID: "mac-intel", CreatedAt: time.Unix(1, 0).UTC(), Host: domain.HostFacts{NodeID: "mac-intel", OS: "darwin", Arch: "amd64", Platform: "darwin/amd64"}},
		{ID: "spark", CreatedAt: time.Unix(1, 0).UTC(), Host: domain.HostFacts{NodeID: "spark", OS: "linux", Arch: "arm64", Platform: "linux/arm64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}},
		{ID: "nvidia-x64", CreatedAt: time.Unix(1, 0).UTC(), Host: domain.HostFacts{NodeID: "nvidia-x64", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}},
		{ID: "b70", CreatedAt: time.Unix(1, 0).UTC(), Host: domain.HostFacts{NodeID: "b70", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "intel"}}}},
		{ID: "amd", CreatedAt: time.Unix(1, 0).UTC(), Host: domain.HostFacts{NodeID: "amd", OS: "linux", Arch: "amd64", Platform: "linux/amd64", Accelerators: []domain.Accelerator{{Vendor: "amd"}}}},
	}
	report, err := Advise(Request{
		Model: "logical/fleet-model",
		Artifacts: []Artifact{
			{Ref: "/models/model.gguf", Format: catalog.FormatGGUF},
			{Ref: "hf://org/model", Format: "safetensors"},
			{Ref: "/models/model-mlx", Format: "mlx"},
			{Ref: "/models/model-ov", Format: catalog.FormatOpenVINOIR},
		},
		Plans: plans,
	})
	if err != nil {
		t.Fatalf("Advise: %v", err)
	}

	requireRow(t, report.Rows, "mac-as", "/models/model-mlx", domain.BackendMLX, StatusCanRunAfterEngineSetup, "")
	requireRow(t, report.Rows, "mac-as", "/models/model.gguf", domain.BackendLlamaCpp, StatusCanRunAfterEngineSetup, "")
	row := requireRow(t, report.Rows, "mac-intel", "/models/model-mlx", domain.BackendMLX, StatusNotSupported, "mlx")
	if !strings.Contains(row.Reason, "MLX is not supported on Intel macOS") {
		t.Fatalf("intel mac mlx row = %+v", row)
	}
	row = requireRow(t, report.Rows, "mac-intel", "/models/model.gguf", domain.BackendLlamaCpp, StatusNotSupported, catalog.FormatGGUF)
	if !strings.Contains(row.Reason, "CPU fallback requires explicit opt-in") {
		t.Fatalf("intel mac llamacpp row = %+v", row)
	}
	requireRow(t, report.Rows, "spark", "hf://org/model", domain.BackendVLLM, StatusCanRunAfterEngineSetup, "")
	row = requireRow(t, report.Rows, "spark", "hf://org/model", domain.BackendSGLang, StatusCanRunAfterEngineSetup, "")
	if !strings.Contains(row.Reason, "adapter/profile must exist") {
		t.Fatalf("spark sglang row = %+v", row)
	}
	requireRow(t, report.Rows, "spark", "/models/model.gguf", domain.BackendLlamaCpp, StatusCanRunAfterEngineSetup, "")
	requireRow(t, report.Rows, "nvidia-x64", "hf://org/model", domain.BackendVLLM, StatusCanRunAfterEngineSetup, "")
	requireRow(t, report.Rows, "b70", "/models/model-ov", domain.BackendOpenVINO, StatusCanRunAfterEngineSetup, "")
	requireRow(t, report.Rows, "b70", "/models/model.gguf", domain.BackendLlamaCpp, StatusCanRunAfterEngineSetup, "")
	row = requireRow(t, report.Rows, "b70", "hf://org/model", domain.BackendVLLM, StatusNotSupported, catalog.FormatHFTransformers+",safetensors")
	if !strings.Contains(row.Reason, "Intel XPU/vLLM-compatible wrappers") {
		t.Fatalf("b70 vllm row = %+v", row)
	}
	requireRow(t, report.Rows, "amd", "/models/model.gguf", domain.BackendLlamaCpp, StatusCanRunAfterEngineSetup, "")
	row = requireRow(t, report.Rows, "amd", "hf://org/model", domain.BackendVLLM, StatusNotSupported, catalog.FormatHFTransformers+",safetensors")
	if !strings.Contains(row.Reason, "ROCm vLLM combinations require a saved verified profile") {
		t.Fatalf("amd vllm row = %+v", row)
	}
	row = requireRow(t, report.Rows, "mac-as", "hf://org/model", domain.BackendMLX, StatusNeedsArtifactFormat, "mlx")
	if !strings.Contains(row.Reason, "requires mlx artifacts") {
		t.Fatalf("mac needs mlx row = %+v", row)
	}
	row = requireRow(t, report.Rows, "b70", "/models/model-ov", domain.BackendOpenVINO, StatusCanRunAfterEngineSetup, "")
	if !strings.Contains(row.Multimodal, "model metadata") {
		t.Fatalf("openvino multimodal caveat = %+v", row)
	}
}

func keyedProfile(profile domain.EngineProfile, host domain.HostFacts, modelFormat string) domain.EngineProfile {
	profile.CompatibilityKey = enginecompat.HostProfileKey(host, profile, modelFormat)
	return profile
}

func requireRow(t *testing.T, rows []Row, host, artifact string, backend domain.Backend, status, needed string) Row {
	t.Helper()
	for _, row := range rows {
		if row.HostID == host && row.ArtifactRef == artifact && row.Backend == backend && row.Status == status {
			if needed != "" && row.NeededFormat != needed {
				t.Fatalf("row needed format = %q, want %q row=%+v", row.NeededFormat, needed, row)
			}
			return row
		}
	}
	t.Fatalf("missing row host=%s artifact=%s backend=%s status=%s rows=%+v", host, artifact, backend, status, rows)
	return Row{}
}

func artifactByRef(artifacts []Artifact, ref string) Artifact {
	for _, artifact := range artifacts {
		if artifact.Ref == ref {
			return artifact
		}
	}
	return Artifact{}
}
