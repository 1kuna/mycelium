package catalog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/domain"
)

func TestDiscoverCandidatesReportsOpenVINOVisionAssetAndMissingRuntime(t *testing.T) {
	root := t.TempDir()
	modelDir := filepath.Join(root, "gemma-4-31B-it-int4-ov")
	writeFile(t, filepath.Join(modelDir, "openvino_language_model.xml"), "<xml/>")
	writeFile(t, filepath.Join(modelDir, "openvino_language_model.bin"), "weights")
	writeFile(t, filepath.Join(modelDir, "openvino_vision_embeddings_model.xml"), "<xml/>")
	writeFile(t, filepath.Join(modelDir, "config.json"), `{
		"model_type":"gemma4",
		"architectures":["Gemma4ForConditionalGeneration"],
		"image_token_id":258880,
		"video_token_id":258884,
		"text_config":{"max_position_embeddings":262144},
		"vision_config":{"model_type":"gemma4_vision"}
	}`)

	candidates, err := DiscoverCandidates(context.Background(), DiscoveryRequest{
		HostID: "b70",
		Roots:  []string{root},
		Engines: []domain.EngineProfile{{
			Backend:         domain.BackendVLLM,
			Ready:           true,
			SupportedModels: []string{FormatHFTransformers},
		}},
	})
	if err != nil {
		t.Fatalf("DiscoverCandidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %+v", candidates)
	}
	candidate := candidates[0]
	if candidate.HostID != "b70" || candidate.Name != "gemma-4-31B-it-int4-ov" || candidate.Format != FormatOpenVINOIR {
		t.Fatalf("candidate identity = %+v", candidate)
	}
	if candidate.ModelType != "gemma4" || candidate.ContextLength != 262144 || !hasCapability(candidate.Capabilities, domain.CapabilityVision) {
		t.Fatalf("candidate metadata = %+v", candidate)
	}
	if candidate.Metadata["image_token_id"] != "258880" || candidate.Metadata["video_token_id"] != "258884" {
		t.Fatalf("candidate token metadata = %+v", candidate.Metadata)
	}
	if len(candidate.Backends) != 1 || candidate.Backends[0].Backend != domain.BackendOpenVINO || candidate.Backends[0].Ready {
		t.Fatalf("backend compatibility = %+v", candidate.Backends)
	}
	if !strings.Contains(candidate.Backends[0].Reason, "openvino runtime is not configured") {
		t.Fatalf("backend reason = %q", candidate.Backends[0].Reason)
	}
}

func TestDiscoverCandidatesMarksOpenVINOReadyWhenRuntimeSupportsIR(t *testing.T) {
	root := t.TempDir()
	modelDir := filepath.Join(root, "gemma")
	writeFile(t, filepath.Join(modelDir, "openvino_language_model.xml"), "<xml/>")
	writeFile(t, filepath.Join(modelDir, "config.json"), `{"model_type":"gemma4"}`)

	candidates, err := DiscoverCandidates(context.Background(), DiscoveryRequest{
		Roots: []string{root},
		Engines: []domain.EngineProfile{{
			Backend:         domain.BackendOpenVINO,
			Ready:           true,
			SupportedModels: []string{FormatOpenVINOIR},
		}},
	})
	if err != nil {
		t.Fatalf("DiscoverCandidates: %v", err)
	}
	if len(candidates) != 1 || len(candidates[0].Backends) != 1 || !candidates[0].Backends[0].Ready {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestDiscoverCandidatesTreatsGGUFWithMMProjAsVisionLlamaCppCandidate(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "qwen3.5-4b.gguf"), "weights")
	writeFile(t, filepath.Join(root, "mmproj-BF16.gguf"), "projector")

	candidates, err := DiscoverCandidates(context.Background(), DiscoveryRequest{
		Roots: []string{root},
		Engines: []domain.EngineProfile{{
			Backend:         domain.BackendLlamaCpp,
			Ready:           true,
			SupportedModels: []string{FormatGGUF},
		}},
	})
	if err != nil {
		t.Fatalf("DiscoverCandidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %+v", candidates)
	}
	candidate := candidates[0]
	if candidate.Name != "qwen3.5-4b" || candidate.Format != FormatGGUF || !hasCapability(candidate.Capabilities, domain.CapabilityVision) {
		t.Fatalf("candidate = %+v", candidate)
	}
	if len(candidate.Backends) != 1 || candidate.Backends[0].Backend != domain.BackendLlamaCpp || !candidate.Backends[0].Ready {
		t.Fatalf("backend compatibility = %+v", candidate.Backends)
	}
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func hasCapability(caps []domain.Capability, target domain.Capability) bool {
	for _, cap := range caps {
		if cap == target {
			return true
		}
	}
	return false
}
