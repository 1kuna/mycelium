package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"mycelium/internal/domain"
)

const (
	FormatGGUF           = "gguf"
	FormatHFTransformers = "hf-transformers"
	FormatOpenVINOIR     = "openvino-ir"
)

type DiscoveryRequest struct {
	HostID  string
	Roots   []string
	Engines []domain.EngineProfile
}

type ModelCandidate struct {
	HostID        string                 `json:"host_id,omitempty"`
	Name          string                 `json:"name"`
	Path          string                 `json:"path"`
	Format        string                 `json:"format"`
	SizeMB        int                    `json:"size_mb,omitempty"`
	ModelType     string                 `json:"model_type,omitempty"`
	Architectures []string               `json:"architectures,omitempty"`
	ContextLength int                    `json:"context_length,omitempty"`
	Capabilities  []domain.Capability    `json:"capabilities,omitempty"`
	Metadata      map[string]string      `json:"metadata,omitempty"`
	Backends      []BackendCompatibility `json:"backends,omitempty"`
}

type BackendCompatibility struct {
	Backend domain.Backend `json:"backend"`
	Ready   bool           `json:"ready"`
	Reason  string         `json:"reason,omitempty"`
}

type modelConfigFile struct {
	ModelType               string          `json:"model_type"`
	Architectures           []string        `json:"architectures"`
	MaxPositionEmbeddings   int             `json:"max_position_embeddings"`
	ImageTokenID            int             `json:"image_token_id"`
	VideoTokenID            int             `json:"video_token_id"`
	VisionConfig            json.RawMessage `json:"vision_config"`
	TextConfig              textConfigFile  `json:"text_config"`
	ProjectorHiddenAct      string          `json:"projector_hidden_act"`
	VisionFeatureLayer      int             `json:"vision_feature_layer"`
	VisionFeatureSelect     string          `json:"vision_feature_select_strategy"`
	MultimodalProjectorBias bool            `json:"multimodal_projector_bias"`
}

type textConfigFile struct {
	MaxPositionEmbeddings int `json:"max_position_embeddings"`
}

func DiscoverCandidates(ctx context.Context, req DiscoveryRequest) ([]ModelCandidate, error) {
	if len(req.Roots) == 0 {
		return nil, fmt.Errorf("at least one discovery root is required")
	}
	engines := engineMap(req.Engines)
	var candidates []ModelCandidate
	for _, root := range req.Roots {
		if root == "" {
			return nil, fmt.Errorf("discovery root is required")
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.IsDir() {
				candidate, ok, err := discoverModelDir(req.HostID, path, engines)
				if err != nil {
					return err
				}
				if ok {
					candidates = append(candidates, candidate)
					if path != root {
						return filepath.SkipDir
					}
				}
				return nil
			}
			if isGGUFModel(entry.Name()) {
				candidate, err := discoverGGUF(req.HostID, path, engines)
				if err != nil {
					return err
				}
				candidates = append(candidates, candidate)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].HostID != candidates[j].HostID {
			return candidates[i].HostID < candidates[j].HostID
		}
		return candidates[i].Path < candidates[j].Path
	})
	return candidates, nil
}

func discoverModelDir(hostID, path string, engines map[domain.Backend]domain.EngineProfile) (ModelCandidate, bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return ModelCandidate{}, false, err
	}
	names := map[string]fs.DirEntry{}
	for _, entry := range entries {
		names[entry.Name()] = entry
	}
	if _, ok := names["openvino_language_model.xml"]; ok {
		if _, ok := names["config.json"]; ok {
			return discoverConfiguredDir(hostID, path, FormatOpenVINOIR, domain.BackendOpenVINO, engines)
		}
	}
	if _, ok := names["config.json"]; ok && hasSuffixEntry(names, ".safetensors") {
		return discoverConfiguredDir(hostID, path, FormatHFTransformers, domain.BackendVLLM, engines)
	}
	return ModelCandidate{}, false, nil
}

func discoverConfiguredDir(hostID, path, format string, required domain.Backend, engines map[domain.Backend]domain.EngineProfile) (ModelCandidate, bool, error) {
	cfg, err := readModelConfig(filepath.Join(path, "config.json"))
	if err != nil {
		return ModelCandidate{}, false, err
	}
	sizeMB, err := dirSizeMB(path)
	if err != nil {
		return ModelCandidate{}, false, err
	}
	vision := hasVisionConfig(cfg)
	metadata := map[string]string{}
	if cfg.ImageTokenID != 0 {
		metadata["image_token_id"] = fmt.Sprint(cfg.ImageTokenID)
	}
	if cfg.VideoTokenID != 0 {
		metadata["video_token_id"] = fmt.Sprint(cfg.VideoTokenID)
	}
	return ModelCandidate{
		HostID:        hostID,
		Name:          filepath.Base(path),
		Path:          path,
		Format:        format,
		SizeMB:        sizeMB,
		ModelType:     cfg.ModelType,
		Architectures: append([]string(nil), cfg.Architectures...),
		ContextLength: contextLength(cfg),
		Capabilities:  capabilities(vision),
		Metadata:      metadataOrNil(metadata),
		Backends:      []BackendCompatibility{compatibility(required, format, engines)},
	}, true, nil
}

func discoverGGUF(hostID, path string, engines map[domain.Backend]domain.EngineProfile) (ModelCandidate, error) {
	info, err := os.Stat(path)
	if err != nil {
		return ModelCandidate{}, err
	}
	return ModelCandidate{
		HostID:       hostID,
		Name:         strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)),
		Path:         path,
		Format:       FormatGGUF,
		SizeMB:       bytesToMB(info.Size()),
		Capabilities: capabilities(hasSiblingMMProj(path)),
		Backends:     []BackendCompatibility{compatibility(domain.BackendLlamaCpp, FormatGGUF, engines)},
	}, nil
}

func compatibility(required domain.Backend, format string, engines map[domain.Backend]domain.EngineProfile) BackendCompatibility {
	engine, ok := engines[required]
	if !ok {
		return BackendCompatibility{Backend: required, Reason: string(required) + " runtime is not configured"}
	}
	if !engine.Ready {
		reason := engine.UnreadyReason
		if reason == "" {
			reason = string(required) + " runtime is not ready"
		}
		return BackendCompatibility{Backend: required, Reason: reason}
	}
	if len(engine.SupportedModels) > 0 && !contains(engine.SupportedModels, format) {
		return BackendCompatibility{Backend: required, Reason: string(required) + " does not advertise " + format + " support"}
	}
	return BackendCompatibility{Backend: required, Ready: true}
}

func engineMap(engines []domain.EngineProfile) map[domain.Backend]domain.EngineProfile {
	out := map[domain.Backend]domain.EngineProfile{}
	for _, engine := range engines {
		if engine.Backend == "" {
			continue
		}
		out[engine.Backend] = engine
	}
	return out
}

func readModelConfig(path string) (modelConfigFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return modelConfigFile{}, err
	}
	var cfg modelConfigFile
	if err := json.Unmarshal(data, &cfg); err != nil {
		return modelConfigFile{}, err
	}
	return cfg, nil
}

func contextLength(cfg modelConfigFile) int {
	if cfg.TextConfig.MaxPositionEmbeddings != 0 {
		return cfg.TextConfig.MaxPositionEmbeddings
	}
	return cfg.MaxPositionEmbeddings
}

func capabilities(vision bool) []domain.Capability {
	caps := []domain.Capability{domain.CapabilityChat}
	if vision {
		caps = append(caps, domain.CapabilityVision)
	}
	return caps
}

func hasVisionConfig(cfg modelConfigFile) bool {
	value := strings.TrimSpace(string(cfg.VisionConfig))
	return value != "" && value != "null"
}

func hasSiblingMMProj(path string) bool {
	dir := filepath.Dir(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		name := strings.ToLower(entry.Name())
		if !entry.IsDir() && strings.HasSuffix(name, ".gguf") && strings.Contains(name, "mmproj") {
			return true
		}
	}
	return false
}

func isGGUFModel(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".gguf") && !strings.Contains(lower, "mmproj")
}

func hasSuffixEntry(entries map[string]fs.DirEntry, suffix string) bool {
	for name, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(strings.ToLower(name), suffix) {
			return true
		}
	}
	return false
}

func dirSizeMB(path string) (int, error) {
	var total int64
	if err := filepath.WalkDir(path, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	}); err != nil {
		return 0, err
	}
	return bytesToMB(total), nil
}

func bytesToMB(size int64) int {
	if size <= 0 {
		return 0
	}
	return int((size + 1024*1024 - 1) / (1024 * 1024))
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func metadataOrNil(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}
