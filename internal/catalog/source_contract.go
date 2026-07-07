package catalog

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/safeid"
)

type SourcePresetRequest struct {
	Source        string
	ID            string
	Model         string
	Backend       domain.Backend
	ContextLength int
	Contract      *domain.ModelSourceContract
	Now           time.Time
}

func (i Installer) MaterializeSourcePreset(ctx context.Context, req SourcePresetRequest) (InstallResult, error) {
	if err := ctx.Err(); err != nil {
		return InstallResult{}, err
	}
	if strings.TrimSpace(i.StoreDir) == "" {
		return InstallResult{}, fmt.Errorf("store dir is required")
	}
	if strings.TrimSpace(req.Source) == "" {
		return InstallResult{}, fmt.Errorf("source is required")
	}
	if req.Backend == "" {
		return InstallResult{}, fmt.Errorf("source preset requires backend")
	}
	modelRef, err := backendManagedModelRef(req.Backend, req.Source)
	if err != nil {
		return InstallResult{}, err
	}
	if req.ContextLength <= 0 {
		return InstallResult{}, fmt.Errorf("source preset requires context length")
	}
	if req.Contract == nil {
		return InstallResult{}, fmt.Errorf("source preset requires resource contract")
	}
	if req.Contract.SizeMB <= 0 {
		return InstallResult{}, fmt.Errorf("source preset requires size_mb resource contract")
	}
	if req.Contract.KVPerTokenMB <= 0 {
		return InstallResult{}, fmt.Errorf("source preset requires kv_per_token_mb resource contract")
	}
	if strings.TrimSpace(req.Contract.EngineProfile) == "" {
		return InstallResult{}, fmt.Errorf("source preset requires engine_profile readiness contract")
	}
	if err := ensureStore(i.StoreDir); err != nil {
		return InstallResult{}, err
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = sanitizeID(firstNonEmpty(req.Model, filepath.Base(modelRef)))
	}
	if err := safeid.Validate("preset id", id); err != nil {
		return InstallResult{}, err
	}
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = id
	}
	now := req.Now
	if now.IsZero() {
		now = i.now()
	}
	caps := append([]domain.Capability(nil), req.Contract.Capabilities...)
	if len(caps) == 0 {
		caps = []domain.Capability{domain.CapabilityChat}
	}
	format := strings.TrimSpace(req.Contract.Format)
	if format == "" {
		format = "hf-transformers"
	}
	profile := string(req.Backend)
	preset := domain.Preset{
		ID:                    id,
		ModelRef:              modelRef,
		Aliases:               modelAliases(id, model, modelRef),
		Backend:               req.Backend,
		ContextLength:         req.ContextLength,
		Quant:                 strings.TrimSpace(req.Contract.Quant),
		Capabilities:          caps,
		ProviderProfile:       profile,
		LaunchProfile:         profile,
		ArtifactSizeMB:        req.Contract.SizeMB,
		ModelFormat:           format,
		EstWeightsMB:          req.Contract.SizeMB,
		KVPerTokenMB:          req.Contract.KVPerTokenMB,
		GeneratedBy:           "catalog-source",
		GeneratedFrom:         req.Source,
		EngineProfileID:       strings.TrimSpace(req.Contract.EngineProfile),
		EngineReadiness:       domain.EngineReadinessReadyProfile,
		EngineReadinessReason: "source contract references saved engine profile",
	}
	prov := Provenance{
		PresetID:         id,
		Source:           req.Source,
		Importer:         string(req.Backend) + "-source-contract",
		MaterializedPath: modelRef,
		InstalledAt:      now,
	}
	if err := writeJSONReplace(filepath.Join(i.StoreDir, "presets", id+".json"), preset); err != nil {
		return InstallResult{}, err
	}
	if err := writeJSONReplace(filepath.Join(i.StoreDir, "provenance", id+".json"), prov); err != nil {
		return InstallResult{}, err
	}
	jobID, err := installJobID(InstallRequest{
		Source:        req.Source,
		ID:            req.ID,
		Model:         req.Model,
		ContextLength: req.ContextLength,
		Backend:       req.Backend,
	})
	if err != nil {
		return InstallResult{}, err
	}
	state := InstallState{
		JobID:          jobID,
		Source:         req.Source,
		PresetID:       id,
		Status:         "ready",
		DraftName:      modelRef,
		DraftImporter:  prov.Importer,
		DraftSize:      int64(req.Contract.SizeMB) << 20,
		ModelPath:      modelRef,
		PresetPath:     filepath.Join(i.StoreDir, "presets", id+".json"),
		ProvenancePath: filepath.Join(i.StoreDir, "provenance", id+".json"),
		UpdatedAt:      now,
		Progress: []ProgressEvent{{
			JobID:   jobID,
			Stage:   "ready",
			Message: "backend-managed source preset is materialized",
			At:      now,
		}},
	}
	if err := writeInstallState(i.StoreDir, state); err != nil {
		return InstallResult{}, err
	}
	return InstallResult{Preset: preset, Provenance: prov, Progress: state.Progress}, nil
}

func backendManagedModelRef(backend domain.Backend, source string) (string, error) {
	switch backend {
	case domain.BackendVLLM:
		return huggingFaceRepoModelRef(source)
	default:
		return "", fmt.Errorf("backend %s has no backend-managed source contract", backend)
	}
}

func huggingFaceRepoModelRef(source string) (string, error) {
	parsed, err := url.Parse(source)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "hf" || parsed.Host == "" {
		return "", fmt.Errorf("backend-managed vllm source must be a repo-level hf://owner/repo reference")
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 1 || parts[0] == "" {
		return "", fmt.Errorf("backend-managed vllm source must be a repo-level hf://owner/repo reference")
	}
	return parsed.Host + "/" + parts[0], nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
