package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"mycelium/internal/catalog/importers"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/internal/safeid"
	"mycelium/internal/trace"
)

type Installer struct {
	StoreDir string
	Now      func() time.Time
}

type InstallJob struct {
	done chan struct{}
	res  InstallResult
	err  error
}

type ProgressFunc func(ProgressEvent, InstallState) error

func NewInstaller(storeDir string) Installer {
	return Installer{StoreDir: storeDir, Now: clock.System{}.Now}
}

func (i Installer) Materialize(ctx context.Context, ref string) (domain.Preset, error) {
	result, err := i.Install(ctx, InstallRequest{Source: ref})
	if err != nil {
		return domain.Preset{}, err
	}
	return result.Preset, nil
}

func (i Installer) Start(ctx context.Context, req InstallRequest) *InstallJob {
	return i.StartWithProgress(ctx, req, nil)
}

func (i Installer) StartWithProgress(ctx context.Context, req InstallRequest, progress ProgressFunc) *InstallJob {
	job := &InstallJob{done: make(chan struct{})}
	go func() {
		defer close(job.done)
		job.res, job.err = i.install(ctx, req, progress)
	}()
	return job
}

func (i Installer) Install(ctx context.Context, req InstallRequest) (InstallResult, error) {
	return i.Start(ctx, req).Wait(ctx)
}

func (i Installer) InstallWithProgress(ctx context.Context, req InstallRequest, progress ProgressFunc) (InstallResult, error) {
	return i.StartWithProgress(ctx, req, progress).Wait(ctx)
}

func (j *InstallJob) Wait(ctx context.Context) (InstallResult, error) {
	select {
	case <-ctx.Done():
		return InstallResult{}, ctx.Err()
	case <-j.done:
		return j.res, j.err
	}
}

func (i Installer) install(ctx context.Context, req InstallRequest, onProgress ProgressFunc) (InstallResult, error) {
	tr := trace.New(i.now)
	var progress []ProgressEvent
	result := func(preset domain.Preset, prov Provenance) InstallResult {
		return InstallResult{
			Preset:     preset,
			Provenance: prov,
			Progress:   append([]ProgressEvent(nil), progress...),
			Trace:      append([]trace.Step(nil), tr.Steps...),
		}
	}
	if err := ctx.Err(); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	if req.Source == "" {
		return result(domain.Preset{}, Provenance{}), fmt.Errorf("source is required")
	}
	if i.StoreDir == "" {
		return result(domain.Preset{}, Provenance{}), fmt.Errorf("store dir is required")
	}
	if err := tr.Do("install/ensure_store", map[string]any{"store_dir": i.StoreDir}, func() error {
		return ensureStore(i.StoreDir)
	}); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	now := i.now()
	jobID, err := installJobID(req)
	if err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	var state InstallState
	if err := tr.Do("install/read_state", map[string]any{"job_id": jobID}, func() error {
		var err error
		state, err = readInstallState(i.StoreDir, jobID)
		return err
	}); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	progress = append([]ProgressEvent(nil), state.Progress...)
	if state.Status == "ready" && state.PresetID != "" {
		var preset domain.Preset
		if err := tr.Do("install/read_ready_preset", map[string]any{"preset_id": state.PresetID}, func() error {
			var err error
			preset, err = ReadPreset(i.StoreDir, state.PresetID)
			return err
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
		var prov Provenance
		if err := tr.Do("install/read_ready_provenance", map[string]any{"preset_id": state.PresetID}, func() error {
			var err error
			prov, err = ReadProvenance(i.StoreDir, state.PresetID)
			return err
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
		return result(preset, prov), nil
	}
	if state.JobID == "" {
		state = InstallState{JobID: jobID, Source: req.Source, Status: "created", UpdatedAt: now}
	}
	emit := func(stage, msg string) error {
		event := ProgressEvent{JobID: jobID, Stage: stage, Message: msg, At: i.now()}
		progress = append(progress, event)
		state.Progress = append(state.Progress, event)
		state.Status = stage
		state.UpdatedAt = event.At
		if err := writeInstallState(i.StoreDir, state); err != nil {
			return err
		}
		if onProgress != nil {
			if err := onProgress(event, state); err != nil {
				return err
			}
		}
		return nil
	}
	if err := emit("import", "inspecting source"); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	draft := importers.Draft{}
	if state.DraftName != "" {
		draft.Name = state.DraftName
		draft.Importer = state.DraftImporter
		draft.Size = state.DraftSize
	} else {
		if err := tr.Do("install/import_source", map[string]any{"source": req.Source}, func() error {
			var err error
			draft, err = importers.Import(ctx, req.Source)
			return err
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
		state.DraftName = draft.Name
		state.DraftImporter = draft.Importer
		state.DraftSize = draft.Size
		state.UpdatedAt = i.now()
		if err := tr.Do("install/write_draft_state", map[string]any{"job_id": jobID}, func() error {
			return writeInstallState(i.StoreDir, state)
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
	}
	if err := safeid.Validate("draft name", draft.Name); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	id := req.ID
	if id == "" {
		id = state.PresetID
	}
	if id == "" {
		id = sanitizeID(strings.TrimSuffix(draft.Name, filepath.Ext(draft.Name)))
	}
	if id == "" {
		return result(domain.Preset{}, Provenance{}), fmt.Errorf("could not derive preset id from %q", req.Source)
	}
	if err := safeid.Validate("preset id", id); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	state.PresetID = id
	model := req.Model
	if model == "" {
		model = id
	}
	contextLen := req.ContextLength
	if contextLen == 0 {
		contextLen = 2048
	}
	backend := req.Backend
	if backend == "" {
		backend = domain.BackendLlamaCpp
	}
	quant := req.Quant
	if quant == "" {
		quant = "unknown"
	}

	stageDir := filepath.Join(i.StoreDir, "jobs", jobID)
	if err := tr.Do("install/create_stage_dir", map[string]any{"job_id": jobID}, func() error {
		return os.MkdirAll(stageDir, 0755)
	}); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}

	if err := emit("copy", "copying model artifact"); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	finalModel := filepath.Join(i.StoreDir, "models", id+"-"+draft.Name)
	stageModel := filepath.Join(stageDir, "model")
	if !fileExists(finalModel) && !fileExists(stageModel) {
		if draft.Path == "" {
			return result(domain.Preset{}, Provenance{}), fmt.Errorf("install job %q has no staged artifact and source must be re-imported", jobID)
		}
		if err := tr.Do("install/copy_artifact", map[string]any{"job_id": jobID, "preset_id": id}, func() error {
			return copyFile(ctx, draft.Path, stageModel)
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
	}
	preset := domain.Preset{
		ID:             id,
		ModelRef:       finalModel,
		Aliases:        modelAliases(id, model, finalModel),
		Backend:        backend,
		ContextLength:  contextLen,
		Quant:          quant,
		Capabilities:   []domain.Capability{domain.CapabilityChat},
		LaunchProfile:  "llamacpp-metal",
		ArtifactSizeMB: estimateWeightsMB(draft.Size),
		EstWeightsMB:   estimateWeightsMB(draft.Size),
		KVPerTokenMB:   0.01,
	}
	prov := Provenance{
		PresetID:         id,
		Source:           req.Source,
		Importer:         draft.Importer,
		MaterializedPath: finalModel,
		InstalledAt:      now,
	}
	stagePreset := filepath.Join(stageDir, "preset.json")
	stageProvenance := filepath.Join(stageDir, "provenance.json")
	if !fileExists(filepath.Join(i.StoreDir, "presets", id+".json")) && !fileExists(stagePreset) {
		if err := tr.Do("install/write_stage_preset", map[string]any{"preset_id": id}, func() error {
			return writeJSON(stagePreset, preset)
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
	}
	if !fileExists(filepath.Join(i.StoreDir, "provenance", id+".json")) && !fileExists(stageProvenance) {
		if err := tr.Do("install/write_stage_provenance", map[string]any{"preset_id": id}, func() error {
			return writeJSON(stageProvenance, prov)
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
	}
	state.ModelPath = finalModel
	state.PresetPath = filepath.Join(i.StoreDir, "presets", id+".json")
	state.ProvenancePath = filepath.Join(i.StoreDir, "provenance", id+".json")
	state.UpdatedAt = i.now()
	if err := tr.Do("install/write_paths_state", map[string]any{"job_id": jobID, "preset_id": id}, func() error {
		return writeInstallState(i.StoreDir, state)
	}); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	if err := ctx.Err(); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}

	if err := emit("commit", "registering preset"); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	if !fileExists(finalModel) {
		if err := tr.Do("install/commit_artifact", map[string]any{"preset_id": id}, func() error {
			return os.Rename(stageModel, finalModel)
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
	}
	if !fileExists(state.ProvenancePath) {
		if err := tr.Do("install/commit_provenance", map[string]any{"preset_id": id}, func() error {
			return os.Rename(stageProvenance, state.ProvenancePath)
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
	}
	if !fileExists(state.PresetPath) {
		if err := tr.Do("install/commit_preset", map[string]any{"preset_id": id}, func() error {
			return os.Rename(stagePreset, state.PresetPath)
		}); err != nil {
			return result(domain.Preset{}, Provenance{}), err
		}
	}
	state.Status = "ready"
	state.UpdatedAt = i.now()
	if err := tr.Do("install/write_ready_state", map[string]any{"job_id": jobID, "preset_id": id}, func() error {
		return writeInstallState(i.StoreDir, state)
	}); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	if err := emit("ready", "preset is materialized"); err != nil {
		return result(domain.Preset{}, Provenance{}), err
	}
	return result(preset, prov), nil
}

func modelAliases(id, model, modelRef string) []string {
	if model == "" || model == id || model == modelRef {
		return nil
	}
	return []string{model}
}

func (i Installer) now() time.Time {
	if i.Now != nil {
		return i.Now()
	}
	return clock.System{}.Now()
}

func ensureStore(root string) error {
	for _, dir := range []string{"models", "presets", "provenance", "tmp", "jobs"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			return err
		}
	}
	return nil
}

func readInstallState(root, jobID string) (InstallState, error) {
	if err := safeid.Validate("install job id", jobID); err != nil {
		return InstallState{}, err
	}
	path := installStatePath(root, jobID)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return InstallState{}, nil
	}
	if err != nil {
		return InstallState{}, err
	}
	var state InstallState
	if err := json.Unmarshal(data, &state); err != nil {
		return InstallState{}, err
	}
	return state, nil
}

func writeInstallState(root string, state InstallState) error {
	if state.JobID == "" {
		return fmt.Errorf("install job id is required")
	}
	if err := safeid.Validate("install job id", state.JobID); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(root, "jobs"), 0755); err != nil {
		return err
	}
	return writeJSONReplace(installStatePath(root, state.JobID), state)
}

func installStatePath(root, jobID string) string {
	return filepath.Join(root, "jobs", jobID+".json")
}

func copyFile(ctx context.Context, src, dst string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return ctx.Err()
}

func writeJSON(path string, v any) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func writeJSONReplace(path string, v any) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func estimateWeightsMB(bytes int64) int {
	return int(math.Max(1, math.Ceil(float64(bytes)/(1024*1024))))
}

var idChars = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

func sanitizeID(s string) string {
	return strings.Trim(idChars.ReplaceAllString(strings.ToLower(s), "-"), "-")
}

func installJobID(req InstallRequest) (string, error) {
	if req.ID != "" {
		if err := safeid.Validate("preset id", req.ID); err != nil {
			return "", err
		}
		return "install-" + req.ID, nil
	}
	id := sanitizeID(filepath.Base(req.Source))
	if id == "" {
		return "", fmt.Errorf("could not derive install job id from %q", req.Source)
	}
	return "install-" + id, nil
}

func InstallJobID(req InstallRequest) (string, error) {
	return installJobID(req)
}

var _ ports.Catalog = Installer{}
