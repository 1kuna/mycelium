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
	job := &InstallJob{done: make(chan struct{})}
	go func() {
		defer close(job.done)
		job.res, job.err = i.install(ctx, req)
	}()
	return job
}

func (i Installer) Install(ctx context.Context, req InstallRequest) (InstallResult, error) {
	return i.Start(ctx, req).Wait(ctx)
}

func (j *InstallJob) Wait(ctx context.Context) (InstallResult, error) {
	select {
	case <-ctx.Done():
		return InstallResult{}, ctx.Err()
	case <-j.done:
		return j.res, j.err
	}
}

func (i Installer) install(ctx context.Context, req InstallRequest) (InstallResult, error) {
	if err := ctx.Err(); err != nil {
		return InstallResult{}, err
	}
	if req.Source == "" {
		return InstallResult{}, fmt.Errorf("source is required")
	}
	if i.StoreDir == "" {
		return InstallResult{}, fmt.Errorf("store dir is required")
	}
	if err := ensureStore(i.StoreDir); err != nil {
		return InstallResult{}, err
	}
	now := i.now()
	jobID := installJobID(req)
	state, err := readInstallState(i.StoreDir, jobID)
	if err != nil {
		return InstallResult{}, err
	}
	if state.Status == "ready" && state.PresetID != "" {
		preset, err := ReadPreset(i.StoreDir, state.PresetID)
		if err != nil {
			return InstallResult{}, err
		}
		prov, err := ReadProvenance(i.StoreDir, state.PresetID)
		if err != nil {
			return InstallResult{}, err
		}
		return InstallResult{Preset: preset, Provenance: prov, Progress: state.Progress}, nil
	}
	if state.JobID == "" {
		state = InstallState{JobID: jobID, Source: req.Source, Status: "created", UpdatedAt: now}
	}
	progress := append([]ProgressEvent(nil), state.Progress...)
	emit := func(stage, msg string) error {
		event := ProgressEvent{JobID: jobID, Stage: stage, Message: msg, At: i.now()}
		progress = append(progress, event)
		state.Progress = append(state.Progress, event)
		state.Status = stage
		state.UpdatedAt = event.At
		return writeInstallState(i.StoreDir, state)
	}
	if err := emit("import", "inspecting source"); err != nil {
		return InstallResult{Progress: progress}, err
	}
	draft := importers.Draft{}
	if state.DraftName != "" {
		draft.Name = state.DraftName
		draft.Importer = state.DraftImporter
		draft.Size = state.DraftSize
	} else {
		draft, err = importers.Import(ctx, req.Source)
		if err != nil {
			return InstallResult{Progress: progress}, err
		}
		state.DraftName = draft.Name
		state.DraftImporter = draft.Importer
		state.DraftSize = draft.Size
		state.UpdatedAt = i.now()
		if err := writeInstallState(i.StoreDir, state); err != nil {
			return InstallResult{Progress: progress}, err
		}
	}
	id := req.ID
	if id == "" {
		id = state.PresetID
	}
	if id == "" {
		id = sanitizeID(strings.TrimSuffix(draft.Name, filepath.Ext(draft.Name)))
	}
	if id == "" {
		return InstallResult{}, fmt.Errorf("could not derive preset id from %q", req.Source)
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
	if err := os.MkdirAll(stageDir, 0755); err != nil {
		return InstallResult{Progress: progress}, err
	}

	if err := emit("copy", "copying model artifact"); err != nil {
		return InstallResult{Progress: progress}, err
	}
	finalModel := filepath.Join(i.StoreDir, "models", id+"-"+draft.Name)
	stageModel := filepath.Join(stageDir, "model")
	if !fileExists(finalModel) && !fileExists(stageModel) {
		if draft.Path == "" {
			return InstallResult{Progress: progress}, fmt.Errorf("install job %q has no staged artifact and source must be re-imported", jobID)
		}
		if err := copyFile(ctx, draft.Path, stageModel); err != nil {
			return InstallResult{Progress: progress}, err
		}
	}
	preset := domain.Preset{
		ID:            id,
		ModelRef:      finalModel,
		Backend:       backend,
		ContextLength: contextLen,
		Quant:         quant,
		Capabilities:  []domain.Capability{domain.CapabilityChat},
		LaunchProfile: "llamacpp-metal",
		EstWeightsMB:  estimateWeightsMB(draft.Size),
		KVPerTokenMB:  0.01,
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
		if err := writeJSON(stagePreset, preset); err != nil {
			return InstallResult{Progress: progress}, err
		}
	}
	if !fileExists(filepath.Join(i.StoreDir, "provenance", id+".json")) && !fileExists(stageProvenance) {
		if err := writeJSON(stageProvenance, prov); err != nil {
			return InstallResult{Progress: progress}, err
		}
	}
	state.ModelPath = finalModel
	state.PresetPath = filepath.Join(i.StoreDir, "presets", id+".json")
	state.ProvenancePath = filepath.Join(i.StoreDir, "provenance", id+".json")
	state.UpdatedAt = i.now()
	if err := writeInstallState(i.StoreDir, state); err != nil {
		return InstallResult{Progress: progress}, err
	}
	if err := ctx.Err(); err != nil {
		return InstallResult{Progress: progress}, err
	}

	if err := emit("commit", "registering preset"); err != nil {
		return InstallResult{Progress: progress}, err
	}
	if !fileExists(finalModel) {
		if err := os.Rename(stageModel, finalModel); err != nil {
			return InstallResult{Progress: progress}, err
		}
	}
	if !fileExists(state.ProvenancePath) {
		if err := os.Rename(stageProvenance, state.ProvenancePath); err != nil {
			return InstallResult{Progress: progress}, err
		}
	}
	if !fileExists(state.PresetPath) {
		if err := os.Rename(stagePreset, state.PresetPath); err != nil {
			return InstallResult{Progress: progress}, err
		}
	}
	state.Status = "ready"
	state.UpdatedAt = i.now()
	if err := writeInstallState(i.StoreDir, state); err != nil {
		return InstallResult{Progress: progress}, err
	}
	if err := emit("ready", "preset is materialized"); err != nil {
		return InstallResult{Progress: progress}, err
	}
	return InstallResult{Preset: preset, Provenance: prov, Progress: progress}, nil
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

func installJobID(req InstallRequest) string {
	if req.ID != "" {
		return "install-" + req.ID
	}
	return "install-" + sanitizeID(filepath.Base(req.Source))
}

var _ ports.Catalog = Installer{}
