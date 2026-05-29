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
	now := i.now()
	progress := []ProgressEvent{}
	emit := func(stage, msg string) {
		progress = append(progress, ProgressEvent{JobID: installJobID(req), Stage: stage, Message: msg, At: i.now()})
	}
	emit("import", "inspecting source")
	draft, err := importers.Import(ctx, req.Source)
	if err != nil {
		return InstallResult{Progress: progress}, err
	}
	id := req.ID
	if id == "" {
		id = sanitizeID(strings.TrimSuffix(draft.Name, filepath.Ext(draft.Name)))
	}
	if id == "" {
		return InstallResult{}, fmt.Errorf("could not derive preset id from %q", req.Source)
	}
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

	if err := ensureStore(i.StoreDir); err != nil {
		return InstallResult{Progress: progress}, err
	}
	tmp, err := os.MkdirTemp(filepath.Join(i.StoreDir, "tmp"), id+".")
	if err != nil {
		return InstallResult{Progress: progress}, err
	}
	defer os.RemoveAll(tmp)

	emit("copy", "copying model artifact")
	finalModel := filepath.Join(i.StoreDir, "models", id+"-"+draft.Name)
	tmpModel := filepath.Join(tmp, "model")
	if err := copyFile(ctx, draft.Path, tmpModel); err != nil {
		return InstallResult{Progress: progress}, err
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
	if err := writeJSON(filepath.Join(tmp, "preset.json"), preset); err != nil {
		return InstallResult{Progress: progress}, err
	}
	if err := writeJSON(filepath.Join(tmp, "provenance.json"), prov); err != nil {
		return InstallResult{Progress: progress}, err
	}
	if err := ctx.Err(); err != nil {
		return InstallResult{Progress: progress}, err
	}

	emit("commit", "registering preset")
	if err := os.Rename(tmpModel, finalModel); err != nil {
		return InstallResult{Progress: progress}, err
	}
	if err := os.Rename(filepath.Join(tmp, "provenance.json"), filepath.Join(i.StoreDir, "provenance", id+".json")); err != nil {
		return InstallResult{Progress: progress}, err
	}
	if err := os.Rename(filepath.Join(tmp, "preset.json"), filepath.Join(i.StoreDir, "presets", id+".json")); err != nil {
		return InstallResult{Progress: progress}, err
	}
	emit("ready", "preset is materialized")
	return InstallResult{Preset: preset, Provenance: prov, Progress: progress}, nil
}

func (i Installer) now() time.Time {
	if i.Now != nil {
		return i.Now()
	}
	return clock.System{}.Now()
}

func ensureStore(root string) error {
	for _, dir := range []string{"models", "presets", "provenance", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			return err
		}
	}
	return nil
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
