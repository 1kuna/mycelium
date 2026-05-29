package catalog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mycelium/internal/domain"
)

func TestInstallLocalModelMaterializesPresetAndProvenance(t *testing.T) {
	source := writeModel(t, "tiny.gguf", "model")
	store := t.TempDir()
	installer := NewInstaller(store)
	installer.Now = func() time.Time { return time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC) }

	result, err := installer.Install(context.Background(), InstallRequest{
		Source:        source,
		ID:            "tiny",
		Model:         "tiny-model",
		ContextLength: 4096,
		Quant:         "Q4_0",
	})
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if result.Preset.ID != "tiny" || result.Preset.ModelRef == source || result.Preset.ContextLength != 4096 {
		t.Fatalf("preset = %+v", result.Preset)
	}
	if _, err := os.Stat(result.Preset.ModelRef); err != nil {
		t.Fatalf("materialized model missing: %v", err)
	}
	if len(result.Progress) < 4 || result.Progress[len(result.Progress)-1].Stage != "ready" {
		t.Fatalf("progress = %+v", result.Progress)
	}
	stored, err := ReadPreset(store, "tiny")
	if err != nil {
		t.Fatalf("ReadPreset: %v", err)
	}
	if stored.ModelRef != result.Preset.ModelRef {
		t.Fatalf("stored = %+v result = %+v", stored, result.Preset)
	}
	prov, err := ReadProvenance(store, "tiny")
	if err != nil {
		t.Fatalf("ReadProvenance: %v", err)
	}
	if prov.Source != source || prov.Importer != "local" || prov.MaterializedPath != result.Preset.ModelRef {
		t.Fatalf("provenance = %+v", prov)
	}
	state, err := readInstallState(store, "install-tiny")
	if err != nil {
		t.Fatalf("read install state: %v", err)
	}
	if state.Status != "ready" || state.PresetID != "tiny" || len(state.Progress) == 0 {
		t.Fatalf("install state = %+v", state)
	}
	if err := os.Remove(source); err != nil {
		t.Fatalf("remove source: %v", err)
	}
	resumed, err := installer.Install(context.Background(), InstallRequest{Source: source, ID: "tiny"})
	if err != nil {
		t.Fatalf("resume completed install: %v", err)
	}
	if resumed.Preset.ID != result.Preset.ID || resumed.Provenance.MaterializedPath != result.Preset.ModelRef {
		t.Fatalf("resumed = %+v", resumed)
	}
}

func TestInstallResumesStagedJobWithoutRegisteringEarly(t *testing.T) {
	store := t.TempDir()
	req := InstallRequest{Source: filepath.Join(t.TempDir(), "missing.gguf"), ID: "tiny"}
	jobID := installJobID(req)
	if err := ensureStore(store); err != nil {
		t.Fatalf("ensureStore: %v", err)
	}
	stageDir := filepath.Join(store, "jobs", jobID)
	if err := os.MkdirAll(stageDir, 0755); err != nil {
		t.Fatalf("stage dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stageDir, "model"), []byte("model"), 0644); err != nil {
		t.Fatalf("stage model: %v", err)
	}
	presetPath := filepath.Join(store, "presets", "tiny.json")
	provenancePath := filepath.Join(store, "provenance", "tiny.json")
	finalModel := filepath.Join(store, "models", "tiny-tiny.gguf")
	preset := domain.Preset{ID: "tiny", ModelRef: finalModel, Backend: domain.BackendLlamaCpp, ContextLength: 2048, LaunchProfile: "llamacpp-metal", EstWeightsMB: 1, KVPerTokenMB: 0.01}
	prov := Provenance{PresetID: "tiny", Source: req.Source, Importer: "local", MaterializedPath: finalModel, InstalledAt: time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)}
	if err := writeJSON(filepath.Join(stageDir, "preset.json"), preset); err != nil {
		t.Fatalf("stage preset: %v", err)
	}
	if err := writeJSON(filepath.Join(stageDir, "provenance.json"), prov); err != nil {
		t.Fatalf("stage provenance: %v", err)
	}
	if err := writeInstallState(store, InstallState{
		JobID:          jobID,
		Source:         req.Source,
		PresetID:       "tiny",
		Status:         "copy",
		DraftName:      "tiny.gguf",
		DraftImporter:  "local",
		DraftSize:      int64(len("model")),
		ModelPath:      finalModel,
		PresetPath:     presetPath,
		ProvenancePath: provenancePath,
	}); err != nil {
		t.Fatalf("write state: %v", err)
	}
	if _, err := ReadPreset(store, "tiny"); err == nil {
		t.Fatal("staged job registered a preset before commit")
	}

	installer := NewInstaller(store)
	installer.Now = func() time.Time { return prov.InstalledAt }
	result, err := installer.Install(context.Background(), req)
	if err != nil {
		t.Fatalf("Install resume: %v", err)
	}
	if result.Preset.ID != "tiny" || result.Preset.ModelRef != finalModel {
		t.Fatalf("result = %+v", result)
	}
	if _, err := ReadPreset(store, "tiny"); err != nil {
		t.Fatalf("ReadPreset after resume: %v", err)
	}
}

func TestInstallCanceledBeforeStartDoesNotRegisterPreset(t *testing.T) {
	source := writeModel(t, "tiny.gguf", "model")
	store := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewInstaller(store).Install(ctx, InstallRequest{Source: source, ID: "tiny"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if _, err := ReadPreset(store, "tiny"); err == nil {
		t.Fatal("canceled install registered preset")
	}
}

func TestMalformedRemoteImportsFailCleanly(t *testing.T) {
	for _, source := range []string{"hf://org", "oci://"} {
		_, err := NewInstaller(t.TempDir()).Install(context.Background(), InstallRequest{Source: source})
		if err == nil {
			t.Fatalf("%s err = %v", source, err)
		}
	}
}

func TestInstallerSatisfiesCatalogPort(t *testing.T) {
	source := writeModel(t, "tiny.gguf", "model")
	preset, err := NewInstaller(t.TempDir()).Materialize(context.Background(), source)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if preset.Backend != domain.BackendLlamaCpp {
		t.Fatalf("preset = %+v", preset)
	}
}

func writeModel(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write model: %v", err)
	}
	return path
}
