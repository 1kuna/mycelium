package catalog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/trace"
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
	if result.Preset.ID != "tiny" || result.Preset.ModelRef == source || result.Preset.ContextLength != 4096 || strings.Join(result.Preset.Aliases, ",") != "tiny-model" {
		t.Fatalf("preset = %+v", result.Preset)
	}
	if _, err := os.Stat(result.Preset.ModelRef); err != nil {
		t.Fatalf("materialized model missing: %v", err)
	}
	if len(result.Progress) < 4 || result.Progress[len(result.Progress)-1].Stage != "ready" {
		t.Fatalf("progress = %+v", result.Progress)
	}
	if !hasInstallTrace(result.Trace, "install/import_source") || !hasInstallTrace(result.Trace, "install/commit_preset") {
		t.Fatalf("trace = %+v", result.Trace)
	}
	stored, err := ReadPreset(store, "tiny")
	if err != nil {
		t.Fatalf("ReadPreset: %v", err)
	}
	if stored.ModelRef != result.Preset.ModelRef || strings.Join(stored.Aliases, ",") != "tiny-model" {
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
	if !hasInstallTrace(resumed.Trace, "install/read_ready_preset") {
		t.Fatalf("resumed trace = %+v", resumed.Trace)
	}
}

func TestInstallWithProgressReportsDurableState(t *testing.T) {
	source := writeModel(t, "tiny.gguf", "model")
	store := t.TempDir()
	var events []ProgressEvent
	var states []InstallState
	result, err := NewInstaller(store).InstallWithProgress(context.Background(), InstallRequest{Source: source, ID: "tiny"}, func(event ProgressEvent, state InstallState) error {
		events = append(events, event)
		states = append(states, state)
		durable, err := readInstallState(store, state.JobID)
		if err != nil {
			return err
		}
		if durable.Status != state.Status || len(durable.Progress) != len(state.Progress) {
			return errors.New("progress callback fired before durable state was written")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("InstallWithProgress: %v", err)
	}
	if result.Preset.ID != "tiny" || len(events) == 0 || events[len(events)-1].Stage != "ready" {
		t.Fatalf("result=%+v events=%+v", result, events)
	}
	if states[len(states)-1].Status != "ready" || states[len(states)-1].PresetID != "tiny" {
		t.Fatalf("states = %+v", states)
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

func TestInstallHelperErrorPaths(t *testing.T) {
	store := t.TempDir()
	if _, err := readInstallState(store, "missing"); err != nil {
		t.Fatalf("missing state: %v", err)
	}
	badState := filepath.Join(store, "jobs", "bad.json")
	if err := os.MkdirAll(filepath.Dir(badState), 0755); err != nil {
		t.Fatalf("mkdir jobs: %v", err)
	}
	if err := os.WriteFile(badState, []byte(`{`), 0644); err != nil {
		t.Fatalf("write bad state: %v", err)
	}
	if _, err := readInstallState(store, "bad"); err == nil {
		t.Fatal("bad install state accepted")
	}
	if err := writeInstallState(store, InstallState{}); err == nil {
		t.Fatal("missing job id accepted")
	}
	if _, err := NewInstaller("").Install(context.Background(), InstallRequest{Source: "x"}); err == nil || !strings.Contains(err.Error(), "store dir") {
		t.Fatalf("store dir err = %v", err)
	}
	source := writeModel(t, "tiny.gguf", "model")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := copyFile(ctx, source, filepath.Join(t.TempDir(), "out")); !errors.Is(err, context.Canceled) {
		t.Fatalf("copy ctx err = %v", err)
	}
	out := filepath.Join(t.TempDir(), "exists.json")
	if err := os.WriteFile(out, []byte("{}"), 0644); err != nil {
		t.Fatalf("write exists: %v", err)
	}
	if err := writeJSON(out, map[string]string{"x": "y"}); err == nil {
		t.Fatal("writeJSON overwrote existing file")
	}
	if err := writeJSON(filepath.Join(t.TempDir(), "bad.json"), map[string]any{"x": func() {}}); err == nil {
		t.Fatal("writeJSON accepted unsupported value")
	}
	if err := writeJSONReplace(out, map[string]string{"x": "y"}); err != nil {
		t.Fatalf("writeJSONReplace: %v", err)
	}
	if err := writeJSONReplace(filepath.Join(t.TempDir(), "bad.json"), map[string]any{"x": func() {}}); err == nil {
		t.Fatal("writeJSONReplace accepted unsupported value")
	}
	fileRoot := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(fileRoot, []byte("x"), 0644); err != nil {
		t.Fatalf("write file root: %v", err)
	}
	if err := ensureStore(fileRoot); err == nil {
		t.Fatal("ensureStore accepted file root")
	}
	if got := NewInstaller(store).now(); got.IsZero() {
		t.Fatal("default now returned zero")
	}
}

func TestInstallValidationErrors(t *testing.T) {
	if _, err := NewInstaller(t.TempDir()).Install(context.Background(), InstallRequest{}); err == nil || !strings.Contains(err.Error(), "source is required") {
		t.Fatalf("source err = %v", err)
	}
	if _, err := NewInstaller(t.TempDir()).Materialize(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "source is required") {
		t.Fatalf("materialize err = %v", err)
	}
	source := writeModel(t, "!!!", "model")
	if _, err := NewInstaller(t.TempDir()).Install(context.Background(), InstallRequest{Source: source}); err == nil || !strings.Contains(err.Error(), "derive preset id") {
		t.Fatalf("derive err = %v", err)
	}
}

func TestInstallReadyStateRequiresCommittedPresetAndProvenance(t *testing.T) {
	store := t.TempDir()
	if err := ensureStore(store); err != nil {
		t.Fatalf("ensureStore: %v", err)
	}
	if err := writeInstallState(store, InstallState{JobID: "install-tiny", Source: "tiny.gguf", PresetID: "tiny", Status: "ready"}); err != nil {
		t.Fatalf("write ready state: %v", err)
	}
	if _, err := NewInstaller(store).Install(context.Background(), InstallRequest{Source: "tiny.gguf", ID: "tiny"}); err == nil {
		t.Fatal("ready state without committed preset succeeded")
	}

	preset := domain.Preset{ID: "tiny", ModelRef: "model.gguf", Backend: domain.BackendLlamaCpp}
	if err := writeJSON(filepath.Join(store, "presets", "tiny.json"), preset); err != nil {
		t.Fatalf("write preset: %v", err)
	}
	if _, err := NewInstaller(store).Install(context.Background(), InstallRequest{Source: "tiny.gguf", ID: "tiny"}); err == nil {
		t.Fatal("ready state without provenance succeeded")
	}
}

func TestInstallResumedDraftWithoutArtifactFailsLoudly(t *testing.T) {
	store := t.TempDir()
	if err := ensureStore(store); err != nil {
		t.Fatalf("ensureStore: %v", err)
	}
	if err := writeInstallState(store, InstallState{
		JobID:         "install-tiny",
		Source:        filepath.Join(t.TempDir(), "missing.gguf"),
		Status:        "import",
		DraftName:     "tiny.gguf",
		DraftImporter: "local",
		DraftSize:     4,
	}); err != nil {
		t.Fatalf("write resumed state: %v", err)
	}
	_, err := NewInstaller(store).Install(context.Background(), InstallRequest{Source: filepath.Join(t.TempDir(), "missing.gguf"), ID: "tiny"})
	if err == nil || !strings.Contains(err.Error(), "no staged artifact") {
		t.Fatalf("artifact err = %v", err)
	}
}

func TestReadPresetAndProvenanceRejectBadJSON(t *testing.T) {
	store := t.TempDir()
	if err := os.MkdirAll(filepath.Join(store, "presets"), 0755); err != nil {
		t.Fatalf("mkdir presets: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(store, "provenance"), 0755); err != nil {
		t.Fatalf("mkdir provenance: %v", err)
	}
	if err := os.WriteFile(filepath.Join(store, "presets", "bad.json"), []byte(`{`), 0644); err != nil {
		t.Fatalf("write preset: %v", err)
	}
	if err := os.WriteFile(filepath.Join(store, "provenance", "bad.json"), []byte(`{`), 0644); err != nil {
		t.Fatalf("write provenance: %v", err)
	}
	if _, err := ReadPreset(store, "bad"); err == nil {
		t.Fatal("bad preset json accepted")
	}
	if _, err := ReadProvenance(store, "bad"); err == nil {
		t.Fatal("bad provenance json accepted")
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

func hasInstallTrace(steps []trace.Step, op string) bool {
	for _, step := range steps {
		if step.Operation == op && step.Status == "success" {
			return true
		}
	}
	return false
}
