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
