package importers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImportLocalPathAndFileURL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tiny.gguf")
	if err := os.WriteFile(path, []byte("model"), 0644); err != nil {
		t.Fatalf("write model: %v", err)
	}
	for _, source := range []string{path, "file://" + path} {
		draft, err := Import(context.Background(), source)
		if err != nil {
			t.Fatalf("Import(%s): %v", source, err)
		}
		if draft.Importer != "local" || draft.Path != path || draft.Name != "tiny.gguf" || draft.Size == 0 {
			t.Fatalf("draft = %+v", draft)
		}
	}
}

func TestImportUnsupportedRemoteSchemesFailCleanly(t *testing.T) {
	for _, source := range []string{"hf://org/model", "oci://registry/model"} {
		_, err := Import(context.Background(), source)
		if err == nil || !strings.Contains(err.Error(), "not implemented in Phase 3") {
			t.Fatalf("%s err = %v", source, err)
		}
	}
}

func TestImportRejectsDirectoriesAndCanceledContext(t *testing.T) {
	_, err := Import(context.Background(), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "is a directory") {
		t.Fatalf("dir err = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Import(ctx, "anything.gguf")
	if err != context.Canceled {
		t.Fatalf("canceled err = %v", err)
	}
}
