package importers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestImportHuggingFaceDownloadsFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/owner/repo/resolve/main/model.gguf" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer token-a" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		_, _ = w.Write([]byte("hf model"))
	}))
	defer server.Close()
	t.Setenv("MYCELIUM_HF_BASE_URL", server.URL)
	t.Setenv("HF_TOKEN", "token-a")

	draft, err := Import(context.Background(), "hf://owner/repo/model.gguf")
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(draft.Path) })
	body, err := os.ReadFile(draft.Path)
	if err != nil {
		t.Fatalf("read draft: %v", err)
	}
	if draft.Importer != "huggingface" || draft.Name != "model.gguf" || string(body) != "hf model" {
		t.Fatalf("draft=%+v body=%q", draft, body)
	}
}

func TestImportOCIDownloadsFirstLayer(t *testing.T) {
	layer := []byte("oci model")
	sum := sha256.Sum256(layer)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/ns/model/manifests/v1":
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"schemaVersion": 2,
				"layers": []map[string]any{{
					"digest": digest,
					"size":   len(layer),
					"annotations": map[string]string{
						"org.opencontainers.image.title": "model.gguf",
					},
				}},
			})
		case "/v2/ns/model/blobs/" + digest:
			_, _ = w.Write(layer)
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()
	t.Setenv("MYCELIUM_OCI_INSECURE", "1")
	host := strings.TrimPrefix(server.URL, "http://")

	draft, err := Import(context.Background(), "oci://"+host+"/ns/model:v1")
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(draft.Path) })
	body, err := os.ReadFile(draft.Path)
	if err != nil {
		t.Fatalf("read draft: %v", err)
	}
	if draft.Importer != "oci" || draft.Name != "model.gguf" || string(body) != string(layer) {
		t.Fatalf("draft=%+v body=%q", draft, body)
	}
}

func TestImportMalformedRemoteSourcesFailCleanly(t *testing.T) {
	for _, source := range []string{"hf://owner", "oci://"} {
		_, err := Import(context.Background(), source)
		if err == nil {
			t.Fatalf("%s unexpectedly imported", source)
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
