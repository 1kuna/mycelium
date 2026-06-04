package importers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func directHTTPClient(handler http.Handler) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Result(), nil
	})}
}

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
	body := []byte("hf model")
	sum := sha256.Sum256(body)
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/owner/repo/resolve/main/model.gguf" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer token-a" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Length", "8")
		_, _ = w.Write(body)
	}))
	t.Setenv("MYCELIUM_HF_BASE_URL", "http://hf.test")
	t.Setenv("HF_TOKEN", "token-a")

	draft, err := importWithClient(context.Background(), "hf://owner/repo/model.gguf?sha256="+hex.EncodeToString(sum[:]), client)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(draft.Path) })
	draftBody, err := os.ReadFile(draft.Path)
	if err != nil {
		t.Fatalf("read draft: %v", err)
	}
	if draft.Importer != "huggingface" || draft.Name != "model.gguf" || string(draftBody) != "hf model" {
		t.Fatalf("draft=%+v body=%q", draft, draftBody)
	}
}

func TestImportOCIDownloadsFirstLayer(t *testing.T) {
	layer := []byte("oci model")
	sum := sha256.Sum256(layer)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	client := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	t.Setenv("MYCELIUM_OCI_INSECURE", "1")
	host := "oci.test"

	draft, err := importWithClient(context.Background(), "oci://"+host+"/ns/model:v1", client)
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

func TestRemoteImportErrorPathsAndHelpers(t *testing.T) {
	statusClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusTeapot)
	}))
	t.Setenv("MYCELIUM_HF_BASE_URL", "http://hf-status.test")
	hfSource := "hf://owner/repo/model.gguf?sha256=" + strings.Repeat("0", 64)
	if _, err := importWithClient(context.Background(), hfSource, statusClient); err == nil || !strings.Contains(err.Error(), "huggingface download failed") {
		t.Fatalf("hf status err = %v", err)
	}
	t.Setenv("MYCELIUM_HF_BASE_URL", "://bad")
	if _, err := Import(context.Background(), hfSource); err == nil {
		t.Fatal("expected bad base URL error")
	}
	if _, err := Import(context.Background(), "hf:///repo/model.gguf"); err == nil {
		t.Fatal("expected missing hf owner error")
	}
	if _, err := importHuggingFace(context.Background(), "%", statusClient); err == nil {
		t.Fatal("expected invalid hf URL error")
	}
	if _, err := importWithClient(context.Background(), "hf://owner/repo/model.gguf", statusClient); err == nil || !strings.Contains(err.Error(), "requires sha256") {
		t.Fatalf("hf digest err = %v", err)
	}
	noLengthClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("model"))
	}))
	modelSum := sha256.Sum256([]byte("model"))
	t.Setenv("MYCELIUM_HF_BASE_URL", "http://hf-nolength.test")
	if _, err := importWithClient(context.Background(), "hf://owner/repo/model.gguf?sha256="+hex.EncodeToString(modelSum[:]), noLengthClient); err == nil || !strings.Contains(err.Error(), "Content-Length") {
		t.Fatalf("hf content length err = %v", err)
	}
	oversizeClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "549755813889")
	}))
	if _, err := importWithClient(context.Background(), "hf://owner/repo/model.gguf?sha256="+hex.EncodeToString(modelSum[:]), oversizeClient); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("hf oversize err = %v", err)
	}
	t.Setenv("HF_TOKEN", "")
	t.Setenv("HUGGING_FACE_HUB_TOKEN", "token-b")
	if token := huggingFaceToken(); token != "token-b" {
		t.Fatalf("token = %q", token)
	}

	ociClient := directHTTPClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		layer := []byte("oci layer")
		goodSum := sha256.Sum256(layer)
		goodDigest := "sha256:" + hex.EncodeToString(goodSum[:])
		badDigest := "sha256:" + strings.Repeat("0", 64)
		switch {
		case strings.Contains(r.URL.Path, "/manifests/status"):
			http.Error(w, "missing", http.StatusNotFound)
		case strings.Contains(r.URL.Path, "/manifests/empty"):
			_, _ = w.Write([]byte(`{"layers":[]}`))
		case strings.Contains(r.URL.Path, "/manifests/badjson"):
			_, _ = w.Write([]byte(`{`))
		case strings.Contains(r.URL.Path, "/manifests/blobstatus"):
			_, _ = w.Write([]byte(`{"layers":[{"digest":"sha256:dead","annotations":{"org.opencontainers.image.title":"model.gguf"}}]}`))
		case strings.Contains(r.URL.Path, "/manifests/badsize"):
			_ = json.NewEncoder(w).Encode(map[string]any{"layers": []map[string]any{{"digest": goodDigest, "size": len(layer) + 1}}})
		case strings.Contains(r.URL.Path, "/manifests/baddigest"):
			_ = json.NewEncoder(w).Encode(map[string]any{"layers": []map[string]any{{"digest": badDigest, "size": len(layer)}}})
		case strings.Contains(r.URL.Path, "/manifests/badalg"):
			_ = json.NewEncoder(w).Encode(map[string]any{"layers": []map[string]any{{"digest": "sha512:abc", "size": len(layer)}}})
		case strings.Contains(r.URL.Path, "/blobs/sha256:dead"):
			http.Error(w, "blob missing", http.StatusNotFound)
		case strings.Contains(r.URL.Path, "/blobs/"):
			_, _ = w.Write(layer)
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	t.Setenv("MYCELIUM_OCI_INSECURE", "1")
	host := "oci-errors.test"
	for _, source := range []string{
		"oci://" + host + "/ns/model:status",
		"oci://" + host + "/ns/model:empty",
		"oci://" + host + "/ns/model:badjson",
		"oci://" + host + "/ns/model:blobstatus",
		"oci://" + host + "/ns/model:badsize",
		"oci://" + host + "/ns/model:baddigest",
		"oci://" + host + "/ns/model:badalg",
	} {
		if _, err := importWithClient(context.Background(), source, ociClient); err == nil {
			t.Fatalf("%s expected error", source)
		}
	}
	if repo, ref, err := splitOCIReference("ns/model@sha256:abc"); err != nil || repo != "ns/model" || ref != "sha256:abc" {
		t.Fatalf("digest split = %s %s %v", repo, ref, err)
	}
	if repo, ref, err := splitOCIReference("ns/model"); err != nil || repo != "ns/model" || ref != "latest" {
		t.Fatalf("latest split = %s %s %v", repo, ref, err)
	}
	for _, raw := range []string{"", "ns/model:", "ns/model@"} {
		if _, _, err := splitOCIReference(raw); err == nil {
			t.Fatalf("%q expected error", raw)
		}
	}
	if got := sanitizeTempName("../weird name!.gguf"); got != "weird-name-.gguf" {
		t.Fatalf("sanitize = %q", got)
	}
	if got := sanitizeTempName(""); got != "model" {
		t.Fatalf("empty sanitize = %q", got)
	}
	t.Setenv("MYCELIUM_OCI_INSECURE", "")
	if got := ociURL("registry.example", "v2/ns/model"); !strings.HasPrefix(got, "https://registry.example/") {
		t.Fatalf("ociURL = %s", got)
	}
	if _, err := importOCI(context.Background(), "%", ociClient); err == nil {
		t.Fatal("expected invalid oci URL error")
	}
	req := &http.Request{URL: &url.URL{Scheme: "http", Host: "127.0.0.1:1"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := downloadDraft(ctx, req, "src", "test", "x.gguf", statusClient, downloadOptions{}); err != context.Canceled {
		t.Fatalf("download canceled err = %v", err)
	}
}

func TestImportRejectsDirectoriesAndCanceledContext(t *testing.T) {
	_, err := Import(context.Background(), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "is a directory") {
		t.Fatalf("dir err = %v", err)
	}
	_, err = Import(context.Background(), filepath.Join(t.TempDir(), "missing.gguf"))
	if err == nil {
		t.Fatal("missing local file imported")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Import(ctx, "anything.gguf")
	if err != context.Canceled {
		t.Fatalf("canceled err = %v", err)
	}
	_, err = importLocal(ctx, "anything.gguf")
	if err != context.Canceled {
		t.Fatalf("local canceled err = %v", err)
	}
}
