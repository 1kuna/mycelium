package importers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"mycelium/internal/safeid"
)

const maxRemoteDraftBytes int64 = 512 << 30

type downloadOptions struct {
	requireContentLength bool
	expectedDigest       string
}

type Draft struct {
	Source   string
	Importer string
	Path     string
	Name     string
	Size     int64
}

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

func Import(ctx context.Context, source string) (Draft, error) {
	return importWithClient(ctx, source, http.DefaultClient)
}

func importWithClient(ctx context.Context, source string, client httpDoer) (Draft, error) {
	if err := ctx.Err(); err != nil {
		return Draft{}, err
	}
	if client == nil {
		return Draft{}, fmt.Errorf("catalog importer http client is required")
	}
	switch {
	case strings.HasPrefix(source, "hf://"):
		return importHuggingFace(ctx, source, client)
	case strings.HasPrefix(source, "oci://"):
		return importOCI(ctx, source, client)
	case strings.HasPrefix(source, "file://"):
		return importLocal(ctx, strings.TrimPrefix(source, "file://"))
	default:
		return importLocal(ctx, source)
	}
}

func importLocal(ctx context.Context, path string) (Draft, error) {
	if err := ctx.Err(); err != nil {
		return Draft{}, err
	}
	clean, err := filepath.Abs(path)
	if err != nil {
		return Draft{}, err
	}
	info, err := os.Stat(clean)
	if err != nil {
		return Draft{}, err
	}
	if info.IsDir() {
		return Draft{}, fmt.Errorf("local model source %q is a directory", clean)
	}
	return Draft{
		Source:   path,
		Importer: "local",
		Path:     clean,
		Name:     filepath.Base(clean),
		Size:     info.Size(),
	}, nil
}

func importHuggingFace(ctx context.Context, source string, client httpDoer) (Draft, error) {
	parsed, err := url.Parse(source)
	if err != nil {
		return Draft{}, err
	}
	if parsed.Host == "" {
		return Draft{}, fmt.Errorf("hf source must include owner")
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 2 {
		return Draft{}, fmt.Errorf("hf source must be hf://owner/repo/path")
	}
	revision := parsed.Query().Get("revision")
	if revision == "" {
		revision = "main"
	}
	digest, err := huggingFaceDigest(parsed.Query())
	if err != nil {
		return Draft{}, err
	}
	modelPath := strings.Join(parts[1:], "/")
	base := os.Getenv("MYCELIUM_HF_BASE_URL")
	if base == "" {
		base = "https://huggingface.co"
	}
	baseURL, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil {
		return Draft{}, err
	}
	baseURL.Path = path.Join(baseURL.Path, parsed.Host, parts[0], "resolve", revision, modelPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL.String(), nil)
	if err != nil {
		return Draft{}, err
	}
	if token := huggingFaceToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return downloadDraft(ctx, req, source, "huggingface", filepath.Base(modelPath), client, downloadOptions{
		requireContentLength: true,
		expectedDigest:       digest,
	})
}

func importOCI(ctx context.Context, source string, client httpDoer) (Draft, error) {
	parsed, err := url.Parse(source)
	if err != nil {
		return Draft{}, err
	}
	if parsed.Host == "" {
		return Draft{}, fmt.Errorf("oci source must include registry host")
	}
	repo, ref, err := splitOCIReference(strings.Trim(parsed.Path, "/"))
	if err != nil {
		return Draft{}, err
	}
	manifestURL := ociURL(parsed.Host, path.Join("v2", repo, "manifests", ref))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return Draft{}, err
	}
	req.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json")
	if token := os.Getenv("MYCELIUM_OCI_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return Draft{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Draft{}, fmt.Errorf("oci manifest fetch failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var manifest struct {
		Layers []struct {
			Digest      string            `json:"digest"`
			Size        int64             `json:"size"`
			Annotations map[string]string `json:"annotations"`
		} `json:"layers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return Draft{}, err
	}
	if len(manifest.Layers) == 0 || manifest.Layers[0].Digest == "" {
		return Draft{}, fmt.Errorf("oci manifest has no downloadable layers")
	}
	layer := manifest.Layers[0]
	name := layer.Annotations["org.opencontainers.image.title"]
	if name == "" {
		name = filepath.Base(repo)
	}
	blobURL := ociURL(parsed.Host, path.Join("v2", repo, "blobs", layer.Digest))
	blobReq, err := http.NewRequestWithContext(ctx, http.MethodGet, blobURL, nil)
	if err != nil {
		return Draft{}, err
	}
	if token := os.Getenv("MYCELIUM_OCI_TOKEN"); token != "" {
		blobReq.Header.Set("Authorization", "Bearer "+token)
	}
	draft, err := downloadDraft(ctx, blobReq, source, "oci", name, client, downloadOptions{})
	if err != nil {
		return Draft{}, err
	}
	if layer.Size > 0 && draft.Size != layer.Size {
		_ = os.Remove(draft.Path)
		return Draft{}, fmt.Errorf("oci layer size mismatch: manifest=%d downloaded=%d", layer.Size, draft.Size)
	}
	if err := verifyDigest(draft.Path, layer.Digest); err != nil {
		_ = os.Remove(draft.Path)
		return Draft{}, err
	}
	return draft, nil
}

func downloadDraft(ctx context.Context, req *http.Request, source, importer, name string, client httpDoer, opts downloadOptions) (Draft, error) {
	if err := ctx.Err(); err != nil {
		return Draft{}, err
	}
	if err := safeid.Validate("draft name", name); err != nil {
		return Draft{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return Draft{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Draft{}, fmt.Errorf("%s download failed: %s: %s", importer, resp.Status, strings.TrimSpace(string(body)))
	}
	if opts.requireContentLength && resp.ContentLength <= 0 {
		return Draft{}, fmt.Errorf("%s download must include a positive Content-Length", importer)
	}
	if resp.ContentLength > maxRemoteDraftBytes {
		return Draft{}, fmt.Errorf("%s download content length %d exceeds limit %d", importer, resp.ContentLength, maxRemoteDraftBytes)
	}
	tmp, err := os.CreateTemp("", "mycelium-"+importer+"-*-"+sanitizeTempName(name))
	if err != nil {
		return Draft{}, err
	}
	defer tmp.Close()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(tmp.Name())
		}
	}()
	size, err := io.Copy(tmp, io.LimitReader(resp.Body, maxRemoteDraftBytes+1))
	if err != nil {
		return Draft{}, err
	}
	if size > maxRemoteDraftBytes {
		return Draft{}, fmt.Errorf("%s download exceeds limit %d", importer, maxRemoteDraftBytes)
	}
	if err := ctx.Err(); err != nil {
		return Draft{}, err
	}
	if opts.expectedDigest != "" {
		if err := tmp.Close(); err != nil {
			return Draft{}, err
		}
		if err := verifyDigest(tmp.Name(), opts.expectedDigest); err != nil {
			return Draft{}, err
		}
	}
	keep = true
	return Draft{
		Source:   source,
		Importer: importer,
		Path:     tmp.Name(),
		Name:     name,
		Size:     size,
	}, nil
}

func huggingFaceDigest(query url.Values) (string, error) {
	digest := query.Get("digest")
	if digest == "" {
		if sha := query.Get("sha256"); sha != "" {
			digest = "sha256:" + sha
		}
	}
	if digest == "" {
		return "", fmt.Errorf("hf import requires sha256 digest")
	}
	if !strings.Contains(digest, ":") {
		digest = "sha256:" + digest
	}
	return digest, nil
}

func splitOCIReference(raw string) (string, string, error) {
	if raw == "" {
		return "", "", fmt.Errorf("oci source must include repository")
	}
	if repo, digest, ok := strings.Cut(raw, "@"); ok {
		if digest == "" {
			return "", "", fmt.Errorf("oci digest reference is empty")
		}
		return repo, digest, nil
	}
	lastSlash := strings.LastIndex(raw, "/")
	lastColon := strings.LastIndex(raw, ":")
	if lastColon > lastSlash {
		repo := raw[:lastColon]
		tag := raw[lastColon+1:]
		if tag == "" {
			return "", "", fmt.Errorf("oci tag reference is empty")
		}
		return repo, tag, nil
	}
	return raw, "latest", nil
}

func ociURL(host, p string) string {
	scheme := "https"
	if os.Getenv("MYCELIUM_OCI_INSECURE") == "1" {
		scheme = "http"
	}
	return (&url.URL{Scheme: scheme, Host: host, Path: "/" + p}).String()
}

func verifyDigest(filePath, digest string) error {
	algorithm, expected, ok := strings.Cut(digest, ":")
	if !ok || expected == "" {
		return fmt.Errorf("oci layer digest %q is malformed", digest)
	}
	var h hash.Hash
	switch algorithm {
	case "sha256":
		h = sha256.New()
	default:
		return fmt.Errorf("unsupported oci layer digest algorithm %q", algorithm)
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := io.Copy(h, file); err != nil {
		return err
	}
	actual := fmt.Sprintf("%x", h.Sum(nil))
	if !strings.EqualFold(actual, expected) {
		return fmt.Errorf("oci layer digest mismatch: expected %s:%s got %s:%s", algorithm, expected, algorithm, actual)
	}
	return nil
}

func huggingFaceToken() string {
	if token := os.Getenv("HF_TOKEN"); token != "" {
		return token
	}
	return os.Getenv("HUGGING_FACE_HUB_TOKEN")
}

func sanitizeTempName(name string) string {
	name = filepath.Base(name)
	name = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '.', r == '_', r == '-':
			return r
		default:
			return '-'
		}
	}, name)
	if name == "" || name == "." {
		return "model"
	}
	return name
}
