package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
)

// Declared snapshots missing from this host are fetched from the model hub
// during attempt preparation, outside any queue lock, straight into the
// worker's model snapshot cache. Consumers never pre-download or name hosts.

const (
	defaultModelHub = "https://huggingface.co"
	// Local secret (see --secret-root) for gated repositories. Only the worker
	// reads it; it never enters a job, a queue message or a log.
	modelHubSecret = "hf_token"
	// Hub metadata is a file list; 64 MiB bounds a malicious or broken reply,
	// not any real repository's listing.
	modelHubMetadataBytes = 64 << 20
)

// modelSnapshotIndex maps a declaration (repo, revision, files) to the cache
// entry holding its verified bytes, and remembers every snapshot size this
// worker has learned. Both are reported to the queue for disk admission and
// placement, and read from memory only on the heartbeat path.
type modelSnapshotIndex struct {
	mu      sync.Mutex
	entries map[string]SharedInput
	sizes   map[string]uint64
}

type modelSnapshotRecord struct {
	Snapshot ModelSnapshot `json:"snapshot"`
	Input    SharedInput   `json:"input"`
}

const modelSnapshotRecordName = "snapshot.json"

// snapshotIdentity is the declaration's cache identity, independent of which
// source (local hub or model hub) supplied the bytes.
func snapshotIdentity(m ModelSnapshot) string {
	files := slices.Clone(m.Files)
	slices.Sort(files)
	return identityHash(struct {
		Repo, Revision string
		Files          []string
	}{m.Repo, m.Revision, files})
}

func snapshotBytes(s SharedInput) uint64 { return uint64(max(0, s.bytes())) }

// load reads the small per-entry records once at worker start, so a restarted
// worker still knows (and reports) the snapshots it holds.
func (x *modelSnapshotIndex) load(c *SharedInputCache) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.entries, x.sizes = map[string]SharedInput{}, map[string]uint64{}
	dirs, _ := os.ReadDir(c.cache.Root)
	for _, d := range dirs {
		if !d.IsDir() || (FileRef{SHA256: d.Name()}).validate() != nil {
			continue
		}
		var r modelSnapshotRecord
		b, err := os.ReadFile(filepath.Join(c.cache.Root, d.Name(), modelSnapshotRecordName))
		if err != nil || json.Unmarshal(b, &r) != nil || r.Input.key() != d.Name() {
			continue
		}
		if _, err := os.Stat(filepath.Join(c.cache.Root, d.Name(), "ready.json")); err != nil {
			continue
		}
		id := snapshotIdentity(r.Snapshot)
		x.entries[id], x.sizes[id] = r.Input, snapshotBytes(r.Input)
		c.seedReady(d.Name())
	}
}

func (x *modelSnapshotIndex) lookup(id string) (SharedInput, bool) {
	if x == nil {
		return SharedInput{}, false
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	s, ok := x.entries[id]
	return s, ok
}

func (x *modelSnapshotIndex) learn(id string, bytes uint64) {
	if x == nil {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.sizes == nil {
		x.sizes = map[string]uint64{}
	}
	x.sizes[id] = bytes
}

// record runs after a successful acquisition, while the use lock is held.
func (x *modelSnapshotIndex) record(root string, m ModelSnapshot, s SharedInput) {
	if x == nil {
		return
	}
	id := snapshotIdentity(m)
	stored := SharedInput{NAS: s.NAS, Manifest: s.Manifest}
	if b, err := json.Marshal(modelSnapshotRecord{Snapshot: m, Input: stored}); err == nil {
		_ = writeFileAtomic(filepath.Join(root, stored.key(), modelSnapshotRecordName), b)
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.entries == nil {
		x.entries = map[string]SharedInput{}
	}
	x.entries[id] = stored
	if x.sizes == nil {
		x.sizes = map[string]uint64{}
	}
	x.sizes[id] = snapshotBytes(stored)
}

// report lists held declarations (whose entry is still verified/present in the
// cache) and every learned size. Memory only.
func (x *modelSnapshotIndex) report(ready []string) ([]string, map[string]uint64) {
	if x == nil {
		return nil, nil
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	var held []string
	for id, s := range x.entries {
		if slices.Contains(ready, s.key()) {
			held = append(held, id)
		}
	}
	slices.Sort(held)
	var sizes map[string]uint64
	if len(x.sizes) > 0 {
		sizes = make(map[string]uint64, len(x.sizes))
		for id, n := range x.sizes {
			sizes[id] = n
		}
	}
	return held, sizes
}

func writeFileAtomic(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(b)
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// modelHub is the worker's configured hub endpoint and optional credential.
type modelHub struct {
	endpoint *url.URL
	token    string
	client   *http.Client
}

// The endpoint is operator configuration (Worker.ModelHubURL, else the
// standard HF_ENDPOINT variable). It must be HTTPS; plain HTTP is accepted
// only on loopback, for laptop tests and local mirrors.
func (w Worker) modelHub() (*modelHub, error) {
	raw := w.ModelHubURL
	if raw == "" {
		raw = os.Getenv("HF_ENDPOINT")
	}
	if raw == "" {
		raw = defaultModelHub
	}
	u, err := url.Parse(strings.TrimRight(raw, "/"))
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || !(u.Scheme == "https" || u.Scheme == "http" && loopbackHost(u.Hostname())) {
		return nil, errors.New("model hub endpoint must be an HTTPS URL")
	}
	hub := &modelHub{endpoint: u, client: http.DefaultClient}
	if w.SecretRoot != "" {
		if _, err := os.Lstat(filepath.Join(w.SecretRoot, modelHubSecret)); err == nil {
			secrets, err := readModelSecrets(w.SecretRoot, map[string]string{"token": modelHubSecret}, w.Client.Token)
			if err != nil {
				return nil, err
			}
			hub.token = secrets["token"]
		}
	}
	return hub, nil
}

func loopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (h *modelHub) url(parts ...string) string {
	u := *h.endpoint
	escaped := make([]string, len(parts))
	for i, p := range parts {
		segments := strings.Split(p, "/")
		for j := range segments {
			segments[j] = url.PathEscape(segments[j])
		}
		escaped[i] = strings.Join(segments, "/")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.Join(escaped, "/")
	return u.String()
}

// get sends the credential only to the configured endpoint. Go's client drops
// Authorization on redirects to another host (the hub's signed CDN URLs).
func (h *modelHub) get(ctx context.Context, raw string, offset int64) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, err
	}
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	res, err := h.client.Do(req)
	if err != nil {
		return nil, modelHubHostFault("network", err)
	}
	return res, nil
}

// A hub refusal that no other host can repair fails the job with a typed,
// path-free predicate. Network, server and local disk faults are this host's:
// they carry no predicate, so the attempt is requeued as infrastructure loss.
func modelHubStatusError(code int, file bool) error {
	switch {
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return modelSnapshotError("model_snapshot_access", "permission_denied", nil)
	case code == http.StatusNotFound && file:
		return modelSnapshotError("model_snapshot_mismatch", "not_found", nil)
	case code == http.StatusNotFound:
		return modelSnapshotError("model_snapshot_revision", "not_found", nil)
	}
	return modelHubHostFault("io_error", fmt.Errorf("model hub HTTP %d", code))
}

func modelHubHostFault(class string, cause error) error {
	if class == "network" {
		class = "connection_refused"
	}
	return &runtimeError{diagnostic: RuntimeCause{Operation: "input_staging", Class: class}, cause: fmt.Errorf("model snapshot fetch: %w", cause)}
}

type hubSibling struct {
	Name   string `json:"rfilename"`
	Size   int64  `json:"size"`
	BlobID string `json:"blobId"`
	LFS    *struct {
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	} `json:"lfs"`
}

// resolveHubSnapshot builds the verified tree identity of an exact revision.
// Large (LFS) files are named by SHA-256 and are fetched later, only on a
// cache miss, verified while populating. Small files are named by their Git
// blob SHA-1, so they are fetched now into scratch and verified against that
// name to learn their SHA-256.
func (w Worker) resolveHubSnapshot(ctx context.Context, hub *modelHub, m ModelSnapshot, scratch string) (SharedInput, error) {
	res, err := hub.get(ctx, hub.url("api", "models", m.Repo, "revision", m.Revision)+"?blobs=true", 0)
	if err != nil {
		return SharedInput{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return SharedInput{}, modelHubStatusError(res.StatusCode, false)
	}
	var info struct {
		SHA      string       `json:"sha"`
		Siblings []hubSibling `json:"siblings"`
	}
	if err := json.NewDecoder(io.LimitReader(contextReader{ctx, res.Body}, modelHubMetadataBytes)).Decode(&info); err != nil {
		if ctx.Err() != nil {
			return SharedInput{}, ctx.Err()
		}
		return SharedInput{}, modelHubHostFault("invalid_response", err)
	}
	if info.SHA != m.Revision {
		return SharedInput{}, modelSnapshotError("model_snapshot_revision", "not_found", nil)
	}
	allowed := map[string]bool{}
	for _, f := range m.Files {
		allowed[f] = true
	}
	input := SharedInput{NAS: "snapshots/" + m.Revision, Manifest: FileManifest{}, sources: map[string]string{}, hub: true}
	large := map[string]bool{}
	for _, f := range info.Siblings {
		if len(allowed) > 0 && !allowed[f.Name] {
			continue
		}
		if !safeRelative(f.Name) {
			return SharedInput{}, modelSnapshotError("model_snapshot_escape", "permission_denied", nil)
		}
		switch {
		case f.LFS != nil:
			if !sha256Name.MatchString(f.LFS.SHA256) || f.LFS.Size < 0 {
				return SharedInput{}, modelSnapshotError("model_snapshot_mismatch", "invalid_response", nil)
			}
			input.Manifest[f.Name] = FileRef{SHA256: f.LFS.SHA256, SizeBytes: f.LFS.Size}
			large[f.Name] = true
		case modelCommitPattern.MatchString(f.BlobID):
			path := filepath.Join(scratch, f.BlobID)
			if err := hub.download(ctx, hub.url(m.Repo, "resolve", m.Revision, f.Name), path, -1); err != nil {
				return SharedInput{}, err
			}
			ref, err := modelBlobIdentity(ctx, path)
			if err != nil {
				return SharedInput{}, err
			}
			input.Manifest[f.Name], input.sources[f.Name] = ref, path
		default:
			return SharedInput{}, modelSnapshotError("model_snapshot_mismatch", "invalid_response", nil)
		}
		delete(allowed, f.Name)
	}
	if len(allowed) > 0 && len(m.Files) > 0 || len(input.Manifest) == 0 {
		return SharedInput{}, modelSnapshotError("model_snapshot_mismatch", "not_found", nil)
	}
	input.fetch = func(ctx context.Context, name, target string, ref FileRef) error {
		if !large[name] {
			return errors.New("model hub file has no source")
		}
		return hub.fetchVerified(ctx, hub.url(m.Repo, "resolve", m.Revision, name), target, ref)
	}
	return input, nil
}

// fetchVerified resumes an interrupted prefix, verifies size and SHA-256, and
// retries once from zero before refusing bytes that do not match the hub's
// declared identity.
func (h *modelHub) fetchVerified(ctx context.Context, raw, target string, ref FileRef) error {
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return modelHubHostFault(diskClass(err), err)
	}
	// Attempt 0 may resume a retained prefix; attempt 1 always starts fresh.
	for attempt := 0; ; attempt++ {
		var offset int64
		if st, err := os.Lstat(target); err == nil {
			if st.Mode().IsRegular() && st.Size() <= ref.SizeBytes {
				offset = st.Size()
			} else if err := os.Remove(target); err != nil {
				return modelHubHostFault(diskClass(err), err)
			}
		}
		if offset == 0 || offset < ref.SizeBytes {
			if err := h.download(ctx, raw, target, offset); err != nil {
				return err
			}
		}
		err := verifyFile(ctx, target, ref)
		if err == nil || ctx.Err() != nil {
			return err
		}
		if err := os.Remove(target); err != nil {
			return modelHubHostFault(diskClass(err), err)
		}
		if attempt > 0 {
			return modelSnapshotError("model_snapshot_mismatch", "io_error", nil)
		}
	}
}

// download writes the body at offset (appending a resumed range). offset -1
// means a small file fetched whole, without resume.
func (h *modelHub) download(ctx context.Context, raw, target string, offset int64) error {
	res, err := h.get(ctx, raw, max(0, offset))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	flags := os.O_CREATE | os.O_WRONLY
	switch {
	case offset > 0 && res.StatusCode == http.StatusPartialContent:
	case res.StatusCode == http.StatusOK:
		flags |= os.O_TRUNC // Range ignored, or a fresh body.
	default:
		return modelHubStatusError(res.StatusCode, true)
	}
	f, err := os.OpenFile(target, flags, 0600)
	if err != nil {
		return modelHubHostFault(diskClass(err), err)
	}
	if offset > 0 && res.StatusCode == http.StatusPartialContent {
		_, err = f.Seek(offset, io.SeekStart)
	}
	if err == nil {
		_, err = io.Copy(f, contextReader{ctx, res.Body})
	}
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return modelHubHostFault(diskClass(err), err)
	}
	return nil
}

func diskClass(err error) string {
	if errors.Is(err, syscall.ENOSPC) {
		return "storage_full"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return "network"
	}
	return "io_error"
}

// withHubLock serializes one declaration's resolution and population across
// every worker sharing this cache, so concurrent jobs share one download.
func (c *SharedInputCache) withHubLock(ctx context.Context, id string, run func(scratch string) error) error {
	lock, err := c.cache.lock(ctx, "hub-"+id+".fetch", syscall.LOCK_EX)
	if err != nil {
		return err
	}
	defer unlockEnvironment(lock)
	// Scratch is owned by this lock; anything left there crashed with its writer.
	scratch := filepath.Join(c.cache.Root, "hub-fetch-"+id)
	if err := os.RemoveAll(scratch); err != nil {
		return modelHubHostFault(diskClass(err), err)
	}
	if err := os.MkdirAll(scratch, 0700); err != nil {
		return modelHubHostFault(diskClass(err), err)
	}
	defer os.RemoveAll(scratch)
	return run(scratch)
}

// seedReady marks an entry found at worker start as held. ensure() still
// verifies its recorded metadata signature before any use.
func (c *SharedInputCache) seedReady(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.verified == nil {
		c.verified = map[string]string{}
	}
	if _, ok := c.verified[key]; !ok {
		c.verified[key] = ""
	}
}

var errModelIndexStale = errors.New("indexed model snapshot entry is no longer cached")
