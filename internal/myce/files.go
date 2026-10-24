package myce

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
	tus "github.com/tus/tusd/v2/pkg/handler"
	"golang.org/x/exp/slog"
)

// FileRef identifies immutable bytes. Names live in a separate manifest.
type FileRef struct {
	SHA256    string `json:"sha256" yaml:"sha256"`
	SizeBytes int64  `json:"size_bytes" yaml:"size_bytes"`
}
type FileManifest map[string]FileRef

func (f FileRef) validate() error {
	hash, err := hex.DecodeString(f.SHA256)
	if err != nil || len(hash) != sha256.Size || f.SHA256 != strings.ToLower(f.SHA256) || f.SizeBytes < 0 {
		return errors.New("file requires lowercase SHA-256 and nonnegative size")
	}
	return nil
}
func (m FileManifest) validate() error {
	files := make([]ModelFile, 0, len(m))
	for name, f := range m {
		if err := f.validate(); err != nil {
			return err
		}
		if !safeRelative(name) {
			return errors.New("file manifest requires clean relative names")
		}
		files = append(files, ModelFile{Path: name, SHA256: f.SHA256, SizeBytes: f.SizeBytes})
	}
	if len(files) == 0 {
		return nil
	}
	_, _, err := ModelFilesIdentity(files)
	return err
}
func IdentifyFile(path string) (FileRef, error) {
	var err error
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return FileRef{}, err
	}
	path = filepath.Join(parent, filepath.Base(path))
	if err = noModelSymlinks(path, false); err != nil {
		return FileRef{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return FileRef{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return FileRef{}, err
	}
	if !st.Mode().IsRegular() {
		return FileRef{}, errors.New("file must be regular")
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return FileRef{}, err
	}
	if n != st.Size() {
		return FileRef{}, errors.New("file changed during identification")
	}
	return FileRef{hex.EncodeToString(h.Sum(nil)), n}, nil
}
func verifyFile(ctx context.Context, path string, ref FileRef) error {
	if err := ref.validate(); err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return err
	}
	path = filepath.Join(parent, filepath.Base(path))
	if err := noModelSymlinks(path, false); err != nil {
		return err
	}
	return verifyModelContext(ctx, path, Model{Name: "file", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes})
}

type FileStore struct {
	root      string
	limit     int64
	mu        sync.Mutex
	handler   http.Handler
	syncDir   func(string) error // Durability failure injection.
	expired   func(string) bool
	lifecycle *sync.RWMutex
	clock     Clock
}
type FileConfig struct {
	Checkpoints bool `json:"checkpoints"`
}

// Stored inputs and their results use the queue file store independently of
// checkpoint mode. FileTransport continues to select file-store checkpoints.
func (j Job) storedFileResults() bool {
	return j.FileTransport || len(j.Spec.Files) > 0
}

func (q *Queue) ConfigureFileStore(root string, limit int64, checkpoints bool) error {
	if limit <= 0 {
		return errors.New("file store limit must be positive")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if err = PrepareStateRoots(root); err != nil {
		return err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	if err = noModelSymlinks(root, false); err != nil {
		return err
	}
	for _, dir := range []string{root, filepath.Join(root, "blobs"), filepath.Join(root, "uploads")} {
		if err = os.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}
	s := &FileStore{root: root, limit: limit, syncDir: syncWorkerDir, expired: q.expiredBlob, lifecycle: &q.fileLifecycle, clock: q.clock}
	fs := filestore.New(filepath.Join(root, "uploads"))
	fs.DirModePerm = 0700
	fs.FileModePerm = 0600
	composer := tus.NewStoreComposer()
	fs.UseIn(composer)
	filelocker.New(fs.Path).UseIn(composer)
	h, err := tus.NewHandler(tus.Config{
		BasePath: "/api/files/uploads/", StoreComposer: composer, MaxSize: limit, DisableDownload: true, DisableTermination: true, DisableConcatenation: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		PreUploadCreateCallback: func(e tus.HookEvent) (tus.HTTPResponse, tus.FileInfoChanges, error) {
			ref := FileRef{e.Upload.MetaData["sha256"], e.Upload.Size}
			if err := ref.validate(); err != nil || e.Upload.SizeIsDeferred {
				return tus.HTTPResponse{}, tus.FileInfoChanges{}, errors.New("invalid file upload identity")
			}
			return tus.HTTPResponse{}, tus.FileInfoChanges{ID: ref.SHA256}, nil
		},
		PreFinishResponseCallback: func(e tus.HookEvent) (tus.HTTPResponse, error) {
			return tus.HTTPResponse{}, s.publish(e.Context, FileRef{e.Upload.MetaData["sha256"], e.Upload.Size})
		},
	})
	if err != nil {
		return err
	}
	s.handler = http.StripPrefix("/api/files/uploads/", h)
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	q.mu.Lock()
	defer q.mu.Unlock()
	q.files = s
	q.fileCheckpoints = checkpoints
	return nil
}
func (s *FileStore) blob(ref FileRef) string { return filepath.Join(s.root, "blobs", ref.SHA256) }
func (s *FileStore) verify(ctx context.Context, ref FileRef) error {
	return verifyFile(ctx, s.blob(ref), ref)
}

// Publication/HEAD callers hold lifecycle.RLock. Refresh even deduplicated
// bytes: they may be about to acquire a new job or attempt reference.
func (s *FileStore) refreshBlob(path string) error {
	now := s.clock.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
func (s *FileStore) publish(ctx context.Context, ref FileRef) error {
	if err := ref.validate(); err != nil {
		return err
	}
	source := filepath.Join(s.root, "uploads", ref.SHA256)
	if err := verifyFile(ctx, source, ref); err != nil {
		return errors.New("uploaded file size or SHA-256 mismatch")
	}
	f, err := os.OpenFile(source, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err = errors.Join(f.Sync(), f.Close()); err != nil {
		return err
	}
	if err = os.Link(source, s.blob(ref)); err != nil && !os.IsExist(err) {
		return err
	}
	if err = s.verify(ctx, ref); err != nil {
		return err
	}
	if err = s.refreshBlob(s.blob(ref)); err != nil {
		return err
	}
	if err = s.syncDir(filepath.Join(s.root, "blobs")); err != nil {
		return err
	}
	// Keep only the authoritative link after publication. A later tus creation
	// must never truncate a surviving hard link to the immutable blob.
	if err = os.Remove(source); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = os.Remove(source + ".info"); err != nil && !os.IsNotExist(err) {
		return err
	}
	return s.syncDir(filepath.Join(s.root, "uploads"))
}
func (s *FileStore) serveUpload(w http.ResponseWriter, r *http.Request) {
	// Fence upload publication against unlink without serializing other uploads.
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	id := strings.TrimPrefix(r.URL.Path, "/api/files/uploads/")
	if id != "" {
		if err := (FileRef{SHA256: id}).validate(); err != nil {
			http.Error(w, "invalid upload", 400)
			return
		}
		s.handler.ServeHTTP(w, r)
		return
	}
	if r.Method != "POST" {
		s.handler.ServeHTTP(w, r)
		return
	}
	// tusd serializes each PATCH/HEAD; only creation/admission is global.
	s.mu.Lock()
	defer s.mu.Unlock()
	length, err := strconv.ParseInt(r.Header.Get("Upload-Length"), 10, 64)
	var hash string
	for _, field := range strings.Split(r.Header.Get("Upload-Metadata"), ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(field), " ")
		if k == "sha256" {
			b, e := base64.StdEncoding.DecodeString(v)
			if e == nil {
				hash = string(b)
			}
		}
	}
	ref := FileRef{hash, length}
	if err != nil || ref.validate() != nil || r.Header.Get("Upload-Defer-Length") != "" || r.Header.Get("Upload-Concat") != "" {
		http.Error(w, "invalid upload identity", 400)
		return
	}
	if s.verify(r.Context(), ref) == nil {
		if e := s.refreshBlob(s.blob(ref)); e != nil {
			http.Error(w, "storage freshness unavailable", 503)
			return
		}
		if e := s.syncDir(filepath.Join(s.root, "blobs")); e != nil {
			http.Error(w, "storage durability unavailable", 503)
			return
		}
		w.Header().Set("Tus-Resumable", "1.0.0")
		w.Header().Set("Location", "/api/files/uploads/"+hash)
		w.WriteHeader(201)
		return
	}
	if st, e := os.Stat(filepath.Join(s.root, "uploads", hash+".info")); e == nil && st.Mode().IsRegular() {
		// A repeated create (including a lost response) addresses the same tus upload.
		w.Header().Set("Tus-Resumable", "1.0.0")
		w.Header().Set("Location", "/api/files/uploads/"+hash)
		w.WriteHeader(201)
		return
	}
	used := int64(0)
	remaining := int64(0)
	blobs, e := os.ReadDir(filepath.Join(s.root, "blobs"))
	if e != nil {
		http.Error(w, "storage unavailable", 503)
		return
	}
	for _, entry := range blobs {
		st, e := entry.Info()
		if e != nil || !st.Mode().IsRegular() || st.Size() > s.limit-used {
			http.Error(w, "file store full or unavailable", 507)
			return
		}
		used += st.Size()
	}
	entries, e := os.ReadDir(filepath.Join(s.root, "uploads"))
	if e != nil {
		http.Error(w, "storage unavailable", 503)
		return
	}
	fs := filestore.New(filepath.Join(s.root, "uploads"))
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".info") {
			hash := strings.TrimSuffix(entry.Name(), ".info")
			if _, e := os.Stat(filepath.Join(s.root, "blobs", hash)); e == nil {
				continue
			}
			upload, e := fs.GetUpload(r.Context(), strings.TrimSuffix(entry.Name(), ".info"))
			if e != nil {
				http.Error(w, "storage metadata unavailable", 503)
				return
			}
			info, e := upload.GetInfo(r.Context())
			if e != nil || info.Size < 0 || info.Size > s.limit-used {
				http.Error(w, "file store full", 507)
				return
			}
			used += info.Size
			remaining += max(0, info.Size-info.Offset)
		}
	}
	free, e := diskFree(s.root)
	if e != nil {
		http.Error(w, "storage unavailable", 503)
		return
	}
	if length > s.limit-used || length > free-remaining {
		http.Error(w, "file store full", 507)
		return
	}
	if _, e := os.Lstat(filepath.Join(s.root, "uploads", hash)); e == nil {
		// No .info means no resumable upload authority; do not silently overwrite.
		http.Error(w, "unindexed upload requires operator recovery", 409)
		return
	}
	s.handler.ServeHTTP(w, r)
}
func (s *FileStore) serveBlob(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		// HEAD is upload deduplication authority. Fence verification and refresh
		// together; GET needs only its open descriptor to survive collection.
		s.lifecycle.RLock()
		defer s.lifecycle.RUnlock()
	}
	hash := r.PathValue("hash")
	if (FileRef{SHA256: hash}).validate() != nil {
		http.Error(w, "invalid file", 400)
		return
	}
	missing := func() {
		if s.expired != nil && s.expired(hash) {
			w.Header().Set("X-Myce-Error-Code", "result_expired")
			respond(w, nil, resultExpired())
		} else {
			http.NotFound(w, r)
		}
	}
	path := filepath.Join(s.root, "blobs", hash)
	if err := noModelSymlinks(path, false); err != nil {
		missing()
		return
	}
	f, err := os.Open(path)
	if err != nil {
		missing()
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		http.NotFound(w, r)
		return
	}
	// Recheck disk bytes before serving: an ETag must never certify corrupt data.
	h := sha256.New()
	n, verifyErr := io.Copy(h, contextReader{r.Context(), f})
	if verifyErr != nil || n != st.Size() || hex.EncodeToString(h.Sum(nil)) != hash {
		http.Error(w, "stored file corrupt", 500)
		return
	}
	if r.Method == http.MethodHead {
		if err = s.refreshBlob(path); err != nil {
			http.Error(w, "file freshness unavailable", 503)
			return
		}
	}
	if err = s.syncDir(filepath.Join(s.root, "blobs")); err != nil {
		http.Error(w, "file durability unavailable", 503)
		return
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		http.Error(w, "stored file seek failed", 500)
		return
	}
	w.Header().Set("ETag", `"`+hash+`"`)
	w.Header().Set("Cache-Control", "private, immutable")
	http.ServeContent(w, r, hash, time.Time{}, f)
}
func (a API) registerFiles(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/files/config", func(w http.ResponseWriter, r *http.Request) { respond(w, FileConfig{a.Queue.fileCheckpoints}, nil) })
	if a.Queue.files == nil {
		return
	}
	mux.HandleFunc("/api/files/uploads/", a.Queue.files.serveUpload)
	mux.HandleFunc("GET /api/files/blobs/{hash}", a.Queue.files.serveBlob)
	mux.HandleFunc("POST /api/files/checkpoint", a.checkpointHandler)
}

func (c Client) fileRequest(ctx context.Context, method, path string, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	// Never redirect credentials or data to a different destination.
	local := *hc
	local.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	res, err := local.Do(req)
	if err != nil {
		return nil, errors.New("file transfer connection failed")
	}
	return res, nil
}
func fileResponse(res *http.Response, want int) error {
	defer res.Body.Close()
	_, err := io.Copy(io.Discard, res.Body)
	if res.StatusCode != want {
		return fileTransferError(res, "file transfer failed")
	}
	return err
}
func setUploadHeaders(req *http.Request, ref FileRef) {
	req.Header.Set("Upload-Length", strconv.FormatInt(ref.SizeBytes, 10))
	req.Header.Set("Upload-Metadata", "sha256 "+base64.StdEncoding.EncodeToString([]byte(ref.SHA256)))
}
func (c Client) HasFile(ctx context.Context, ref FileRef) error {
	if err := ref.validate(); err != nil {
		return err
	}
	res, err := c.fileRequest(ctx, "HEAD", "/api/files/blobs/"+ref.SHA256, nil, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fileTransferError(res, "file unavailable")
	}
	if res.ContentLength != ref.SizeBytes || res.Header.Get("ETag") != `"`+ref.SHA256+`"` {
		return errors.New("stored file identity mismatch")
	}
	return nil
}
func (c Client) UploadFile(ctx context.Context, path string, ref FileRef) error {
	if err := verifyFile(ctx, path, ref); err != nil {
		return err
	}
	if err := c.HasFile(ctx, ref); err == nil {
		return nil
	} else {
		var he *HTTPError
		if !errors.As(err, &he) || (he.Status != 404 && he.Status != 410) {
			return err
		}
	}
	headers := map[string]string{"Tus-Resumable": "1.0.0", "Upload-Length": strconv.FormatInt(ref.SizeBytes, 10), "Upload-Metadata": "sha256 " + base64.StdEncoding.EncodeToString([]byte(ref.SHA256))}
	res, err := c.fileRequest(ctx, "POST", "/api/files/uploads/", nil, headers)
	if err != nil {
		return err
	}
	if err = fileResponse(res, 201); err != nil {
		return err
	}
	if err = c.HasFile(ctx, ref); err == nil {
		return nil
	}
	endpoint := "/api/files/uploads/" + ref.SHA256
	res, err = c.fileRequest(ctx, "HEAD", endpoint, nil, map[string]string{"Tus-Resumable": "1.0.0"})
	if err != nil {
		return err
	}
	offset, e := strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64)
	size, se := strconv.ParseInt(res.Header.Get("Upload-Length"), 10, 64)
	err = fileResponse(res, 200)
	if err != nil {
		return err
	}
	if e != nil || se != nil || size != ref.SizeBytes || offset < 0 || offset > size {
		return errors.New("invalid resumable upload offset/size")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	// Bounded PATCH bodies let a slow connection progress without one huge request.
	for {
		n := min(int64(4<<20), size-offset)
		res, err = c.fileRequest(ctx, "PATCH", endpoint, io.LimitReader(f, n), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": strconv.FormatInt(offset, 10), "Content-Type": "application/offset+octet-stream"})
		if err != nil {
			return err
		}
		next, e := strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64)
		if err = fileResponse(res, 204); err != nil {
			return err
		}
		if e != nil || next != offset+n {
			return errors.New("invalid upload acknowledgement")
		}
		offset = next
		if offset == size {
			break
		}
	}
	return c.HasFile(ctx, ref)
}
func (c Client) DownloadFile(ctx context.Context, ref FileRef, dest string) error {
	if err := ref.validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(dest))
	if err != nil {
		return err
	}
	dest = filepath.Join(parent, filepath.Base(dest))
	if err := noModelSymlinks(dest, true); err != nil {
		return err
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	offset := st.Size()
	if offset > ref.SizeBytes {
		return errors.New("file partial exceeds declared size")
	}
	if offset == ref.SizeBytes {
		if ref.SizeBytes == 0 {
			if err := c.HasFile(ctx, ref); err != nil {
				return err
			}
		}
		return verifyFile(ctx, dest, ref)
	}
	headers := map[string]string{"If-Range": `"` + ref.SHA256 + `"`}
	if offset > 0 {
		headers["Range"] = fmt.Sprintf("bytes=%d-", offset)
	}
	res, err := c.fileRequest(ctx, "GET", "/api/files/blobs/"+ref.SHA256, nil, headers)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 && res.StatusCode != 206 {
		return fileTransferError(res, "download failed")
	}
	if res.Header.Get("ETag") != `"`+ref.SHA256+`"` {
		return errors.New("download identity mismatch")
	}
	if res.StatusCode == 200 {
		if err = f.Truncate(0); err != nil {
			return err
		}
		offset = 0
	} else if res.StatusCode == 206 && offset > 0 {
		expected := fmt.Sprintf("bytes %d-%d/%d", offset, ref.SizeBytes-1, ref.SizeBytes)
		if res.Header.Get("Content-Range") != expected {
			return errors.New("invalid download range")
		}
	} else {
		return &HTTPError{Status: res.StatusCode, Message: "download failed"}
	}
	if res.ContentLength != ref.SizeBytes-offset {
		return errors.New("download size mismatch")
	}
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	n, copyErr := io.Copy(f, io.LimitReader(res.Body, ref.SizeBytes-offset+1))
	err = errors.Join(copyErr, f.Sync())
	if err != nil {
		return err
	}
	if n != ref.SizeBytes-offset {
		return errors.New("download interrupted or oversized")
	}
	return verifyFile(ctx, dest, ref)
}

func fileTransferError(res *http.Response, message string) error {
	if res.StatusCode == 410 && res.Header.Get("X-Myce-Error-Code") == "result_expired" {
		return resultExpired()
	}
	return &HTTPError{Status: res.StatusCode, Message: message}
}
