package myce

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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
	reservedOutputs func(map[string]storedPayload) uint64
	diskProbe       func(string) (DiskReading, error)
	root            string
	limit           int64
	mu              sync.Mutex
	handler         http.Handler
	uploadLocker    tus.Locker
	syncDir         func(string) error // Durability failure injection.
	expired         func(string, ...context.Context) bool
	lifecycle       *sync.RWMutex
	clock           Clock
	recordUpload    func(hash, purpose string, created bool, contexts ...context.Context) error
	publishUpload   func(hash string, contexts ...context.Context) error
	recordBundle    func(hash string, created bool, contexts ...context.Context) error
}
type FileConfig struct {
	Checkpoints bool `json:"checkpoints"`
}

type fileStoreConfiguration struct {
	root        string
	limit       int64
	checkpoints bool
}

// Stored inputs and their results use the queue file store independently of
// checkpoint mode. FileTransport continues to select file-store checkpoints.
func (j Job) storedFileResults() bool {
	if j.Spec.OutputDelivery == asyncNAS || j.FileTransport || len(jobInputFiles(&j)) > 0 {
		return true
	}
	for _, input := range j.Spec.SharedInputs {
		if !j.Spec.extensionJob() || input.NAS == "" {
			return true
		}
	}
	return false
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
	s := &FileStore{reservedOutputs: q.reservedOutputBytes, root: root, limit: limit, syncDir: syncWorkerDir, expired: q.expiredBlob, lifecycle: &q.fileLifecycle, clock: q.clock, recordUpload: q.recordFileUpload, publishUpload: q.publishFileUpload, recordBundle: q.recordBundleUpload}
	fs := filestore.New(filepath.Join(root, "uploads"))
	fs.DirModePerm = 0700
	fs.FileModePerm = 0600
	composer := tus.NewStoreComposer()
	fs.UseIn(composer)
	locker := filelocker.New(fs.Path)
	s.uploadLocker = locker
	locker.UseIn(composer)
	h, err := tus.NewHandler(tus.Config{
		BasePath: "/api/files/uploads/", StoreComposer: composer, MaxSize: limit, DisableDownload: true, DisableTermination: true, DisableConcatenation: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		PreUploadCreateCallback: func(e tus.HookEvent) (tus.HTTPResponse, tus.FileInfoChanges, error) {
			ref := FileRef{e.Upload.MetaData["sha256"], e.Upload.Size}
			if err := ref.validate(); err != nil || e.Upload.SizeIsDeferred {
				return tus.HTTPResponse{}, tus.FileInfoChanges{}, errors.New("invalid file upload identity")
			}
			if err := s.recordUpload(ref.SHA256, e.Upload.MetaData["purpose"], true, e.Context); err != nil {
				return tus.HTTPResponse{}, tus.FileInfoChanges{}, err
			}
			return tus.HTTPResponse{}, tus.FileInfoChanges{ID: ref.SHA256}, nil
		},
		PreFinishResponseCallback: func(e tus.HookEvent) (tus.HTTPResponse, error) {
			ref := FileRef{e.Upload.MetaData["sha256"], e.Upload.Size}
			if err := s.publish(e.Context, ref); err != nil {
				return tus.HTTPResponse{}, err
			}
			// An internal writer may have created the same blob after this
			// upload's POST. Publication must still honor the import's custody.
			if err := s.recordUpload(ref.SHA256, e.Upload.MetaData["purpose"], false, e.Context); err != nil {
				return tus.HTTPResponse{}, err
			}
			return tus.HTTPResponse{}, s.publishUpload(ref.SHA256, e.Context)
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
	if q.files != nil && q.files.root != root && len(q.bundleReferences()) > 0 {
		return errors.New("bundle storage already bound; select the file store with WithFileStore when opening the queue")
	}
	if q.files != nil && q.files.root != root {
		for _, j := range q.s.Jobs {
			if deliveryPending(j) || j.Spec.OutputDelivery == asyncNAS && !terminal(j.State) {
				return errors.New("asynchronous output custody binds the file store; keep its root unchanged")
			}
		}
	}
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
		response := &uploadResponse{ResponseWriter: w}
		s.handler.ServeHTTP(response, r)
		if response.status >= 400 {
			return
		}
		if err := s.refreshUpload(id); err != nil {
			log.Printf("upload freshness unavailable: %v", err)
		}
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
	var hash, purpose string
	for _, field := range strings.Split(r.Header.Get("Upload-Metadata"), ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(field), " ")
		if k == "sha256" || k == "purpose" {
			b, e := base64.StdEncoding.DecodeString(v)
			if e == nil {
				if k == "sha256" {
					hash = string(b)
				} else {
					purpose = string(b)
				}
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
		if e := s.recordUpload(hash, purpose, false, r.Context()); e != nil {
			http.Error(w, "upload purpose persistence unavailable", 503)
			return
		}
		w.Header().Set("Tus-Resumable", "1.0.0")
		w.Header().Set("Location", "/api/files/uploads/"+hash)
		w.WriteHeader(201)
		return
	}
	if st, e := os.Stat(filepath.Join(s.root, "uploads", hash+".info")); e == nil && st.Mode().IsRegular() {
		// A repeated create (including a lost response) addresses the same tus upload.
		if e := s.refreshUpload(hash); e != nil {
			http.Error(w, "storage freshness unavailable", 503)
			return
		}
		if e := s.recordUpload(hash, purpose, false, r.Context()); e != nil {
			http.Error(w, "upload purpose persistence unavailable", 503)
			return
		}
		w.Header().Set("Tus-Resumable", "1.0.0")
		w.Header().Set("Location", "/api/files/uploads/"+hash)
		w.WriteHeader(201)
		return
	}
	status, payloads, e := s.capacity(r.Context())
	if e != nil {
		status.Error = "file store capacity unavailable"
	}
	// A declared output becomes allocated in this creation. Preserve every
	// other estimate/identity reservation before admitting these bytes.
	if payloads == nil {
		payloads = map[string]storedPayload{}
	}
	payloads[hash] = storedPayload{length: uint64(length)}
	need := addDiskBytes(uint64(length), s.reservedOutputs(payloads))
	if err := fileStoreRefusal(status, need); err != nil {
		respond(w, nil, err)
		return
	}
	if _, e := os.Lstat(filepath.Join(s.root, "uploads", hash)); e == nil {
		// No .info means no resumable upload authority; do not silently overwrite.
		http.Error(w, "unindexed upload requires operator recovery", 409)
		return
	}
	s.handler.ServeHTTP(w, r)
	if err := s.refreshUpload(hash); err != nil {
		log.Printf("upload freshness unavailable: %v", err)
	}
}

// Keep tus streaming and ResponseController capabilities while observing errors.
type uploadResponse struct {
	http.ResponseWriter
	status int
}

func (w *uploadResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *uploadResponse) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (s *FileStore) serveBlob(w http.ResponseWriter, r *http.Request, metadata bool) {
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
		if s.expired != nil && s.expired(hash, r.Context()) {
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
	if metadata {
		// This is only a size hint for the verification budget, never durability or
		// identity authority. Callers must still verify with HEAD before eviction.
		respond(w, FileRef{SHA256: hash, SizeBytes: st.Size()}, nil)
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
	// An explicit import HEAD promotes an eligible input to an unmarked import.
	// Ordinary HasFile/HEAD probes never change its purpose. This avoids POST's
	// global upload-creation lock for duplicate checkpoints and other imports.
	if r.Method == http.MethodHead && r.Header.Get("X-Myce-Upload-Purpose") == fileImportUploadPurpose {
		if err = s.recordUpload(hash, fileImportUploadPurpose, false, r.Context()); err != nil {
			http.Error(w, "upload purpose persistence unavailable", 503)
			return
		}
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		http.Error(w, "stored file seek failed", 500)
		return
	}
	w.Header().Set("ETag", `"`+hash+`"`)
	w.Header().Set("Cache-Control", "private, immutable")
	http.ServeContent(w, r, hash, time.Time{}, f)
}
func (a API) registerFiles(mux *requestMux) {
	mux.HandleFunc("GET /api/files/config", func(w http.ResponseWriter, r *http.Request) { respond(w, FileConfig{a.Queue.fileCheckpoints}, nil) })
	if a.Queue.files == nil {
		return
	}
	mux.HandleFunc("POST /api/files/preflight", func(w http.ResponseWriter, r *http.Request) {
		var files FileManifest
		if !decode(w, r, &files) {
			return
		}
		respondOperation(w, map[string]bool{"ok": true}, a.Queue.PreflightFileStore(r.Context(), files), "job_spec_invalid")
	})
	mux.HandleFunc("/api/files/uploads/", a.Queue.files.serveUpload)
	mux.HandleFunc("GET /api/files/blobs/{hash}", func(w http.ResponseWriter, r *http.Request) { a.Queue.files.serveBlob(w, r, false) })
	mux.HandleFunc("GET /api/files/blobs/{hash}/metadata", func(w http.ResponseWriter, r *http.Request) { a.Queue.files.serveBlob(w, r, true) })
	mux.HandleFunc("POST /api/files/checkpoint", a.checkpointHandler)
}

// File transfers have no whole-request timeout (#218). Each network operation
// renews one idle budget: at least 60 s (past TCP retransmission backoffs), or
// 30 s plus two full-blob hash passes at a conservative disk rate of 100 MiB/s.
// The queue hashes before sending headers and twice when publishing an upload.
const (
	fileResponseBudget       = 30 * time.Second
	fileVerifyBytesPerSecond = 100 << 20
	fileTransferIdle         = 60 * time.Second
)

var errFileTransferStalled = errors.New("file transfer stalled")

func (c Client) fileTimeout(size int64) time.Duration {
	if c.transferIdle != 0 {
		return c.transferIdle
	}
	return max(fileTransferIdle, fileResponseBudget+time.Duration(2*float64(max(size, 0))/fileVerifyBytesPerSecond*float64(time.Second)))
}

// Network deadlines use real time, outside queue/scheduling Clock decisions.
// Refresh both directions: net/http can already be waiting for the response
// while an upload is still progressing on the same connection.
type fileTransferConn struct {
	net.Conn
	idle time.Duration
}

func (c fileTransferConn) Read(p []byte) (int, error) {
	if err := c.SetDeadline(time.Now().Add(c.idle)); err != nil {
		return 0, err
	}
	n, err := c.Conn.Read(p)
	return n, fileTransferTimeout(err, c.idle)
}
func (c fileTransferConn) Write(p []byte) (int, error) {
	// Observe partial socket writes, including progress within one TLS record.
	total := 0
	for len(p) > 0 {
		if err := c.SetDeadline(time.Now().Add(c.idle)); err != nil {
			return total, err
		}
		n, err := fileWriteSome(c.Conn, p)
		total += n
		p = p[n:]
		if err != nil {
			// Some injected in-memory connections expose partial progress only
			// with a timeout, as handled by x/net/http2's WriteByteTimeout.
			var timeout net.Error
			if n > 0 && errors.As(err, &timeout) && timeout.Timeout() {
				continue
			}
			return total, fileTransferTimeout(err, c.idle)
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}

func fileTransferTimeout(err error, idle time.Duration) error {
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return fmt.Errorf("%w: no network progress for %s", errFileTransferStalled, idle)
	}
	return err
}

// Custom hooks can perform TLS setup themselves, outside our socket wrapper.
// Bound setup separately from transfer progress. A legacy hook cannot be
// cancelled, so close its late result; its goroutine lasts until it returns.
func fileTransferDial(ctx context.Context, idle time.Duration, dial func(context.Context) (net.Conn, error)) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, idle)
	defer cancel()
	type result struct {
		conn net.Conn
		err  error
	}
	done := make(chan result)
	go func() {
		conn, err := dial(ctx)
		select {
		case done <- result{conn, err}:
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
		}
	}()
	select {
	case r := <-done:
		return r.conn, fileTransferTimeout(r.err, idle)
	case <-ctx.Done():
		return nil, fileTransferTimeout(ctx.Err(), idle)
	}
}

// size is the full blob the queue may verify before replying; 0 when none.
func (c Client) fileRequest(ctx context.Context, method, path string, body io.Reader, headers map[string]string, sizes ...int64) (*http.Response, error) {
	var size int64
	if len(sizes) != 0 {
		size = sizes[0]
	}
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
		hc = &http.Client{}
	}
	local := *hc
	local.Timeout = 0
	local.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base := local.Transport
	if base == nil {
		// Own the dial path so file deadlines do not depend on API transport defaults.
		base = &http.Transport{Proxy: http.ProxyFromEnvironment}
	}
	if transport, ok := base.(*http.Transport); ok {
		transport = transport.Clone()
		idle := c.fileTimeout(size)
		dial := transport.DialContext
		if dial == nil {
			if legacy := transport.Dial; legacy != nil {
				dial = func(_ context.Context, network, addr string) (net.Conn, error) {
					return legacy(network, addr)
				}
			} else {
				dial = (&net.Dialer{Timeout: idle}).DialContext
			}
		}
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := fileTransferDial(ctx, idle, func(ctx context.Context) (net.Conn, error) {
				return dial(ctx, network, addr)
			})
			if err != nil {
				return nil, fileTransferTimeout(err, idle)
			}
			return fileTransferConn{conn, idle}, nil
		}
		// TLS normally runs over the wrapped TCP connection. An injected TLS
		// dialer owns its handshake and authority checks; retain that path too.
		dialTLS := transport.DialTLSContext
		if dialTLS == nil && transport.DialTLS != nil {
			legacy := transport.DialTLS
			dialTLS = func(_ context.Context, network, addr string) (net.Conn, error) {
				return legacy(network, addr)
			}
		}
		if dialTLS != nil {
			// Let net/http handle custom ALPN using its standard HTTP/2 path.
			transport.ForceAttemptHTTP2 = true
			transport.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := fileTransferDial(ctx, idle, func(ctx context.Context) (net.Conn, error) {
					conn, err := dialTLS(ctx, network, addr)
					if err == nil {
						if tc, ok := conn.(*tls.Conn); ok {
							err = tc.HandshakeContext(ctx)
						}
						if err != nil {
							conn.Close()
						}
					}
					return conn, err
				})
				if err != nil {
					return nil, err
				}
				// An opaque custom TLS hook owns its I/O deadlines. Keep the
				// actual tls.Conn visible to net/http's protocol negotiation.
				return conn, nil
			}
		} else {
			// Ordinary/default and installed pinned TLS run over our raw
			// progress connection; keep their deadlines isolated from HTTP/2.
			transport.ForceAttemptHTTP2 = false
			if transport.TLSClientConfig == nil {
				transport.TLSClientConfig = &tls.Config{}
			} else {
				transport.TLSClientConfig = transport.TLSClientConfig.Clone()
			}
			transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
			transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		}
		transport.TLSHandshakeTimeout = 0
		transport.ResponseHeaderTimeout = 0
		// Each request owns its size budget and connection; no idle pools to retain.
		transport.DisableKeepAlives = true
		local.Transport = transport
	}
	res, err := local.Do(req)
	if err != nil {
		if errors.Is(err, errFileTransferStalled) {
			return nil, fmt.Errorf("%w: no network progress for %s", errFileTransferStalled, c.fileTimeout(size))
		}
		return nil, errors.New("file transfer connection failed")
	}
	return res, nil
}

func fileResponse(res *http.Response, want int) error {
	defer res.Body.Close()
	if res.StatusCode != want {
		return fileTransferError(res, "file transfer failed")
	}
	_, err := io.Copy(io.Discard, res.Body)
	return err
}
func setUploadHeaders(req *http.Request, ref FileRef) {
	req.Header.Set("Upload-Length", strconv.FormatInt(ref.SizeBytes, 10))
	req.Header.Set("Upload-Metadata", "sha256 "+base64.StdEncoding.EncodeToString([]byte(ref.SHA256)))
}
func (c Client) HasFile(ctx context.Context, ref FileRef) error {
	return c.hasFile(ctx, ref, "")
}

func (c Client) hasFile(ctx context.Context, ref FileRef, purpose string) error {
	if err := ref.validate(); err != nil {
		return err
	}
	var headers map[string]string
	if purpose != "" {
		headers = map[string]string{"X-Myce-Upload-Purpose": purpose}
	}
	res, err := c.fileRequest(ctx, "HEAD", "/api/files/blobs/"+ref.SHA256, nil, headers, ref.SizeBytes)
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
	return c.uploadFile(ctx, path, ref, "")
}

// UploadJobInput opts newly created uploads into abandoned-input retention.
// Generic UploadFile imports (including file put and models) remain unmarked.
func (c Client) UploadJobInput(ctx context.Context, path string, ref FileRef) error {
	return c.uploadFile(ctx, path, ref, jobInputUploadPurpose)
}

func (c Client) uploadFile(ctx context.Context, path string, ref FileRef, purpose string) error {
	if err := verifyFile(ctx, path, ref); err != nil {
		return err
	}
	headPurpose := purpose
	if headPurpose == "" {
		headPurpose = fileImportUploadPurpose
	}
	if err := c.hasFile(ctx, ref, headPurpose); err == nil {
		return nil
	} else {
		var he *HTTPError
		if !errors.As(err, &he) || (he.Status != 404 && he.Status != 410) {
			return err
		}
	}
	headers := map[string]string{"Tus-Resumable": "1.0.0", "Upload-Length": strconv.FormatInt(ref.SizeBytes, 10), "Upload-Metadata": "sha256 " + base64.StdEncoding.EncodeToString([]byte(ref.SHA256))}
	if purpose != "" {
		headers["Upload-Metadata"] += ",purpose " + base64.StdEncoding.EncodeToString([]byte(purpose))
	}
	res, err := c.fileRequest(ctx, "POST", "/api/files/uploads/", nil, headers, ref.SizeBytes)
	if err != nil {
		return err
	}
	if err = fileResponse(res, 201); err != nil {
		return err
	}
	if err = c.hasFile(ctx, ref, headPurpose); err == nil {
		return nil
	}
	endpoint := "/api/files/uploads/" + ref.SHA256
	res, err = c.fileRequest(ctx, "HEAD", endpoint, nil, map[string]string{"Tus-Resumable": "1.0.0"}, 0)
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
		res, err = c.fileRequest(ctx, "PATCH", endpoint, io.LimitReader(f, n), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": strconv.FormatInt(offset, 10), "Content-Type": "application/offset+octet-stream"}, ref.SizeBytes)
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
	return c.hasFile(ctx, ref, headPurpose)
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
	res, err := c.fileRequest(ctx, "GET", "/api/files/blobs/"+ref.SHA256, nil, headers, ref.SizeBytes)
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
	n, copyErr := io.Copy(f, io.LimitReader(contextReader{ctx, res.Body}, ref.SizeBytes-offset+1))
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
	var body struct {
		Code  string `json:"code"`
		Error string `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&body) == nil && fileCapacityCode(body.Code) {
		return &HTTPError{Status: res.StatusCode, Code: body.Code, Message: body.Error}
	}

	if res.StatusCode == 410 && res.Header.Get("X-Myce-Error-Code") == "result_expired" {
		return resultExpired()
	}
	return &HTTPError{Status: res.StatusCode, Message: message}
}
