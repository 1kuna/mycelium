package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	return identifyFileContext(context.Background(), path)
}

func identifyFileContext(ctx context.Context, path string) (FileRef, error) {
	if err := ctx.Err(); err != nil {
		return FileRef{}, err
	}
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
	return IdentifyOpenFile(ctx, f)
}

// IdentifyOpenFile hashes a regular file through its already-open handle.
// The caller retains ownership; identification starts at offset zero.
func IdentifyOpenFile(ctx context.Context, f *os.File) (FileRef, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return FileRef{}, err
	}
	st, err := f.Stat()
	if err != nil {
		return FileRef{}, err
	}
	if !st.Mode().IsRegular() {
		return FileRef{}, errors.New("file must be regular")
	}
	h := sha256.New()
	n, err := io.Copy(h, contextReader{ctx, f})
	if err != nil {
		return FileRef{}, err
	}
	if n != st.Size() {
		return FileRef{}, errors.New("file changed during identification")
	}
	return FileRef{hex.EncodeToString(h.Sum(nil)), n}, nil
}
func verifyFile(ctx context.Context, path string, ref FileRef) (verifyErr error) {
	done := stagingMeasure(ctx, stageVerify)
	defer func() { done(verifyErr) }()
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
	inputClaim                  func(context.Context, FileRef, bool) (context.Context, error)
	unidentifiedReserveAdmitted atomic.Uint64
	checkAttempt                func(context.Context) error
	productionReserve           uint64
	admit                       func(context.Context, FileStoreStatus, map[string]storedPayload, FileManifest, uint64, uint64) (uint64, error)
	reservedOutputs             func(map[string]storedPayload, ...context.Context) uint64
	diskProbe                   func(string) (DiskReading, error)
	controlFiles                controlStorageFiles
	root                        string
	limit                       int64
	mu                          sync.Mutex // In-memory creations only.
	collectionMu                sync.Mutex // collection only; never held with q.mu or for I/O.
	collection                  FileCollectionStatus
	uploadFilesMu               sync.RWMutex
	creations                   map[string]*uploadCreation
	allocationGeneration        uint64
	completedAllocations        map[string]completedAllocation
	capacitySnapshots           map[*fileCapacitySnapshot]bool
	admitCreation               func(context.Context, *fileCapacitySnapshot, FileRef) (uint64, error)
	explainRefusal              func(context.Context, error) error
	handler                     http.Handler
	uploadLocker                tus.Locker
	syncDir                     func(string) error // Durability failure injection.
	expired                     func(string, ...context.Context) bool
	lifecycle                   *sync.RWMutex
	clock                       Clock
	recordUpload                func(hash, purpose string, created bool, contexts ...context.Context) error
	publishUpload               func(hash string, contexts ...context.Context) error
	recordBundle                func(hash string, created bool, contexts ...context.Context) error
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
	if j.Spec.OutputDelivery == asyncNAS || j.FileTransport || len(jobInputFiles(&j))+len(j.Spec.ModelSnapshots) > 0 {
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
	if q.fileStoreProductionReserve < 0 || q.fileStoreProductionReserve > limit {
		return errors.New("file store production reserve must be between zero and the store limit")
	}
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
	s := &FileStore{inputClaim: q.inputClaimContext, checkAttempt: q.checkFileStoreAttempt, productionReserve: uint64(q.fileStoreProductionReserve), admit: q.admitFileUpload, admitCreation: q.admitFileCreation, explainRefusal: q.explainFileStoreRefusal, reservedOutputs: q.reservedOutputBytes, root: root, limit: limit, syncDir: syncWorkerDir, expired: q.expiredBlob, lifecycle: &q.fileLifecycle, clock: q.clock, recordUpload: q.recordFileUpload, publishUpload: q.publishFileUpload, recordBundle: q.recordBundleUpload}
	if q.lock != nil {
		database := strings.TrimSuffix(q.lock.Name(), ".lock")
		s.controlFiles = controlStorageFiles{database: database, wal: database + "-wal", shared: database + "-shm"}
	}
	fs := filestore.New(filepath.Join(root, "uploads"))
	fs.DirModePerm = 0700
	fs.FileModePerm = 0600
	composer := tus.NewStoreComposer()
	fs.UseIn(composer)
	composer.UseCore(creationDataStore{DataStore: fs, mu: &s.uploadFilesMu, store: s})
	locker := filelocker.New(fs.Path)
	s.uploadLocker = locker
	locker.UseIn(composer)
	h, err := tus.NewHandler(tus.Config{
		BasePath: "/api/files/uploads/", StoreComposer: composer, DisableDownload: true, DisableTermination: true, DisableConcatenation: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		PreUploadCreateCallback: func(e tus.HookEvent) (tus.HTTPResponse, tus.FileInfoChanges, error) {
			ctx := creationContext(e.Context)
			ref := FileRef{e.Upload.MetaData["sha256"], e.Upload.Size}
			if err := ref.validate(); err != nil || e.Upload.SizeIsDeferred {
				return tus.HTTPResponse{}, tus.FileInfoChanges{}, errors.New("invalid file upload identity")
			}
			if err := s.recordUpload(ref.SHA256, e.Upload.MetaData["purpose"], true, ctx); err != nil {
				return tus.HTTPResponse{}, tus.FileInfoChanges{}, err
			}
			if err := ctx.Err(); err != nil {
				return tus.HTTPResponse{}, tus.FileInfoChanges{}, err
			}
			return tus.HTTPResponse{}, tus.FileInfoChanges{ID: ref.SHA256}, nil
		},
		PreFinishResponseCallback: func(e tus.HookEvent) (tus.HTTPResponse, error) {
			ctx := creationContext(e.Context)
			ref := FileRef{e.Upload.MetaData["sha256"], e.Upload.Size}
			if err := s.publish(ctx, ref); err != nil {
				return tus.HTTPResponse{}, err
			}
			// An internal writer may have created the same blob after this
			// upload's POST. Publication must still honor the import's custody.
			if err := s.recordUpload(ref.SHA256, e.Upload.MetaData["purpose"], false, ctx); err != nil {
				return tus.HTTPResponse{}, err
			}
			return tus.HTTPResponse{}, s.publishUpload(ref.SHA256, ctx)
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
	q.republishReads()
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
	response := &uploadResponse{ResponseWriter: w}
	w = response
	defer response.finish()
	var err error
	r, err = fileStoreSubmissionRequest(r)
	if err != nil {
		respond(w, nil, reject("job_spec_invalid", err))
		return
	}
	if err := s.checkAttempt(r.Context()); err != nil {
		respond(w, nil, err)
		return
	}
	// Fence upload publication against unlink without serializing other uploads.
	if err := lockUploadContext(r.Context(), s.lifecycle.RLock, s.lifecycle.RUnlock); err != nil {
		uploadCancelled(w, r.Context())
		return
	}
	defer s.lifecycle.RUnlock()
	id := strings.TrimPrefix(r.URL.Path, "/api/files/uploads/")
	if id != "" {
		if err := (FileRef{SHA256: id}).validate(); err != nil {
			http.Error(w, "invalid upload", 400)
			return
		}
		upload, err := filestore.New(filepath.Join(s.root, "uploads")).GetUpload(r.Context(), id)
		if err != nil {
			s.handler.ServeHTTP(w, r)
			return
		}
		info, err := upload.GetInfo(r.Context())
		if err != nil {
			http.Error(w, "upload metadata unavailable", 503)
			return
		}
		claimCtx, err := s.inputClaim(r.Context(), FileRef{SHA256: id, SizeBytes: info.Size}, true)
		if err != nil {
			respond(w, nil, err)
			return
		}
		r = r.WithContext(claimCtx)
		s.handler.ServeHTTP(w, r)
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
	// Parse before claiming an identity or reserving capacity.
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
	finish, err := s.beginCreation(r.Context(), hash)
	if err != nil {
		uploadCancelled(w, r.Context())
		return
	}
	defer finish()
	// Recheck after waiting for a previous creation of this identity.
	if uploadCancelled(w, r.Context()) {
		return
	}
	claimCtx, claimErr := s.inputClaim(r.Context(), ref, false)
	if claimErr != nil {
		respond(w, nil, claimErr)
		return
	}
	r = r.WithContext(claimCtx)
	if s.verify(r.Context(), ref) == nil {
		if _, err := s.inputClaim(r.Context(), ref, true); err != nil {
			respond(w, nil, err)
			return
		}
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
	if uploadCancelled(w, r.Context()) {
		return
	}
	if st, e := os.Stat(filepath.Join(s.root, "uploads", hash+".info")); e == nil && st.Mode().IsRegular() {
		// A repeated create must match the immutable stored upload length.
		upload, e := filestore.New(filepath.Join(s.root, "uploads")).GetUpload(r.Context(), hash)
		if e != nil {
			http.Error(w, "upload metadata unavailable", 503)
			return
		}
		info, e := upload.GetInfo(r.Context())
		if e != nil {
			http.Error(w, "upload metadata unavailable", 503)
			return
		}
		if info.Size != length {
			respond(w, nil, inputClaimError())
			return
		}
		if _, err := s.inputClaim(r.Context(), ref, true); err != nil {
			respond(w, nil, err)
			return
		}
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
	// Disk work happens before queue acquisition. The queue admission checks
	// current in-memory allocations and commits this reservation atomically.
	view := s.snapshotCapacity(r.Context())
	defer view.release()
	unidentified, err := s.admitCreation(r.Context(), view, ref)
	if err != nil {
		if s.explainRefusal != nil {
			err = s.explainRefusal(r.Context(), err)
		}
		respond(w, nil, err)
		return
	}
	if _, e := os.Lstat(filepath.Join(s.root, "uploads", hash)); e == nil {
		// No .info means no resumable upload authority; do not silently overwrite.
		http.Error(w, "unindexed upload requires operator recovery", 409)
		return
	}
	if uploadCancelled(w, r.Context()) {
		return
	}
	if _, err := s.inputClaim(r.Context(), ref, true); err != nil {
		respond(w, nil, err)
		return
	}
	// Keep immediate cancellation and the final queue-issued claim context.
	r = r.WithContext(context.WithValue(r.Context(), uploadCreationContextKey{}, r.Context()))
	s.handler.ServeHTTP(w, r)
	if response.status == http.StatusCreated {
		s.unidentifiedReserveAdmitted.Add(unidentified)
	}
	if err := s.refreshUpload(hash); err != nil {
		log.Printf("upload freshness unavailable: %v", err)
	}
}

// Successes stream unchanged, including ResponseController capabilities. Only
// error bodies are held so both our refusals and tus's plain-text errors leave
// through the same public JSON envelope. HEAD carries its code in a header.
type uploadResponse struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *uploadResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *uploadResponse) WriteHeader(status int) {
	if status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if w.status != 0 {
		return
	}
	w.status = status
	if status < 400 {
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *uploadResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= 400 {
		return w.body.Write(p)
	}
	return w.ResponseWriter.Write(p)
}
func (w *uploadResponse) finish() {
	if w.status < 400 {
		return
	}
	var envelope struct {
		Code  string `json:"code"`
		Error string `json:"error"`
	}
	body := w.body.Bytes()
	if json.Unmarshal(body, &envelope) != nil || envelope.Code == "" || envelope.Error == "" {
		// Never expose tus's internal errors (which may contain storage paths).
		envelope.Code = "file_upload_failed"
		envelope.Error = "file upload failed"
		switch w.status {
		case 400:
			envelope.Code, envelope.Error = "file_upload_invalid", "invalid upload request"
		case 404, 410:
			envelope.Code, envelope.Error = "file_upload_not_found", "upload session not found; check the verified blob before resuming"
		case 405, 501:
			envelope.Code, envelope.Error = "file_upload_method_unsupported", "upload method is not supported"
		case 408:
			envelope.Code, envelope.Error = "file_upload_interrupted", "upload request interrupted"
		case 409:
			envelope.Code, envelope.Error = "file_upload_conflict", "upload conflicts with stored state"
			if bytes.HasPrefix(body, []byte("ERR_MISMATCHED_OFFSET:")) {
				envelope.Code, envelope.Error = "file_upload_offset_mismatch", "upload offset does not match accepted progress"
			}
		case 412:
			envelope.Code, envelope.Error = "file_upload_version_unsupported", "missing or unsupported Tus-Resumable version"
		case 413:
			envelope.Code, envelope.Error = "file_upload_size_exceeded", "upload exceeds its declared size"
		case 423:
			envelope.Code, envelope.Error = "file_upload_locked", "upload session is locked"
		case 500, 502, 503, 504:
			envelope.Code, envelope.Error = "file_upload_unavailable", "upload storage unavailable"
		}
		body, _ = json.Marshal(envelope)
	}
	w.Header().Del("Content-Length")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Myce-Error-Code", envelope.Code)
	w.ResponseWriter.WriteHeader(w.status)
	_, _ = w.ResponseWriter.Write(body)
}
func (s *FileStore) serveBlob(w http.ResponseWriter, r *http.Request, metadata bool) {
	if r.Method == http.MethodHead && (r.Header.Get("X-Myce-Input-Claim") != "" || r.Header.Get("X-Myce-File-Attempt") != "") {
		parsed, err := fileStoreSubmissionRequest(r)
		if err != nil {
			respond(w, nil, err)
			return
		}
		r = parsed
		if err := s.checkAttempt(r.Context()); err != nil {
			respond(w, nil, err)
			return
		}
		claimCtx, err := s.inputClaim(r.Context(), FileRef{SHA256: r.PathValue("hash"), SizeBytes: -1}, false)
		if err != nil {
			respond(w, nil, err)
			return
		}
		r = r.WithContext(claimCtx)
	}
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
	failure := func(operation string, err error) {
		log.Printf("file read failed blob=%s operation=%s cause=%v", hash, operation, err)
		w.Header().Set("X-Myce-Error-Code", "file_blob_unavailable")
		w.Header().Set("X-Myce-Storage-Operation", operation)
		w.Header().Set("X-Myce-Storage-Error", stagingErrorName(err))
		respond(w, nil, &HTTPError{Status: 503, Code: "file_blob_unavailable", Message: "queue blob read failed: " + stagingErrorName(err)})
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
		if !errors.Is(err, os.ErrNotExist) {
			failure("store_resolve", err)
			return
		}
		missing()
		return
	}
	f, err := os.Open(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			failure("store_open", err)
			return
		}
		missing()
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		failure("store_stat", err)
		return
	}
	if !st.Mode().IsRegular() {
		failure("store_stat", errors.New("invalid cache entry"))
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
		if verifyErr == nil {
			verifyErr = errors.New("stored file identity mismatch")
		}
		failure("store_verify", verifyErr)
		return
	}
	if r.Method == http.MethodHead {
		if err := s.checkAttempt(r.Context()); err != nil {
			respond(w, nil, err)
			return
		}
		if r.Header.Get("X-Myce-Input-Claim") != "" {
			if _, err := s.inputClaim(r.Context(), FileRef{SHA256: hash, SizeBytes: st.Size()}, true); err != nil {
				respond(w, nil, err)
				return
			}
		}
		if err = s.refreshBlob(path); err != nil {
			failure("store_refresh", err)
			return
		}
	}
	if err = s.syncDir(filepath.Join(s.root, "blobs")); err != nil {
		failure("store_sync", err)
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
		failure("store_seek", err)
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
	mux.HandleFunc("POST /api/files/input-claims", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Project string  `json:"project"`
			Class   string  `json:"class,omitempty"`
			File    FileRef `json:"file"`
		}
		if !decode(w, r, &request) {
			return
		}
		claim, err := a.Queue.claimFileInput(r.Context(), Spec{Project: request.Project, Class: request.Class}, request.File)
		respondOperation(w, claim, err, "job_spec_invalid")
	})
	mux.HandleFunc("POST /api/files/preflight", func(w http.ResponseWriter, r *http.Request) {
		var err error
		r, err = fileStoreSubmissionRequest(r)
		if err != nil {
			respond(w, nil, reject("job_spec_invalid", err))
			return
		}
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

// File transfers have no whole-request deadline. Each direction learns its
// own byte service time. Upload PATCH also uses measured request service and
// pressure; other pre-header verification remains bounded by the owner.
var errFileTransferStalled = errors.New("file transfer stalled")

// Record the failed phase before HTTP proxy wrapping or timeout normalization.
// Only a completed dial reports this; a live transfer's idle expiry cannot.
var errFileTransferDial = errors.New("file transfer dial failed")
var errFileTransferLocal = errors.New("local download storage failed")

func (c Client) fileTimeout(_ int64) time.Duration { return c.transferIdle }

// Per-operation evidence spans the tus HEAD/POST/PATCH exchange, not unrelated
// uploads. RFC 6298 sections 2.1/2.4 specify a one-second initial/minimum TCP
// retransmission timer: a faster socket copy cannot justify declaring a dead
// peer before it has an opportunity to retransmit. Four measured service periods
// retain the existing progress jitter allowance. Retry-After is a server signal,
// not a throughput estimate. The owner's context can always end the operation.
type uploadTiming struct {
	mu                sync.Mutex
	service, pressure time.Duration
}

func (t *uploadTiming) observe(service, pressure time.Duration) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.service = max(t.service, service)
	t.pressure = max(t.pressure, pressure)
}
func (t *uploadTiming) window() time.Duration {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return max(clientRecoveryPeriod, growDuration(growDuration(t.service)), t.pressure)
}

type fileTransferConn struct {
	net.Conn
	timing                      *uploadTiming
	idle                        time.Duration // Explicit test/owner override; zero learns from progress.
	clock                       Clock
	readProgress, writeProgress transferProgress
	reading, writing            *atomic.Bool
	awaiting                    *atomic.Bool
	deadlineMu                  sync.Mutex
}

func (c *fileTransferConn) deadline(read bool, bytes int) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	idle := c.idle
	if idle == 0 {
		if read {
			if c.reading == nil || c.reading.Load() {
				idle = max(c.readProgress.window(bytes), c.timing.window())
			} else if c.awaiting != nil && c.awaiting.Load() {
				idle = c.timing.window()
			}
		} else if c.writing == nil || c.writing.Load() {
			idle = max(c.writeProgress.window(bytes), c.timing.window())
		}
	}
	end := time.Time{}
	if idle > 0 {
		clock := c.clock
		if clock == nil {
			clock = RealClock{}
		}
		end = clock.Now().Add(idle)
	}
	if c.idle > 0 {
		return c.SetDeadline(end)
	}
	if read {
		return c.SetReadDeadline(end)
	}
	return c.SetWriteDeadline(end)
}

// Serialize with deadline's phase check AND socket update; otherwise a reader
// that saw the old phase can clear the newly armed response deadline.
func (c *fileTransferConn) awaitResponse() {
	c.awaiting.Store(true)
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	clock := c.clock
	if clock == nil {
		clock = RealClock{}
	}
	c.SetReadDeadline(clock.Now().Add(max(c.idle, c.timing.window())))
}

func (c *fileTransferConn) Read(p []byte) (int, error) {
	if err := c.deadline(true, len(p)); err != nil {
		return 0, err
	}
	clock := c.clock
	if clock == nil {
		clock = RealClock{}
	}
	started := clock.Now()
	n, err, waited := fileSocketSome(c.Conn, p, false)
	if c.reading == nil || c.reading.Load() {
		now := clock.Now()
		c.readProgress.record(now, now.Sub(started), n, waited)
	}
	return n, fileTransferTimeout(err, max(c.idle, c.readProgress.window(len(p)), c.timing.window()))
}
func (c *fileTransferConn) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		if err := c.deadline(false, len(p)); err != nil {
			return total, err
		}
		clock := c.clock
		if clock == nil {
			clock = RealClock{}
		}
		started := clock.Now()
		n, err, waited := fileSocketSome(c.Conn, p, true)
		if c.writing == nil || c.writing.Load() {
			now := clock.Now()
			c.writeProgress.record(now, now.Sub(started), n, waited)
		}
		total += n
		p = p[n:]
		if err != nil {
			var timeout net.Error
			if n > 0 && errors.As(err, &timeout) && timeout.Timeout() {
				continue
			}
			return total, fileTransferTimeout(err, max(c.idle, c.writeProgress.window(len(p)), c.timing.window()))
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}

func fileTransferTimeout(err error, idle time.Duration) error {
	// Dial completion is definitive reachability evidence, not an idle body
	// expiry. Preserve its identity through every transport wrapper so the
	// preparation owner can end a real outage rather than resume forever.
	var dial *net.OpError
	if errors.Is(err, errFileTransferDial) || errors.As(err, &dial) && dial.Op == "dial" {
		return err
	}
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
	if idle > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, idle)
		defer cancel()
	}
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
// Retry reads only: PATCH offsets and arbitrary mutations require their
// caller's reconciliation protocol, never an implicit replay.
func (c Client) fileRequest(ctx context.Context, method, path string, body io.Reader, headers map[string]string, sizes ...int64) (*http.Response, error) {
	pace := ClientPace{}
	for {
		started := c.clock().Now()
		response, err := c.fileRequestOnce(ctx, method, path, body, headers, sizes...)
		if c.singleAttempt || err != nil || method != "GET" && method != "HEAD" || response.StatusCode != 429 && response.StatusCode != 503 || response.Header.Get("X-Myce-Error-Code") != "" {
			return response, err
		}
		failure := &HTTPError{Status: response.StatusCode, RetryAfter: retryAfter(response.Header.Get("Retry-After"), c.clock().Now())}
		response.Body.Close()
		pace.Observe(c.clock().Now().Sub(started), false)
		if err := c.pause(ctx, pace.Delay(c.clock().Now(), time.Time{}, failure, c.Jitter)); err != nil {
			return nil, err
		}
	}
}

func (c Client) fileRequestOnce(ctx context.Context, method, path string, body io.Reader, headers map[string]string, sizes ...int64) (*http.Response, error) {
	var size int64
	if len(sizes) != 0 {
		size = sizes[0]
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	var reading, writing, awaiting atomic.Bool
	var progressConn atomic.Pointer[fileTransferConn]
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		WroteHeaders: func() { writing.Store(true) },
		WroteRequest: func(httptrace.WroteRequestInfo) {
			writing.Store(false)
			awaiting.Store(true)
			// net/http reads the response concurrently with uploading the body.
			// Arm the header wait only after the last body byte, so a long but
			// progressing upload never spends its response-service window.
			if conn := progressConn.Load(); conn != nil && conn.timing != nil {
				conn.awaitResponse()
			}
		},
	}))
	fileStoreSubmissionHeaders(ctx, req)
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
				conn, err := dial(ctx, network, addr)
				if err != nil {
					err = fmt.Errorf("%w: %w", errFileTransferDial, err)
				}
				return conn, err
			})
			if err != nil {
				return nil, fileTransferTimeout(err, idle)
			}
			var timing *uploadTiming
			if method == "PATCH" {
				timing = c.uploadTiming
			}
			// Socket deadlines use the real socket's clock, independently of an
			// injected observation/pacing clock (unit tests may freeze that).
			wrapped := &fileTransferConn{Conn: conn, idle: idle, clock: RealClock{}, reading: &reading, writing: &writing, awaiting: &awaiting, timing: timing}
			progressConn.Store(wrapped)
			return wrapped, nil
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
	started := c.clock().Now()
	res, err := local.Do(req)
	pressure := time.Duration(0)
	if res != nil {
		pressure = retryAfter(res.Header.Get("Retry-After"), c.clock().Now())
	}
	c.uploadTiming.observe(c.clock().Now().Sub(started), pressure)
	if err != nil {
		return nil, fmt.Errorf("file transfer connection failed: %w", err)
	}
	reading.Store(true)
	return res, nil
}

func fileResponse(res *http.Response, want int, now ...time.Time) error {
	defer res.Body.Close()
	if res.StatusCode != want {
		return fileTransferError(res, "file transfer failed", now...)
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

func (c Client) hasFile(ctx context.Context, ref FileRef, purpose string) (headErr error) {
	done := stagingMeasure(ctx, stageBlobHead)
	defer func() { done(headErr) }()
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
		return fileTransferError(res, "file unavailable", c.clock().Now())
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

// UploadJobInputFile uploads from an open handle without reopening a pathname.
// The caller owns the handle. Its bytes must still match the identified ref.
func (c Client) UploadJobInputFile(ctx context.Context, f *os.File, ref FileRef) error {
	actual, err := IdentifyOpenFile(ctx, f)
	if err != nil {
		return err
	}
	if actual != ref {
		return errors.New("file changed before upload")
	}
	return c.uploadFileReader(ctx, f, ref, jobInputUploadPurpose)
}

func (c Client) uploadFile(ctx context.Context, path string, ref FileRef, purpose string) error {
	if err := verifyFile(ctx, path, ref); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return c.uploadFileReader(ctx, f, ref, purpose)
}

func (c Client) uploadFileReader(ctx context.Context, f io.ReadSeeker, ref FileRef, purpose string) error {
	c.uploadTiming = &uploadTiming{}
	pace := ClientPace{}
	for {
		if err := c.ownerError(ctx); err != nil {
			return err
		}
		started := c.clock().Now()
		err := c.uploadFileAttempt(ctx, f, ref, purpose)
		if err == nil || c.singleAttempt || !retryTransient(err) {
			return err
		}
		pace.Observe(c.clock().Now().Sub(started), false)
		if stopped := c.pause(ctx, pace.Delay(c.clock().Now(), time.Time{}, err, c.Jitter)); stopped != nil {
			return &clientRetryStopped{err, stopped}
		}
	}
}

// Every retry rechecks publication and the server's accepted tus offset. Never
// replay a PATCH whose acknowledgement was lost, even if socket writes succeeded.
func (c Client) uploadFileAttempt(ctx context.Context, f io.ReadSeeker, ref FileRef, purpose string) (result error) {
	source := &uploadSource{ctx: ctx, reader: f, digest: sha256.New(), expected: ref.SizeBytes}
	if s, ok := ctx.Value(fileStoreSubmissionKey{}).(Spec); ok && ctx.Value(fileStoreAttemptKey{}) == nil {
		var claim FileInputClaim
		err := c.Call(ctx, "POST", "/api/files/input-claims", map[string]any{"project": s.Project, "class": s.Class, "file": ref}, &claim)
		if err != nil {
			return err
		}
		ctx = context.WithValue(ctx, fileInputClaimKey{}, claim.ID)
	}
	headPurpose := purpose
	if headPurpose == "" {
		headPurpose = fileImportUploadPurpose
	}
	defer func() {
		if result == nil || missingUpload(result) || retryTransient(result) {
			// Verify the bytes this attempt actually read, not a fresh read that
			// could hide a changed PATCH after the source was restored in place.
			if err := source.verify(ref); err != nil {
				result = err
				return
			}
		}
		if !missingUpload(result) {
			return
		}
		// A duplicate final PATCH or late HEAD may find that publication removed
		// the session. Only the hash-verified blob proves completion.
		if err := c.hasFile(ctx, ref, headPurpose); missingUpload(err) {
			result = &uploadPendingError{result}
		} else {
			result = err
		}
	}()
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
	if err = fileResponse(res, 201, c.clock().Now()); err != nil {
		return err
	}
	if err = c.hasFile(ctx, ref, headPurpose); err == nil {
		return nil
	} else {
		var he *HTTPError
		if !errors.As(err, &he) || (he.Status != 404 && he.Status != 410) {
			return err
		}
	}
	endpoint := "/api/files/uploads/" + ref.SHA256
	offset, err := c.uploadOffset(ctx, endpoint, ref.SizeBytes)
	if err != nil {
		return err
	}
	if err = source.start(offset); err != nil {
		return err
	}
	for {
		if err := c.ownerError(ctx); err != nil {
			return err
		}
		n := min(int64(4<<20), ref.SizeBytes-offset)
		// Close synchronizes with any late net/http body reader before a retry
		// seeks the caller's handle. A failed response may race the writer goroutine.
		body := &uploadBody{reader: io.LimitReader(source, n)}
		res, err = c.fileRequest(ctx, "PATCH", endpoint, body, map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": strconv.FormatInt(offset, 10), "Content-Type": "application/offset+octet-stream"}, ref.SizeBytes)
		body.Close()
		if err != nil {
			return err
		}
		next, e := strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64)
		if err = fileResponse(res, 204, c.clock().Now()); err != nil {
			return err
		}
		if e != nil || next != offset+n {
			return errors.New("invalid upload acknowledgement")
		}
		offset = next
		if offset == ref.SizeBytes {
			break
		}
	}

	return c.hasFile(ctx, ref, headPurpose)
}

// One attempt hashes its resumed prefix and the exact bytes read by HTTP. After
// uploadBody.Close fences late transport reads, verify consumes any unread tail
// (including unexpected growth). This is client I/O, never queue-mutex work.
type uploadSource struct {
	ctx      context.Context
	reader   io.ReadSeeker
	digest   hash.Hash
	size     int64
	expected int64
	short    bool
	started  bool
}

func (s *uploadSource) Read(p []byte) (int, error) {
	n, err := io.ReadFull(contextReader{s.ctx, s.reader}, p)
	_, _ = s.digest.Write(p[:n])
	s.size += int64(n)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		s.short = s.short || s.size < s.expected
		err = io.EOF
	}
	return n, err
}

func (s *uploadSource) start(offset int64) error {
	if _, err := s.reader.Seek(0, io.SeekStart); err != nil {
		return err
	}
	s.started = true
	if _, err := io.CopyN(io.Discard, s, offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errors.New("upload source size or SHA-256 mismatch")
		}
		return err
	}
	return nil
}

func (s *uploadSource) verify(ref FileRef) error {
	if !s.started {
		if err := s.start(0); err != nil {
			return err
		}
	}
	if _, err := io.Copy(io.Discard, s); err != nil {
		return err
	}
	if s.short || s.size != ref.SizeBytes || hex.EncodeToString(s.digest.Sum(nil)) != ref.SHA256 {
		return errors.New("upload source size or SHA-256 mismatch")
	}
	return nil
}

func missingUpload(err error) bool {
	var failure *HTTPError
	return errors.As(err, &failure) && (failure.Status == 404 || failure.Status == 410)
}

// Scoped to upload reconciliation: missing sessions elsewhere remain permanent.
type uploadPendingError struct{ error }

func (e *uploadPendingError) Unwrap() error { return e.error }

// net/http may close a request body asynchronously after RoundTrip returns.
// Fence reads so the next attempt can safely seek the same open source handle.
type uploadBody struct {
	mu     sync.Mutex
	reader io.Reader
}

func (b *uploadBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reader == nil {
		return 0, io.EOF
	}
	return b.reader.Read(p)
}
func (b *uploadBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reader = nil
	return nil
}

// tus HEAD is authoritative; local socket writes are not accepted progress.
func (c Client) uploadOffset(ctx context.Context, endpoint string, size int64) (int64, error) {
	res, err := c.fileRequest(ctx, "HEAD", endpoint, nil, map[string]string{"Tus-Resumable": "1.0.0"})
	if err != nil {
		return 0, err
	}
	offset, oe := strconv.ParseInt(res.Header.Get("Upload-Offset"), 10, 64)
	length, le := strconv.ParseInt(res.Header.Get("Upload-Length"), 10, 64)
	if err = fileResponse(res, 200, c.clock().Now()); err != nil {
		return 0, err
	}
	if oe != nil || le != nil || length != size || offset < 0 || offset > size {
		return 0, errors.New("invalid resumable upload offset/size")
	}
	return offset, nil
}

func (c Client) DownloadFile(ctx context.Context, ref FileRef, dest string) (stageErr error) {
	traceSharedFile(ctx, ref)
	defer func() { stageErr = tracedSharedError(ctx, stageErr) }()
	traceSharedInput(ctx, "prepare", "cache")
	if err := ref.validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		return err
	}
	// EvalSymlinks preserves relative paths. Make the user-selected parent
	// absolute first so logical cwd ancestors (e.g. macOS /tmp) resolve too.
	// Keep the leaf unresolved: noModelSymlinks must still reject a link there.
	dest, err := filepath.Abs(dest)
	if err != nil {
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
	traceSharedInput(ctx, "open", "cache")
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	traceSharedInput(ctx, "stat", "cache")
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
		traceSharedInput(ctx, "verify", "cache")
		return verifyFile(ctx, dest, ref)
	}
	headers := map[string]string{"If-Range": `"` + ref.SHA256 + `"`}
	if offset > 0 {
		headers["Range"] = fmt.Sprintf("bytes=%d-", offset)
	}
	traceSharedInput(ctx, "headers", "store")
	headersDone := stagingMeasure(ctx, stageBlobHeaders)
	res, err := c.fileRequest(ctx, "GET", "/api/files/blobs/"+ref.SHA256, nil, headers, ref.SizeBytes)
	headerErr := err
	if headerErr == nil && res.StatusCode != 200 && res.StatusCode != 206 {
		headerErr = &HTTPError{Status: res.StatusCode}
	}
	headersDone(headerErr)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 && res.StatusCode != 206 {
		return fileTransferError(res, "download failed", c.clock().Now())
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
	var output io.Writer = f
	var input io.Reader = res.Body
	if t, ok := ctx.Value(sharedInputTraceKey{}).(*sharedInputTrace); ok {
		t.mu.Lock()
		t.detail.BytesCopied = offset
		t.mu.Unlock()
		output, input = sharedInputWriter{ctx, f}, sharedInputReader{ctx, res.Body, "store"}
	}
	downloadDone := stagingMeasure(ctx, stageDownload)
	n, copyErr := io.Copy(fileDownloadWriter{output}, io.LimitReader(contextReader{ctx, input}, ref.SizeBytes-offset+1))
	downloadDone(copyErr)
	copyErr = tracedSharedCopyError(ctx, copyErr)
	traceSharedInput(ctx, "sync", "cache")
	err = copyErr
	syncDone := stagingMeasure(ctx, stageCachePublish)
	syncErr := f.Sync()
	syncDone(syncErr)
	if syncErr != nil {
		err = fmt.Errorf("%w: %w", errFileTransferLocal, tracedSharedError(ctx, syncErr))
	}
	if err != nil {
		return err
	}
	if n != ref.SizeBytes-offset {
		return errors.New("download interrupted or oversized")
	}
	traceSharedInput(ctx, "verify", "cache")
	return verifyFile(ctx, dest, ref)
}

type fileDownloadWriter struct{ io.Writer }

func (w fileDownloadWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		err = fmt.Errorf("%w: %w", errFileTransferLocal, err)
	}
	return n, err
}

func fileTransferError(res *http.Response, message string, observed ...time.Time) error {
	now := (RealClock{}).Now()
	if len(observed) > 0 {
		now = observed[0]
	}
	delay := retryAfter(res.Header.Get("Retry-After"), now)
	if res.Header.Get("X-Myce-Error-Code") == "file_blob_unavailable" {
		d := safeStagingCause(&StagingCause{Operation: res.Header.Get("X-Myce-Storage-Operation"), Error: res.Header.Get("X-Myce-Storage-Error"), Status: res.StatusCode})
		if d != nil {
			return &stagingError{*d, &HTTPError{Status: res.StatusCode, Code: "file_blob_unavailable", Message: "queue blob read failed", RetryAfter: delay}}
		}
	}
	var body struct {
		Code  string `json:"code"`
		Error string `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&body) == nil {
		if fileStoreRefusalCode(body.Code) {
			return &HTTPError{Status: res.StatusCode, Code: body.Code, Message: body.Error, RetryAfter: delay}
		}
		if publicErrorCode(body.Code) || body.Code == "request_failed" {
			return &HTTPError{Status: res.StatusCode, Code: body.Code, Message: message, RetryAfter: delay}
		}
	}

	if res.StatusCode == 410 && res.Header.Get("X-Myce-Error-Code") == "result_expired" {
		return resultExpired()
	}
	// HEAD has no response body; upload errors retain their typed code here.
	if code := res.Header.Get("X-Myce-Error-Code"); publicErrorCode(code) || code == "request_failed" {
		return &HTTPError{Status: res.StatusCode, Code: code, Message: message, RetryAfter: delay}
	}
	return &HTTPError{Status: res.StatusCode, Message: message, RetryAfter: delay}
}
