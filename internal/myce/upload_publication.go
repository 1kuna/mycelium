package myce

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	tus "github.com/tus/tusd/v2/pkg/handler"
)

type uploadDiagnosticKey struct{}

const uploadQueueServiceHeader = "X-Myce-Upload-Queue-Service"

// Keep the peak across two short buckets so a burst of fast check-ins cannot
// immediately erase contention. Evidence ages out after 15–30 seconds, roughly
// one to three turns at the measured 11-second p95 wait under production load.
const checkinServiceBucket = 15 * time.Second

type recentCheckinService struct {
	mu                sync.Mutex
	start             time.Time
	current, previous time.Duration
}

func (s *recentCheckinService) advance(now time.Time) {
	elapsed := now.Sub(s.start)
	switch {
	case elapsed >= 2*checkinServiceBucket:
		s.start, s.current, s.previous = now, 0, 0
	case elapsed >= checkinServiceBucket:
		s.start, s.current, s.previous = s.start.Add(checkinServiceBucket), 0, s.current
	}
}

func (s *recentCheckinService) record(clock Clock, service time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(clock.Now())
	s.current = max(s.current, service)
}

func (s *recentCheckinService) peak(clock Clock) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(clock.Now())
	return max(s.current, s.previous)
}

func (q *Queue) recordCheckinService(ctx context.Context) {
	span := spanFrom(ctx)
	if span == nil {
		return
	}
	span.mu.Lock()
	service := span.wait + max(0, span.hold-span.writerWait)
	span.mu.Unlock()
	// This runs before NAS checks and the HTTP reply. Body reads and reply
	// writes never contribute to the mutex intervals or refresh their age.
	q.checkinService.record(q.clock, service)
}

// The store's service time for each upload route, measured here over whole
// requests (body receipt, write, sync, publication, response), whatever their
// outcome: an interrupted request still took at least that long. A new client
// has no samples of its own, so the hint lets it wait for this server's measured
// service instead of abandoning a slow store's normal create or chunk (#2165).
// The peak ages out like the check-in peak, so one outlier does not stick.
func (q *Queue) uploadServiceFor(method string) *recentCheckinService {
	switch method {
	case http.MethodPost:
		return &q.uploadService[0]
	case http.MethodPatch:
		return &q.uploadService[1]
	}
	return nil
}

func (q *Queue) recordUploadService(method string, service time.Duration) {
	if s := q.uploadServiceFor(method); s != nil {
		s.record(q.clock, service)
	}
}

// Before the upload can block on custody or accounting, give this connection
// the queue's own service measurement. Verification 102s still represent only
// actual reads. The hint is scoped to this response and the owner can cancel.
func (q *Queue) reportUploadQueueService(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(uploadQueueServiceHeader) != "1" || r.Method != http.MethodPost && r.Method != http.MethodPatch {
		return
	}
	period, err := time.ParseDuration(r.Header.Get("X-Myce-Upload-Progress"))
	if err != nil || period <= 0 {
		return
	}
	// Each check-in takes a turn through the same mutex. Long polls and
	// unrelated transfers can wait by design, and do not estimate that turn.
	service := max(q.checkinService.peak(q.clock), q.uploadServiceFor(r.Method).peak(q.clock))
	if p := q.mu.pressure.Load(); p != nil && q.mu.waiting.Load() > 0 {
		_, wait, _ := p.measurement()
		service = max(service, wait)
	}
	if service <= period {
		return // Already covered by the client's current response window.
	}
	w.Header().Set(uploadQueueServiceHeader, service.String())
	w.WriteHeader(http.StatusProcessing)
	w.Header().Del(uploadQueueServiceHeader)
}

type uploadDiagnostic struct {
	started                                              time.Time
	hash, operation, failure                             string
	length, accepted                                     int64
	bodyRead, fileWrite, hashBytes, fileSync, inputClaim time.Duration
}

type uploadMeasuredReader struct {
	source  io.Reader
	elapsed *time.Duration
}

func (r uploadMeasuredReader) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := r.source.Read(p)
	*r.elapsed += time.Since(start)
	return n, err
}

type uploadMeasuredWriter struct {
	destination io.Writer
	elapsed     *time.Duration
}

func (w uploadMeasuredWriter) Write(p []byte) (int, error) {
	start := time.Now()
	n, err := w.destination.Write(p)
	*w.elapsed += time.Since(start)
	return n, err
}

func uploadSync(ctx context.Context, sync func() error) error {
	start := time.Now()
	err := sync()
	if d, ok := ctx.Value(uploadDiagnosticKey{}).(*uploadDiagnostic); ok {
		d.fileSync += time.Since(start)
	}
	return err
}

func uploadRequestDiagnostic(r *http.Request) (*http.Request, *uploadDiagnostic) {
	d := &uploadDiagnostic{hash: strings.TrimPrefix(r.URL.Path, "/api/files/uploads/"), length: -1, accepted: -1, started: time.Now()}
	return r.WithContext(context.WithValue(r.Context(), uploadDiagnosticKey{}, d)), d
}

// Informational responses report real verification and publication I/O, never
// timer-only liveness. A stuck read or sync produces no reports, so
// dead-connection detection remains bounded. Opt-in preserves clients that
// cannot consume HTTP 102.
//
// X-Myce-Verification-Reads counts every completed operation, so clients that
// predate publication steps still extend their wait on them (#2171).
// X-Myce-Publication-Steps counts the publication steps among them. A response
// held open only by publication is ordinary final-PATCH service, and the client
// still learns its latency; whole-blob verification reads are not.
const publicationStepsHeader = "X-Myce-Publication-Steps"

type publicationStepKey struct{}

func (s *FileStore) uploadVerificationProgress(w http.ResponseWriter, r *http.Request) *http.Request {
	period, err := time.ParseDuration(r.Header.Get("X-Myce-Upload-Progress"))
	if err != nil || period <= 0 || r.Method != "PATCH" && r.Method != "POST" {
		return r
	}
	// The client window has the RFC 6298 recovery floor. At most four reports
	// per recovery period prevents read-buffer-sized header traffic.
	period = max(period, clientRecoveryPeriod/4)
	var last time.Time
	var operations, steps uint64
	report := func() {
		now := s.clock.Now()
		if !last.IsZero() && now.Sub(last) < period {
			return
		}
		last = now
		w.Header().Set("X-Myce-Verification-Reads", strconv.FormatUint(operations, 10))
		w.Header().Set(publicationStepsHeader, strconv.FormatUint(steps, 10))
		w.WriteHeader(http.StatusProcessing)
		w.Header().Del("X-Myce-Verification-Reads")
		w.Header().Del(publicationStepsHeader)
	}
	previous, _ := r.Context().Value(ioProgressKey{}).(func())
	read := func() {
		if previous != nil {
			previous()
		}
		operations++
		report()
	}
	step := func() {
		operations++
		steps++
		report()
	}
	ctx := context.WithValue(r.Context(), ioProgressKey{}, read)
	return r.WithContext(context.WithValue(ctx, publicationStepKey{}, step))
}

// Preserve error identity for the server while the HTTP envelope stays private.
func uploadFailure(ctx context.Context, operation string, err error) error {
	if err != nil {
		if d, ok := ctx.Value(uploadDiagnosticKey{}).(*uploadDiagnostic); ok {
			d.operation, d.failure = operation, stagingErrorName(err)
		}
	}
	return err
}

func (d *uploadDiagnostic) log(r *http.Request, status int, headers http.Header, body []byte) {
	if status < 500 && r.Method != "POST" && r.Method != "PATCH" {
		return
	}
	if (FileRef{SHA256: d.hash}).validate() != nil {
		d.hash = "invalid"
	}
	requested, _ := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
	if offset, err := strconv.ParseInt(headers.Get("Upload-Offset"), 10, 64); err == nil {
		d.accepted = offset
	}
	// tus errors can include paths. Retain only a known protocol code, never
	// its message, credentials, metadata, URL or a submitted filename.
	code, _, _ := strings.Cut(string(body), ":")
	switch code {
	case "ERR_INTERNAL_SERVER_ERROR", "ERR_READ_TIMEOUT", "ERR_CONNECTION_RESET", "ERR_UNEXPECTED_EOF", "ERR_LOCK_TIMEOUT":
	default:
		code = ""
	}
	attempt, _ := r.Context().Value(fileStoreAttemptKey{}).(fileStoreAttempt)
	job := attempt.ID
	if _, err := hex.DecodeString(job); err != nil || len(job) != 32 {
		job = ""
	}
	var wait, hold time.Duration
	if span := spanFrom(r.Context()); span != nil {
		span.mu.Lock()
		wait, hold = span.wait, span.hold
		span.mu.Unlock()
	}
	log.Printf("event=file_upload method=%s upload=%s job=%s attempt=%d status=%d requested_offset=%d accepted_offset=%d length=%d operation=%s error_class=%s tus_code=%s duration_ms=%.3f body_read_ms=%.3f file_write_ms=%.3f hash_ms=%.3f fsync_ms=%.3f input_claim_ms=%.3f mutex_wait_ms=%.3f mutex_hold_ms=%.3f",
		r.Method, d.hash, job, attempt.Attempt, status, requested, d.accepted, d.length, d.operation, d.failure, code,
		holdMS(time.Since(d.started)), holdMS(d.bodyRead), holdMS(d.fileWrite), holdMS(d.hashBytes), holdMS(d.fileSync), holdMS(d.inputClaim), holdMS(wait), holdMS(hold))
}

// tus answers any other callback error with 500. A queue wait that ended
// because the request did is an interruption, and a stopping queue keeps its
// typed retryable refusal. Genuine storage and persistence failures stay 500.
func uploadCallbackError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return tus.NewError("ERR_UPLOAD_INTERRUPTED", "upload request interrupted", http.StatusRequestTimeout)
	}
	var api *HTTPError
	if errors.As(err, &api) && api.Code == "queue_stopping" {
		body, _ := json.Marshal(map[string]string{"code": api.Code, "error": api.Message})
		return tus.Error{ErrorCode: "ERR_QUEUE_STOPPING", Message: api.Message, HTTPResponse: tus.HTTPResponse{
			StatusCode: api.Status, Body: string(body), Header: tus.HTTPHeader{"Content-Type": "application/json"}}}
	}
	return err
}

// Both ordinary completion and an accepted-end retry run the same durability
// and purpose/custody barriers. No queue mutex is held during file I/O.
func (s *FileStore) finishUpload(ctx context.Context, info tus.FileInfo) error {
	if d, ok := ctx.Value(uploadDiagnosticKey{}).(*uploadDiagnostic); ok {
		d.length, d.accepted = info.Size, info.Offset
	}
	ref := FileRef{info.MetaData["sha256"], info.Size}
	if err := s.publish(ctx, ref); err != nil {
		return err
	}
	// An internal writer may have created the same blob after POST. Honor the
	// import's custody even when publication deduplicates it.
	if err := s.recordUpload(ref.SHA256, info.MetaData["purpose"], false, ctx); err != nil {
		return uploadFailure(ctx, "upload_purpose_commit", err)
	}
	reportPublicationStep(ctx)
	return uploadFailure(ctx, "upload_publication_commit", s.publishUpload(ref.SHA256, ctx))
}

// tus deliberately skips PreFinishResponseCallback when offset==length. A
// previous callback may have failed after accepting the final byte, so perform
// publication again under tus's existing per-upload lock, without replaying
// bytes or replacing the session. A concurrent finisher may remove the session;
// return 404 then and let the client verify the authoritative blob.
func (s *FileStore) resumeUploadPublication(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	lock, err := s.uploadLocker.NewLock(id)
	if err == nil {
		err = lock.Lock(ctx, cancel)
	}
	if err != nil {
		uploadFailure(ctx, "upload_lock", err)
		http.Error(w, "upload lock unavailable", http.StatusServiceUnavailable)
		return
	}
	defer lock.Unlock()
	upload, err := s.getUpload(ctx, id)
	if err != nil {
		if errors.Is(err, tus.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			uploadFailure(ctx, "upload_metadata", err)
			http.Error(w, "upload metadata unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	info, err := upload.GetInfo(ctx)
	if err != nil {
		uploadFailure(ctx, "upload_metadata", err)
		http.Error(w, "upload metadata unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := s.checkAttempt(ctx); err != nil {
		uploadFailure(ctx, "attempt_fence", err)
		respond(w, nil, err)
		return
	}
	if info.Offset != info.Size || info.MetaData["sha256"] != id {
		http.Error(w, "upload state changed", http.StatusConflict)
		return
	}
	err = uploadFailure(ctx, "upload_finish", upload.FinishUpload(ctx))
	if err == nil {
		err = s.finishUpload(ctx, info)
	}
	var refusal tus.Error
	if errors.As(uploadCallbackError(ctx, err), &refusal) {
		for name, value := range refusal.HTTPResponse.Header {
			w.Header().Set(name, value)
		}
		w.WriteHeader(refusal.HTTPResponse.StatusCode)
		_, _ = io.WriteString(w, refusal.HTTPResponse.Body)
		return
	}
	if err != nil {
		http.Error(w, "upload publication unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Tus-Resumable", "1.0.0")
	w.Header().Set("Upload-Offset", strconv.FormatInt(info.Offset, 10))
	w.WriteHeader(http.StatusNoContent)
}
