package myce

import (
	"context"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tus/tusd/v2/pkg/filestore"
	tus "github.com/tus/tusd/v2/pkg/handler"
)

type uploadDiagnosticKey struct{}
type uploadDiagnostic struct {
	hash, operation, failure string
	length, accepted         int64
}

func uploadRequestDiagnostic(r *http.Request) (*http.Request, *uploadDiagnostic) {
	d := &uploadDiagnostic{hash: strings.TrimPrefix(r.URL.Path, "/api/files/uploads/"), length: -1, accepted: -1}
	return r.WithContext(context.WithValue(r.Context(), uploadDiagnosticKey{}, d)), d
}

// Informational responses report real verification I/O, never timer-only
// liveness. A stuck read produces no reports, so dead-connection detection
// remains bounded. Opt-in preserves clients that cannot consume HTTP 102.
func (s *FileStore) uploadVerificationProgress(w http.ResponseWriter, r *http.Request) *http.Request {
	period, err := time.ParseDuration(r.Header.Get("X-Myce-Upload-Progress"))
	if err != nil || period <= 0 || r.Method != "PATCH" {
		return r
	}
	// The client window has the RFC 6298 recovery floor. At most four reports
	// per recovery period prevents read-buffer-sized header traffic.
	period = max(period, clientRecoveryPeriod/4)
	var last time.Time
	var reads uint64
	previous, _ := r.Context().Value(ioProgressKey{}).(func())
	progress := func() {
		if previous != nil {
			previous()
		}
		reads++
		now := s.clock.Now()
		if !last.IsZero() && now.Sub(last) < period {
			return
		}
		last = now
		w.Header().Set("X-Myce-Verification-Reads", strconv.FormatUint(reads, 10))
		w.WriteHeader(http.StatusProcessing)
		w.Header().Del("X-Myce-Verification-Reads")
	}
	return r.WithContext(context.WithValue(r.Context(), ioProgressKey{}, progress))
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
	log.Printf("event=file_upload method=%s upload=%s job=%s attempt=%d status=%d requested_offset=%d accepted_offset=%d length=%d operation=%s error_class=%s tus_code=%s",
		r.Method, d.hash, job, attempt.Attempt, status, requested, d.accepted, d.length, d.operation, d.failure, code)
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
	upload, err := filestore.New(filepath.Join(s.root, "uploads")).GetUpload(ctx, id)
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
	if err != nil {
		http.Error(w, "upload publication unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Tus-Resumable", "1.0.0")
	w.Header().Set("Upload-Offset", strconv.FormatInt(info.Offset, 10))
	w.WriteHeader(http.StatusNoContent)
}
