package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// StagingCause names logical inputs and operations, never expanded worker paths
// or untrusted transport text. The underlying error remains in the local chain.
type StagingCause struct {
	Stage          string `json:"stage,omitempty"`
	PathClass      string `json:"path_class,omitempty"`
	BytesCopied    int64  `json:"bytes_copied,omitempty"`
	ExpectedBytes  int64  `json:"expected_bytes,omitempty"`
	Errno          int    `json:"errno,omitempty"`
	RequiredBytes  int64  `json:"required_bytes,omitempty"`
	AvailableBytes int64  `json:"available_bytes,omitempty"`
	LimitBytes     int64  `json:"limit_bytes,omitempty"`
	Capacity       string `json:"capacity,omitempty"`
	Input          string `json:"input,omitempty"`
	Blob           string `json:"blob,omitempty"`
	Operation      string `json:"operation"`
	Error          string `json:"error"`
	Syscall        string `json:"syscall,omitempty"`
	Status         int    `json:"status,omitempty"`
}

func (d StagingCause) String() string {
	text := fmt.Sprintf("input=%q blob=%s operation=%s error=%s", d.Input, d.Blob, d.Operation, d.Error)
	if d.Stage != "" {
		text += fmt.Sprintf(" stage=%s path_class=%s bytes_copied=%d expected_bytes=%d", d.Stage, d.PathClass, d.BytesCopied, d.ExpectedBytes)
	}
	if d.Errno != 0 {
		text += fmt.Sprintf(" errno=%d", d.Errno)
	}
	if d.Syscall != "" {
		text += " syscall=" + d.Syscall
	}
	if d.Status != 0 {
		text += fmt.Sprintf(" status=%d", d.Status)
	}
	if d.Capacity != "" {
		text += fmt.Sprintf(" capacity=%s required_bytes=%d available_bytes=%d limit_bytes=%d", d.Capacity, d.RequiredBytes, d.AvailableBytes, d.LimitBytes)
	}
	return text
}

type stagingError struct {
	detail StagingCause
	cause  error
}

func (e *stagingError) Error() string { return e.detail.String() }
func (e *stagingError) Unwrap() error { return e.cause }

type stagingCapacityError struct {
	required, available, limit int64
	capacity                   string
}

func (e *stagingCapacityError) Error() string {
	return fmt.Sprintf("disk_headroom: %s capacity: required=%d available=%d limit=%d", e.capacity, e.required, e.available, e.limit)
}

var stagingOperations = strings.Fields("store_resolve store_open store_stat store_verify store_refresh store_sync store_seek shared_input_admission prepare cache_init cache_scan cache_admission cache_verify cache_evict cache_download cache_publish copy source_extract source_replace nas_read extension_write batch_write sync publish shared_input model_snapshot submit_verify")
var stagingErrors = strings.Fields("io_error permission_denied timeout cancelled storage_unreachable EOF unexpected_EOF ENOENT EACCES EPERM ENOSPC EDQUOT EIO EROFS ENOTDIR EISDIR EEXIST EMFILE ENFILE ECONNREFUSED ECONNRESET EPIPE blob_missing blob_expired blob_unavailable identity_mismatch invalid_source invalid_cache_entry cache_limit cache_space invalid_input")

func stagingErrorName(err error) string {
	var se *stagingError
	if errors.As(err, &se) {
		return se.detail.Error
	}
	for _, item := range []struct {
		err  error
		name string
	}{
		{errInputTransferUnreachable, "storage_unreachable"},
		{context.DeadlineExceeded, "timeout"}, {errFileTransferStalled, "timeout"}, {errCheckpointIOTimeout, "timeout"}, {context.Canceled, "cancelled"},
		{io.ErrUnexpectedEOF, "unexpected_EOF"}, {io.EOF, "EOF"},
		{syscall.ENOENT, "ENOENT"}, {syscall.EACCES, "EACCES"}, {syscall.EPERM, "EPERM"}, {syscall.ENOSPC, "ENOSPC"}, {syscall.EDQUOT, "EDQUOT"}, {syscall.EIO, "EIO"}, {syscall.EROFS, "EROFS"}, {syscall.ENOTDIR, "ENOTDIR"}, {syscall.EISDIR, "EISDIR"}, {syscall.EEXIST, "EEXIST"}, {syscall.EMFILE, "EMFILE"}, {syscall.ENFILE, "ENFILE"}, {syscall.ECONNREFUSED, "ECONNREFUSED"}, {syscall.ECONNRESET, "ECONNRESET"}, {syscall.EPIPE, "EPIPE"},
	} {
		if errors.Is(err, item.err) {
			return item.name
		}
	}
	if errors.Is(err, os.ErrPermission) {
		return "permission_denied"
	}
	var h *HTTPError
	if errors.As(err, &h) {
		switch {
		case h.Status == 404:
			return "blob_missing"
		case h.Status == 410:
			return "blob_expired"
		default:
			return "blob_unavailable"
		}
	}
	var n net.Error
	if errors.As(err, &n) && n.Timeout() {
		return "timeout"
	}
	switch err.Error() {
	case "file exceeds local cache limit":
		return "cache_limit"
	case "insufficient local file cache space":
		return "cache_space"
	case "invalid cache entry", "unknown file in cache; refusing eviction", "file partial exceeds declared size":
		return "invalid_cache_entry"
	case "frozen native extension source missing", "invalid frozen extension path", "missing frozen source archive", "job files require a root and clean relative names", "attempt input folder is not a directory", "invalid source input name", "input ancestry is not a directory", "input overlaps a source directory":
		return "invalid_input"
	case "stored file identity mismatch", "download identity mismatch", "download size mismatch", "model file size or SHA-256 mismatch", "NAS input SHA-256 mismatch":
		return "identity_mismatch"
	case "invalid NAS input partial":
		return "invalid_cache_entry"
	case "NAS input changed during staging", "NAS input must be a regular file", "invalid source archive name", "source traverses a symlink", "source symlink escapes archive", "source archive contains unsupported file type", "source contains filesystem aliases or reserved names", "archive/tar: invalid tar header":
		return "invalid_source"
	}
	return "io_error"
}
func withStagingCause(input string, ref FileRef, operation string, err error) error {
	if err == nil {
		return nil
	}
	d := StagingCause{Input: input, Blob: ref.SHA256, Operation: operation, Error: stagingErrorName(err)}
	var capacity *stagingCapacityError
	if errors.As(err, &capacity) {
		d.RequiredBytes, d.AvailableBytes, d.LimitBytes, d.Capacity = capacity.required, capacity.available, capacity.limit, capacity.capacity
		d.Operation = "shared_input_admission"
		if capacity.capacity == "file_cache" {
			d.Operation = "cache_admission"
		}
		d.Error = "cache_space"
	}
	var se *stagingError
	if errors.As(err, &se) {
		d = se.detail
		if input != "" {
			d.Input = input
		}
		if ref.SHA256 != "" {
			d.Blob = ref.SHA256
		}
	}
	var path *os.PathError
	var link *os.LinkError
	if errors.As(err, &path) {
		d.Syscall = path.Op
	} else if errors.As(err, &link) {
		d.Syscall = link.Op
	}
	var h *HTTPError
	if errors.As(err, &h) {
		d.Status = h.Status
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		d.Errno = int(errno)
	}
	return &stagingError{d, err}
}
func safeStagingCause(d *StagingCause) *StagingCause {
	if d == nil || !contains(stagingOperations, d.Operation) || !contains(stagingErrors, d.Error) {
		return nil
	}
	if d.Input != "" && !safeRelative(d.Input) {
		return nil
	}
	if d.Blob != "" && (FileRef{SHA256: d.Blob}).validate() != nil {
		return nil
	}
	out := *d
	if !contains(stagingStages, out.Stage) {
		out.Stage = ""
	}
	if !contains(strings.Fields("source cache store"), out.PathClass) {
		out.PathClass = ""
	}
	if out.BytesCopied < 0 || out.ExpectedBytes < 0 || out.BytesCopied > out.ExpectedBytes {
		out.BytesCopied, out.ExpectedBytes = 0, 0
	}
	if out.Errno < 0 {
		out.Errno = 0
	}
	if !contains([]string{"shared_input_cache", "file_cache", "disk_free_floor"}, out.Capacity) || out.RequiredBytes < 0 || out.AvailableBytes < 0 || out.LimitBytes < 0 {
		out.Capacity = ""
		out.RequiredBytes, out.AvailableBytes, out.LimitBytes = 0, 0, 0
	}
	if !contains(strings.Fields("open read write close mkdir mkdirat lstat stat chmod chtimes rename unlink unlinkat remove sync fsync symlink readlink readdir seek truncate"), out.Syscall) {
		out.Syscall = ""
	}
	if out.Status < 400 || out.Status > 599 {
		out.Status = 0
	}
	return &out
}
func bindStagingCause(s Spec, d *StagingCause) *StagingCause {
	d = safeStagingCause(d)
	if d == nil {
		return nil
	}
	if d.Input == "" {
		return d
	}
	if ref, ok := s.Files[d.Input]; ok && ref.SHA256 == d.Blob {
		return d
	}
	if _, ok := s.Inputs[d.Input]; ok && d.Blob == "" {
		return d
	}
	if input, ok := s.SharedInputs[strings.TrimPrefix(d.Input, "shared_inputs/")]; ok && input.SHA256 == d.Blob {
		return d
	}
	return nil
}
func stagingRetryable(err error) bool {
	var re *runtimeError
	if errors.As(err, &re) && re.diagnostic.Predicate != "" {
		return false
	} // Typed immutable model refusal.
	var se *stagingError
	if errors.As(err, &se) {
		if se.detail.Operation == "nas_read" && se.detail.Error == "ENOENT" {
			return false
		}
		if se.detail.Operation == "source_extract" && (se.detail.Error == "EOF" || se.detail.Error == "unexpected_EOF") {
			return false
		}
		switch se.detail.Error {
		case "blob_missing", "blob_expired", "invalid_source", "invalid_input":
			return false
		}
	}
	// Authored NAS absence already has a typed path diagnostic; a different host
	// cannot repair a missing declaration. Other OS/storage/transport failures are infrastructure.
	if pathDiagnostic(err) != nil && errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

func (w Worker) stagingFailure(s *workerStore, j Job, err error) error {
	wrapped := runtimeErrorFor("input_staging", err)
	raw := err
	for errors.Unwrap(raw) != nil {
		raw = errors.Unwrap(raw)
	}
	log.Printf("input staging failed job=%s attempt=%d: %s; cause=%v", j.ID, j.Attempt, wrapped, raw)
	// Create the normal replay spool before process launch so myce logs has the
	// same safe cause even when no payload log could be opened.
	if j.Spec.Logs != "none" {
		name := s.state.Log
		if name == "" {
			name = fmt.Sprintf("staging-%s-%d.log", j.ID, j.Attempt)
			if j.Spec.ephemeral() {
				name = ephemeralLogName(j)
				if err := os.MkdirAll(filepath.Join(s.dir, filepath.Dir(name)), 0700); err != nil {
					return errors.Join(wrapped, err)
				}
			}
		}
		f, e := os.OpenFile(filepath.Join(s.dir, name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e == nil {
			_, e = fmt.Fprintln(f, wrapped.Error())
			e = errors.Join(e, f.Sync(), f.Close())
		}
		if e == nil {
			s.state.Log = name
		}
		if e != nil {
			log.Printf("input staging log unavailable job=%s attempt=%d: %v", j.ID, j.Attempt, e)
		}
	}
	if !stagingRetryable(err) {
		return wrapped
	}
	return s.startFailureAt(j, inputStagingFailed, wrapped, w.Clock.Now())
}
