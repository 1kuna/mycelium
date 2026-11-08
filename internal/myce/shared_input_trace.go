package myce

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
)

var stagingStages = strings.Fields("prepare admission resolve open stat clone read write sync close verify permissions manifest metadata publish headers extract")

type sharedInputTraceKey struct{}

// The detached writer owns updates; its caller can snapshot a blocked syscall.
// This lock protects only a few scalar fields, never filesystem or network I/O.
type sharedInputTrace struct {
	mu     sync.Mutex
	detail StagingCause
}

func traceSharedInput(ctx context.Context, stage, pathClass string) {
	if t, ok := ctx.Value(sharedInputTraceKey{}).(*sharedInputTrace); ok {
		t.mu.Lock()
		t.detail.Stage, t.detail.PathClass = stage, pathClass
		t.mu.Unlock()
	}
}

func traceSharedFile(ctx context.Context, ref FileRef) {
	if t, ok := ctx.Value(sharedInputTraceKey{}).(*sharedInputTrace); ok {
		t.mu.Lock()
		t.detail.Blob, t.detail.ExpectedBytes, t.detail.BytesCopied = ref.SHA256, ref.SizeBytes, 0
		t.mu.Unlock()
	}
}

func tracedSharedError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var existing *stagingError
	if errors.As(err, &existing) && existing.detail.Stage != "" {
		return err
	}
	if t, ok := ctx.Value(sharedInputTraceKey{}).(*sharedInputTrace); ok {
		t.mu.Lock()
		d := t.detail
		t.mu.Unlock()
		wrapped := withStagingCause("", FileRef{SHA256: d.Blob}, "shared_input", err).(*stagingError)
		wrapped.detail.Stage, wrapped.detail.PathClass = d.Stage, d.PathClass
		wrapped.detail.BytesCopied, wrapped.detail.ExpectedBytes = d.BytesCopied, d.ExpectedBytes
		return wrapped
	}
	return err
}

// io.Copy writes a Read's returned bytes before returning its accompanying error.
// Preserve that read boundary, but freeze its final committed byte count before
// sync/close can move the active operation. Never mutate an earlier snapshot.
func tracedSharedCopyError(ctx context.Context, err error) error {
	err = tracedSharedError(ctx, err)
	var se *stagingError
	t, ok := ctx.Value(sharedInputTraceKey{}).(*sharedInputTrace)
	if !ok || !errors.As(err, &se) {
		return err
	}
	detail := se.detail
	t.mu.Lock()
	detail.BytesCopied, detail.ExpectedBytes = t.detail.BytesCopied, t.detail.ExpectedBytes
	t.mu.Unlock()
	return &stagingError{detail, err}
}

type sharedInputReader struct {
	ctx       context.Context
	reader    io.Reader
	pathClass string
}

func (r sharedInputReader) Read(p []byte) (int, error) {
	traceSharedInput(r.ctx, "read", r.pathClass)
	n, err := r.reader.Read(p)
	if err == io.EOF {
		return n, err
	}
	return n, tracedSharedError(r.ctx, err)
}

type sharedInputWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w sharedInputWriter) Write(p []byte) (int, error) {
	traceSharedInput(w.ctx, "write", "cache")
	n, err := w.writer.Write(p)
	if t, ok := w.ctx.Value(sharedInputTraceKey{}).(*sharedInputTrace); ok {
		t.mu.Lock()
		t.detail.BytesCopied += int64(n)
		t.mu.Unlock()
	}
	return n, tracedSharedError(w.ctx, err)
}
