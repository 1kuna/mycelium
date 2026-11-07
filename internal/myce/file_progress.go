package myce

import (
	"context"
	"io"
)

// FileProgress reports bytes hashed or server-acknowledged by the upload
// protocol, never bytes merely handed to a socket. Callbacks are synchronous.
type FileProgress struct {
	Phase        string
	Bytes, Total int64
}

type fileProgressKey struct{}

// WithFileProgress observes local hashing and reconciled upload acknowledgements.
func WithFileProgress(ctx context.Context, report func(FileProgress)) context.Context {
	if previous, ok := ctx.Value(fileProgressKey{}).(func(FileProgress)); ok && previous != nil && report != nil {
		current := report
		report = func(p FileProgress) { current(p); previous(p) }
	}
	return context.WithValue(ctx, fileProgressKey{}, report)
}

func reportFileProgress(ctx context.Context, phase string, bytes, total int64) {
	if report, ok := ctx.Value(fileProgressKey{}).(func(FileProgress)); ok && report != nil {
		report(FileProgress{phase, bytes, total})
	}
}

type fileHashProgress struct {
	ctx          context.Context
	in           io.Reader
	bytes, total int64
}

func (r *fileHashProgress) Read(p []byte) (int, error) {
	n, err := r.in.Read(p)
	r.bytes += int64(n)
	reportFileProgress(r.ctx, "hashing", r.bytes, r.total)
	return n, err
}
