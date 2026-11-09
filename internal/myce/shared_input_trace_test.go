package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type sharedTraceFaultWriter struct{}

func (sharedTraceFaultWriter) Write(p []byte) (int, error) {
	return 3, &os.PathError{Op: "write", Path: "/private/cache", Err: syscall.ENOSPC}
}

func TestSharedInputTracePartialWriteRetainsCauseOnAttempt(t *testing.T) {
	q, clock := fixture(t)
	spec := submitVerificationInput(t, q)
	ref := spec.Files["input"]
	spec.SharedInputs = map[string]SharedInput{"source": {SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}
	j := submit(t, q, spec)
	m := machine("staging", "fake")
	m.Capabilities = append(m.Capabilities, "shared_inputs_v1")
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("not assigned")
	}
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
	if err := cache.cache.init(); err != nil {
		t.Fatal(err)
	}
	err := cache.stageInput(context.Background(), func(ctx context.Context, _ string) error {
		traceSharedFile(ctx, ref)
		_, err := io.Copy(sharedInputWriter{ctx, sharedTraceFaultWriter{}}, strings.NewReader("partial write"))
		return err
	}, func(string) error { t.Fatal("published failed input"); return nil })
	w := Worker{StateDir: newTestStateRoot(t), Clock: clock, Machine: m}
	store, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer store.close()
	store.state.Job, store.state.Session = assigned, assigned.Session
	err = withStagingCause("shared_inputs/source", ref, "shared_input", err)
	r := startFailureOutcome(result(assigned, false), w.stagingFailure(store, *assigned, err))
	if e = q.Complete(r); e != nil {
		t.Fatal(e)
	}
	d := job(t, q, j.ID).AttemptHistory[0].ExecutionCause.Staging
	if d == nil || d.Stage != "write" || d.PathClass != "cache" || d.Error != "ENOSPC" || d.Errno != int(syscall.ENOSPC) || d.BytesCopied != 3 || d.ExpectedBytes != ref.SizeBytes {
		t.Fatalf("cause lost: %+v", d)
	}
	b, _ := json.Marshal(d)
	if strings.Contains(string(b), "/private/") {
		t.Fatal("path leaked", string(b))
	}

	t.Run("stalled-writer-frozen-cause", func(t *testing.T) {

		clock := &workerFakeClock{now: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)}
		cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
		if err := cache.cache.init(); err != nil {
			t.Fatal(err)
		}
		ready, release := make(chan struct{}), make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- cache.stageInput(context.Background(), func(ctx context.Context, _ string) error {
				traceSharedFile(ctx, FileRef{SHA256: strings.Repeat("a", 64), SizeBytes: 100})
				_, err := (sharedInputWriter{ctx, io.Discard}).Write([]byte("copied"))
				if err != nil {
					return err
				}
				traceSharedInput(ctx, "sync", "cache")
				close(ready)
				<-release
				traceSharedInput(ctx, "verify", "cache")
				return nil
			}, func(string) error { t.Error("late publication"); return nil })
		}()
		<-ready
		pending := onlySharedStaging(t, cache)
		clock.Advance(checkpointAvailabilityStallBudget)
		var err error
		select {
		case err = <-done:
		case <-time.After(testWaitTimeout):
			close(release)
			t.Fatal("did not withdraw stalled writer")
		}
		close(release)
		<-pending.done
		var se *stagingError
		if !errors.Is(err, errCheckpointIOTimeout) || !errors.As(err, &se) {
			t.Fatal(err)
		}
		if se.detail.Stage != "sync" || se.detail.PathClass != "cache" || se.detail.BytesCopied != 6 || se.detail.ExpectedBytes != 100 || se.detail.Error != "timeout" || se.detail.Errno != 0 {
			t.Fatalf("wrong stall snapshot: %+v", se.detail)
		}

	})

	t.Run("snapshot-before-cancel", func(t *testing.T) {

		clock := &workerFakeClock{now: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)}
		cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
		if err := cache.cache.init(); err != nil {
			t.Fatal(err)
		}
		ready, advanced := make(chan struct{}), make(chan struct{})
		cache.afterStageCancel = func() { <-advanced }
		done := make(chan error, 1)
		go func() {
			done <- cache.stageInput(context.Background(), func(ctx context.Context, _ string) error {
				traceSharedFile(ctx, FileRef{SHA256: strings.Repeat("a", 64), SizeBytes: 100})
				traceSharedInput(ctx, "read", "source")
				close(ready)
				<-ctx.Done()
				_, _ = (sharedInputWriter{ctx, io.Discard}).Write([]byte("late"))
				traceSharedInput(ctx, "close", "cache")
				close(advanced)
				return ctx.Err()
			}, func(string) error { t.Error("late publication"); return nil })
		}()
		<-ready
		pending := onlySharedStaging(t, cache)
		// Force the cancelled writer to advance before the outer tracing defer runs.
		// Only the snapshot taken at the timeout decision can preserve the read stall.
		clock.Advance(checkpointAvailabilityStallBudget)
		var err error
		select {
		case err = <-done:
		case <-time.After(testWaitTimeout):
			t.Fatal("timeout did not withdraw")
		}
		<-advanced
		<-pending.done
		var se *stagingError
		if !errors.Is(err, errCheckpointIOTimeout) || !errors.As(err, &se) {
			t.Fatal(err)
		}
		if se.detail.Stage != "read" || se.detail.PathClass != "source" || se.detail.BytesCopied != 0 {
			t.Fatalf("post-cancel trace leaked: %+v", se.detail)
		}

	})

	t.Run("partial-read-boundary", func(t *testing.T) {

		for _, pathClass := range []string{"source", "store"} {
			t.Run(pathClass, func(t *testing.T) {
				ctx := context.WithValue(context.Background(), sharedInputTraceKey{}, &sharedInputTrace{detail: StagingCause{Operation: "shared_input", ExpectedBytes: 10}})
				var output strings.Builder
				n, err := io.Copy(sharedInputWriter{ctx, &output}, sharedInputReader{ctx, sharedTraceFaultReader{}, pathClass})
				err = tracedSharedCopyError(ctx, err)
				traceSharedInput(ctx, "sync", "cache")
				var se *stagingError
				if n != 3 || output.String() != "abc" || !errors.Is(err, syscall.EIO) || !errors.As(err, &se) {
					t.Fatal(n, output.String(), err)
				}
				if se.detail.Stage != "read" || se.detail.PathClass != pathClass || se.detail.BytesCopied != 3 || se.detail.Errno != int(syscall.EIO) {
					t.Fatalf("read cause lost: %+v", se.detail)
				}
			})
		}

	})

	t.Run("download-body-store-boundary", func(t *testing.T) {

		ref := FileRef{SHA256: strings.Repeat("a", 64), SizeBytes: 10}
		ctx := context.WithValue(context.Background(), sharedInputTraceKey{}, &sharedInputTrace{detail: StagingCause{Operation: "shared_input"}})
		c := Client{URL: "http://fixture.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, ContentLength: 10, Header: http.Header{"Etag": []string{`"` + ref.SHA256 + `"`}}, Body: io.NopCloser(sharedTraceFaultReader{}), Request: r}, nil
		})}}
		dest := filepath.Join(t.TempDir(), "data")
		err := c.DownloadFile(ctx, ref, dest)
		b, e := os.ReadFile(dest)
		if e != nil || string(b) != "abc" {
			t.Fatal(string(b), e)
		}
		var se *stagingError
		if !errors.Is(err, syscall.EIO) || !errors.As(err, &se) {
			t.Fatal(err)
		}
		if se.detail.Stage != "read" || se.detail.PathClass != "store" || se.detail.BytesCopied != 3 || se.detail.Errno != int(syscall.EIO) {
			t.Fatalf("store read lost: %+v", se.detail)
		}

	})
}

type sharedTraceFaultReader struct{}

func (sharedTraceFaultReader) Read(p []byte) (int, error) {
	copy(p, "abc")
	return 3, &os.PathError{Op: "read", Path: "/private/source", Err: syscall.EIO}
}
