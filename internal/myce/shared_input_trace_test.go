package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
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
}

func TestSharedInputTraceStallSnapshotSurvivesLateWriter(t *testing.T) {
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
}
