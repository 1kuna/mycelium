package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

// A checkpoint scan on the contended share took 32s while the independent
// root probe's operations all succeeded within its stall bound. A heartbeat
// budget expiration in that scan must not fence other jobs on the host.
func TestAttemptPendingCheckpointScanDoesNotFenceHost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := recoveryWorker(t)
		w.control = &processControl{}
		root, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer root.close()
		j := recoveryJob(t, []string{"true"})
		s, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		// Finalization is an event; its completed-error diagnostic stays cached
		// without periodic attempts to reread the unsupported symlink.
		path := attemptCheckpoint(j)
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
		backing, err := checkpointDirectoryIdentity(os.Stat(path))
		if err != nil {
			t.Fatal(err)
		}
		s.state.CheckpointBacking = &checkpointBacking{Attempt: backing}
		if err := os.Symlink(t.TempDir(), filepath.Join(path, "unsupported")); err != nil {
			t.Fatal(err)
		}
		release := make(chan error, 1)
		defer close(release)
		_, err = s.nasProgress.event(w.Clock, "final", attemptCheckpoint(j), func() (int64, error) {
			return 7, <-release
		})
		if !errors.Is(err, errCheckpointIOPending) {
			t.Fatal("scan did not exceed heartbeat wait", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.state.Result = &Result{Machine: w.Machine.ID, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
		a := &attemptSupervisor{store: s, job: j, worker: w, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go a.run(ctx)
		defer func() { cancel(); <-a.done }()
		// Keep the finalizing actor disconnected; no command or queue is
		// needed to reproduce this diagnostic.
		synctest.Wait()
		report, diagnostic, finished, err := a.snapshot()
		if err != nil || finished || report.ID != j.ID {
			t.Fatalf("finalizing attempt did not report: %+v, finished=%t, err=%v", report, finished, err)
		}
		if diagnostic.CheckpointUnavailable {
			t.Fatal("pending attempt scan marked the entire host unavailable")
		}
		// A completed scan error remains a real availability failure.
		release <- errors.New("checkpoint read failed")
		synctest.Wait()
		time.Sleep(w.Interval)
		synctest.Wait()
		if _, diagnostic, _, err := a.snapshot(); err != nil || !diagnostic.CheckpointUnavailable {
			t.Fatalf("completed checkpoint scan error did not report unavailability: %v", err)
		}
	})
}
