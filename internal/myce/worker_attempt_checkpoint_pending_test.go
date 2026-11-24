package myce

import (
	"context"
	"errors"
	"os"
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
		j.Machine, j.Session = w.Machine.ID, root.state.Session
		s, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		// Finalization is an event; its completed-error diagnostic stays cached
		// without periodic attempts to reread the unsupported symlink.
		if err := os.MkdirAll(j.Checkpoint, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), attemptCheckpoint(j)); err != nil {
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
		r := result(&j, true)
		s.state.Result = &r
		a := &attemptSupervisor{store: s, job: j, worker: w, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go a.run(ctx)
		defer func() { cancel(); <-a.done }()
		// Keep the finalizing actor disconnected; no command or queue is
		// needed to reproduce this diagnostic.
		for {
			report, diagnostic, finished, err := a.snapshot()
			if finished || err != nil {
				t.Fatalf("actor exited during pending checkpoint observation: %v", err)
			}
			if report.ID == j.ID {
				if diagnostic.CheckpointUnavailable {
					t.Fatal("pending attempt scan marked the entire host unavailable")
				}
				break
			}
			time.Sleep(time.Millisecond)
		}
		// A completed scan error remains a real availability failure.
		release <- errors.New("checkpoint read failed")
		for {
			_, diagnostic, finished, err := a.snapshot()
			if finished || err != nil {
				t.Fatalf("actor exited before reporting the scan failure: %v", err)
			}
			if diagnostic.CheckpointUnavailable {
				break
			}
			time.Sleep(time.Millisecond)
		}
	})
}
