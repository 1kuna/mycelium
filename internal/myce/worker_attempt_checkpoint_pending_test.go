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
		s, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		// Once the pending read completes, subsequent scans also fail on this
		// unsupported symlink, so the completed-error diagnostic stays observable.
		if err := os.MkdirAll(j.Checkpoint, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), attemptCheckpoint(j)); err != nil {
			t.Fatal(err)
		}
		release := make(chan error, 1)
		defer close(release)
		_, err = s.nasProgress.run(attemptCheckpoint(j), func() (int64, error) {
			return 7, <-release
		})
		if !errors.Is(err, errCheckpointIOPending) {
			t.Fatal("scan did not exceed heartbeat wait", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		a := &attemptSupervisor{store: s, job: j, worker: w, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go a.run(ctx)
		defer func() { cancel(); <-a.done }()
		// Keep the actual actor disconnected so it only observes existing
		// custody; no command or queue is needed to reproduce this diagnostic.
		for {
			report, diagnostic, _, _ := a.snapshot()
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
			_, diagnostic, _, _ := a.snapshot()
			if diagnostic.CheckpointUnavailable {
				break
			}
			time.Sleep(time.Millisecond)
		}
	})
}
