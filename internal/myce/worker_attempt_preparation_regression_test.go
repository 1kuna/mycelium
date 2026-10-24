package myce

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestNewAttemptBlockedCheckpointPreparationIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := recoveryWorker(t)
		root, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer root.close()
		j := recoveryJob(t, []string{"true"})
		store, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		release, joined := make(chan struct{}), make(chan struct{})
		w.checkpointPrepare = func(Job) (string, error) {
			defer close(joined)
			<-release
			return "", errors.New("NAS unreachable")
		}
		started := time.Now()
		p, err := w.start(store, j)
		elapsed := time.Since(started)
		close(release)
		<-joined
		if p != nil || !errors.Is(err, errWorkerCheckpoint) {
			t.Fatalf("blocked NAS preparation must defer attempt startup: process=%v err=%v", p, err)
		}
		if elapsed != checkpointPreparationBudget {
			t.Fatalf("new attempt checkpoint preparation waited %s; want budget %s", elapsed, checkpointPreparationBudget)
		}
	})
}
