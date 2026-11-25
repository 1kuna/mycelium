package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkerCheckpointProgressFencesReplacedBacking(t *testing.T) {
	w := recoveryWorker(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"true"})
	prepared, err := w.prepareBoundCheckpoint(context.Background(), j, filepath.Dir(j.Checkpoint), nil, prepareCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	store.state.CheckpointBacking = prepared.backing
	signal := checkpointSignalFile(store.dir, j)
	if err := os.MkdirAll(filepath.Dir(signal), 0700); err != nil {
		t.Fatal(err)
	}
	notify := func(generation string) {
		t.Helper()
		if err := os.WriteFile(signal, []byte(`{"name":"state.json","generation":`+generation+`}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	report := func(want int64) {
		t.Helper()
		if got, err := store.checkpointReport(w, j, false, false); err != nil || got != want {
			t.Fatalf("checkpoint progress = %d, %v, want %d", got, err, want)
		}
	}
	writeSave(t, prepared.path, "7")
	notify("7")
	report(7)
	healthy := j.Checkpoint + "-healthy"
	if err := os.Rename(j.Checkpoint, healthy); err != nil {
		t.Fatal(err)
	}
	writeSave(t, prepared.path, "8")
	notify("8")
	report(-1)
	if got, err := store.checkpointReport(w, j, false, true); err != nil || got != -1 {
		t.Fatalf("finalization read the replaced backing: %d, %v", got, err)
	}
	// The next fallback save is first observed after the original directory
	// comes back. A current mount check alone would accept that notification.
	writeSave(t, prepared.path, "9")
	notify("9")
	if err := os.Rename(j.Checkpoint, j.Checkpoint+"-fallback"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(healthy, j.Checkpoint); err != nil {
		t.Fatal(err)
	}
	report(-1)
	large := `{"myce_checkpoint":{"generation":10},"value":"` + strings.Repeat("x", 1<<20) + `"}`
	if err := os.WriteFile(filepath.Join(prepared.path, "state.json"), []byte(large), 0600); err != nil {
		t.Fatal(err)
	}
	notify("10")
	report(10)
}

func TestCheckpointPreparationRejectsBackingSwapDuringCopy(t *testing.T) {
	j := recoveryJob(t, []string{"true"})
	j.Attempt = 2
	writeSave(t, filepath.Join(j.Checkpoint, "attempt-1"), "7")
	healthy := j.Checkpoint + "-healthy"
	_, err := prepareCheckpointCopy(j, func(dst io.Writer, src io.Reader) (int64, error) {
		if err := os.Rename(j.Checkpoint, healthy); err != nil {
			return 0, err
		}
		writeSave(t, filepath.Join(j.Checkpoint, "attempt-1"), "7")
		return io.Copy(dst, src)
	})
	if !errors.Is(err, errCheckpointBacking) {
		t.Fatalf("copy followed a replaced pathname: %v", err)
	}
	for _, dir := range []string{j.Checkpoint, healthy} {
		if _, err := os.Stat(filepath.Join(dir, "attempt-2")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("copy published after losing its backing: %v", err)
		}
	}
}

func TestQueueFinalizationRejectsReplacedCheckpointRoot(t *testing.T) {
	q, _, m, _, run := yieldFixture(t, false)
	healthy := q.root + "-healthy"
	if err := os.Rename(q.root, healthy); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Rename(q.root, q.root+"-fallback"); err != nil {
			t.Error(err)
		}
		if err := os.Rename(healthy, q.root); err != nil {
			t.Error(err)
		}
	})
	saveYieldGeneration(t, job(t, q, run.ID), 8)
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, run)
	if got := job(t, q, run.ID).CheckpointTarget; got == nil || got.Generation != 1 {
		t.Fatal("queue finalization accepted generation from replaced root", got)
	}
}
