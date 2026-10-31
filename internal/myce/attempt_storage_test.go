package myce

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestAttemptStorageAllocatedPeaksSpoolAndCheckpoint(t *testing.T) {
	state := t.TempDir()
	j := Job{ID: "storage", Attempt: 1, Machine: "worker", Session: "session", Spec: Spec{Outputs: map[string]string{"out": "results"}}}
	root := filepath.Join(state, "job-files", j.ID, strconv.Itoa(j.Attempt))
	checkpoint := filepath.Join(state, "checkpoints", j.ID, "attempt-1")
	for _, dir := range []string{root, checkpoint} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path string, n int) {
		t.Helper()
		if err := os.WriteFile(path, make([]byte, n), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "input"), 3000)
	j.Spec.Run.Workdir = root
	j.Checkpoint = filepath.Dir(checkpoint)
	clock := &workerFakeClock{now: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
	o := (Worker{Clock: clock}).observeAttemptStorage(j, state, checkpoint, "owned", "custody.json")
	defer o.finish()
	write(filepath.Join(root, "out"), 16000)
	write(filepath.Join(root, "temporary"), 6000)
	write(filepath.Join(checkpoint, "save"), 2000)
	write(filepath.Join(state, "owned.log"), 500)
	write(filepath.Join(state, "custody.json"), 4096)
	// A hardlink must not double-charge physical blocks/inodes.
	if err := os.Link(filepath.Join(root, "temporary"), filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	// An outside symlink must not walk another attempt or private data.
	outside := t.TempDir()
	write(filepath.Join(outside, "large"), 100000)
	if err := os.Symlink(outside, filepath.Join(root, "outside")); err != nil {
		t.Fatal(err)
	}
	o.sample()
	o.mu.Lock()
	first := o.peak
	o.mu.Unlock()
	if !first.Complete || first.ScratchBytes < 10000 || first.MetadataBytes == 0 || first.CheckpointBytes == 0 || first.StagedInputBytes == 0 || first.Files < 5 {
		t.Fatalf("allocated measurements missing: %+v", first)
	}
	if err := os.Remove(filepath.Join(root, "temporary")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	finished := o.finish()
	if finished.ScratchBytes != first.ScratchBytes {
		t.Fatal("deleted scratch peak lost", finished)
	}
	// Create the actual immutable synced spool; final work and spool coexist.
	if _, _, err := captureDeliverySpool(j, state); err != nil {
		t.Fatal(err)
	}
	r := samplePublishedStorage(j, Result{StoragePeak: finished}, state)
	if r.StoragePeak.SpoolBytes < 16000 || r.StoragePeak.ManagedBytes < r.StoragePeak.SpoolBytes+16000+first.CheckpointBytes+first.MetadataBytes || r.StoragePeak.ScratchBytes != first.ScratchBytes {
		t.Fatal("simultaneous spool peak lost", r.StoragePeak)
	}
	if got := roundStorage(r.StoragePeak.ManagedBytes, finished.AllocationUnit); got < r.StoragePeak.ManagedBytes || got%finished.AllocationUnit != 0 {
		t.Fatal("allocation rounding", got)
	}
}
func TestAttemptStorageNativeRunnerAndGroup(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(strconv.FormatBool(grouped), func(t *testing.T) {
			w := recoveryWorker(t)
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = "storage-session"
			w.HostMemorySource = func(context.Context) (float64, error) { return 32, nil }
			w.memoryCeilings = fakeCeilings(t)
			w.checkpointPrepare = func(j Job) (string, error) {
				dir := filepath.Join(store.dir, "checkpoints", j.ID, "attempt-1")
				return dir, os.MkdirAll(dir, 0700)
			}
			j := recoveryJob(t, []string{"sh", "-c", `head -c 4096 /dev/zero > out; head -c 4096 /dev/zero > "$MYCE_SCRATCH_DIR/temp"; head -c 4096 /dev/zero > "$MYCE_CHECKPOINT_DIR/save"`})
			j.Spec.Resumable = false
			if grouped {
				j.Spec.Run.MemoryGB = 1
				j.Spec.Needs.HostMemoryGB = 2
			}
			p, err := startAfterFixtureInputs(t, w, store, j)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case f := <-p.done:
				if f.err != nil || !f.result.Success {
					t.Fatalf("runner: %+v", f)
				}
				peak := f.result.StoragePeak
				if peak == nil || !peak.Complete || peak.ScratchBytes == 0 || peak.CheckpointBytes == 0 || peak.MetadataBytes == 0 || peak.Files == 0 {
					t.Fatalf("real runner did not retain allocated peaks: %+v", peak)
				}
			case <-time.After(testWaitTimeout):
				p.stop(w.Clock.Now(), 0, true)
				t.Fatal("runner stalled")
			}
		})
	}
}
func TestAttemptStorageUnmanagedPathsRemainIncomplete(t *testing.T) {
	state := t.TempDir()
	j := Job{ID: "storage", Attempt: 1}
	root := filepath.Join(state, "job-files", j.ID, "1")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	j.Spec.Run.Workdir = t.TempDir()
	w := Worker{Clock: RealClock{}}
	o := w.observeAttemptStorage(j, state, t.TempDir(), "owned")
	if p := o.finish(); p.Complete {
		t.Fatal("external work/checkpoint coverage claimed", p)
	}
	j.Spec.Run.Workdir = root
	j.Spec.Run.Docker = true
	if p := w.observeAttemptStorage(j, state, "", "owned").finish(); p.Complete {
		t.Fatal("Docker overlay coverage claimed", p)
	}
}
