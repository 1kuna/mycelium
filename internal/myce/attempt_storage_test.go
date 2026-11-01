package myce

import (
	"context"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type storageInfoEntry struct {
	fs.DirEntry
	info func() (fs.FileInfo, error)
}

func (d storageInfoEntry) Info() (fs.FileInfo, error) { return d.info() }

func TestAttemptStorageInfoError(t *testing.T) {
	for _, vanished := range []bool{true, false} {
		t.Run("vanished="+strconv.FormatBool(vanished), func(t *testing.T) {
			root := t.TempDir()
			publication := t.TempDir()
			stage := filepath.Join(publication, ".file-checkpoint-test")
			if err := os.Mkdir(stage, 0700); err != nil {
				t.Fatal(err)
			}
			payload := filepath.Join(stage, "payload")
			retained := filepath.Join(stage, "z-retained")
			for _, path := range []string{payload, retained} {
				if err := os.WriteFile(path, make([]byte, 65536), 0600); err != nil {
					t.Fatal(err)
				}
			}
			o := &storageObserver{root: root, publication: publication, peak: AttemptStoragePeak{Complete: true, Source: managedStorageSource}}
			o.sample()
			before := *o.snapshot()
			if !before.Complete || before.CheckpointBytes == 0 || before.Samples != 1 {
				t.Fatalf("initial checkpoint sample missing: %+v", before)
			}
			injected, continued := false, false
			o.walkDir = func(root string, visit fs.WalkDirFunc) error {
				return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
					if path == payload && err == nil {
						entry := d
						d = storageInfoEntry{DirEntry: d, info: func() (fs.FileInfo, error) {
							injected = true
							if !vanished {
								return nil, &os.PathError{Op: "lstat", Path: path, Err: syscall.EIO}
							}
							// WalkDir has discovered the real entry; remove it only
							// when scan asks for Info, as publication cleanup can.
							if err := os.Remove(path); err != nil {
								t.Fatal(err)
							}
							return entry.Info()
						}}
					}
					if path == retained {
						continued = true
					}
					return visit(path, d, err)
				})
			}
			o.sample()
			want := before
			want.Complete = vanished
			if vanished {
				want.Samples++
			}
			if got := *o.snapshot(); got != want || !injected || (vanished && !continued) {
				t.Fatalf("Info error lost peak or completeness: got %+v, want %+v; injected=%v continued=%v", got, want, injected, continued)
			}
			// A later clean sample preserves peaks and cannot erase an unknown error.
			o.walkDir = nil
			o.sample()
			want.Samples++
			if got := *o.snapshot(); got != want {
				t.Fatalf("later sample changed retained evidence: got %+v, want %+v", got, want)
			}
		})
	}
}

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
	r := samplePublishedStorage(j, Result{StoragePeak: finished}, state, "owned.log", "custody.json")
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

func checkpointStorageAttempt(t *testing.T) (Job, string, *storageObserver) {
	t.Helper()
	state := t.TempDir()
	root := filepath.Join(state, "job-files", "review", "1")
	checkpoint := filepath.Join(state, "checkpoints", "review", "attempt-1")
	for _, dir := range []string{root, checkpoint} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(checkpoint, "payload"), make([]byte, 65536), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkpoint, "checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":1}}`), 0600); err != nil {
		t.Fatal(err)
	}
	j := Job{ID: "review", Attempt: 1, Checkpoint: filepath.Dir(checkpoint), FileTransport: true, Spec: Spec{Resumable: true}}
	j.Spec.Run.Workdir = root
	if err := os.MkdirAll(attemptPublicationRoot(j, state), 0700); err != nil {
		t.Fatal(err)
	}
	o := (Worker{}).observeAttemptStorage(j, state, checkpoint, "review-owned")
	t.Cleanup(func() { o.finish() })
	return j, state, o
}

func TestAttemptStorageCheckpointPublicationCopyCharged(t *testing.T) {
	j, state, o := checkpointStorageAttempt(t)
	before := o.finish()
	// The original reproducer's immutable copy now lives in the same managed
	// root used by publishFileCheckpoint, rather than shared worker state.
	stage, _, err := captureCheckpoint(attemptCheckpoint(j), attemptPublicationRoot(j, state))
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(stage)
	copyScan, err := (&storageObserver{root: stage}).scan()
	if err != nil {
		t.Fatal(err)
	}
	after := samplePublishedStorage(j, Result{StoragePeak: before}, state).StoragePeak
	t.Logf("original checkpoint=%d, publication copy=%d, managed peak=%d, complete=%v", before.CheckpointBytes, copyScan.allocated, after.ManagedBytes, after.Complete)
	if !after.Complete || after.ManagedBytes < before.ManagedBytes+copyScan.allocated || after.Files < before.Files+copyScan.files {
		t.Fatalf("simultaneous checkpoint copy omitted: %+v, copy %+v", after, copyScan)
	}
}

func TestAttemptStorageCheckpointPublicationLifetime(t *testing.T) {
	for _, final := range []bool{false, true} {
		t.Run("final="+strconv.FormatBool(final), func(t *testing.T) {
			j, state, o := checkpointStorageAttempt(t)
			// Include staging, final spool and an uncategorized managed write
			// alongside checkpoint publication, not just selected byte buckets.
			j.Spec.Outputs = map[string]string{"out": "results"}
			root := attemptStorageRoot(j, state)
			if err := os.WriteFile(filepath.Join(root, "out"), make([]byte, 8192), 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := captureDeliverySpool(j, state); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{".new-publication-path", ".result-spool-pending"} {
				if err := os.MkdirAll(filepath.Join(attemptPublicationRoot(j, state), name), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(attemptPublicationRoot(j, state), name, "copy"), make([]byte, 4096), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(state, "review-owned.log"), make([]byte, 4096), 0600); err != nil {
				t.Fatal(err)
			}
			o.sample()
			before := o.snapshot()
			stored := map[string]FileRef{}
			for _, name := range []string{"payload", "checkpoint.json"} {
				ref, err := IdentifyFile(filepath.Join(attemptCheckpoint(j), name))
				if err != nil {
					t.Fatal(err)
				}
				stored[ref.SHA256] = ref
			}
			copies := make(chan storageScan, 1)
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paths, err := filepath.Glob(filepath.Join(attemptPublicationRoot(j, state), ".file-checkpoint-*"))
				if err != nil || len(paths) != 1 {
					t.Errorf("publication escaped managed root: %v, %v", paths, err)
				} else {
					copy, err := (&storageObserver{root: paths[0]}).scan()
					if err != nil {
						t.Error(err)
					} else {
						select {
						case copies <- copy:
						default:
						}
					}
				}
				if final {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if r.Method == "HEAD" {
					ref := stored[strings.TrimPrefix(r.URL.Path, "/api/files/blobs/")]
					w.Header().Set("ETag", `"`+ref.SHA256+`"`)
					w.Header().Set("Content-Length", strconv.FormatInt(ref.SizeBytes, 10))
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer api.Close()
			w := Worker{Client: Client{URL: api.URL}, storageObserver: o}
			var peak *AttemptStoragePeak
			if final {
				// Model final/recovered publication after the live sampler stops.
				w.storageObserver = nil
				result, err := w.finishFileResult(j, Result{StoragePeak: o.finish()}, state, "review-owned.result.json")
				if err == nil {
					t.Fatal("expected injected upload failure")
				}
				peak = result.StoragePeak
			} else {
				if generation, err := w.publishFileCheckpoint(j, state); err != nil || generation != 1 {
					t.Fatal("live publication failed", generation, err)
				}
				peak = o.finish()
			}
			var copy storageScan
			select {
			case copy = <-copies:
			default:
				t.Fatal("checkpoint copy never reached upload")
			}
			if paths, _ := filepath.Glob(filepath.Join(attemptPublicationRoot(j, state), ".file-checkpoint-*")); len(paths) != 0 {
				t.Fatal("checkpoint staging leaked", paths)
			}
			if !peak.Complete || peak.ManagedBytes < before.ManagedBytes+copy.allocated || peak.Files < before.Files+copy.files || peak.CheckpointBytes < before.CheckpointBytes+copy.allocated {
				t.Fatalf("publication peak lost after cleanup: before %+v, copy %+v, after %+v", before, copy, peak)
			}
		})
	}
}
