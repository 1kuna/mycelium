package myce

import (
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
)

type storageInfoEntry struct {
	fs.DirEntry
	info func() (fs.FileInfo, error)
}

func (d storageInfoEntry) Info() (fs.FileInfo, error) { return d.info() }

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

// Retained regression: #530/#531 ENOENT continuation and EIO incompleteness.
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
