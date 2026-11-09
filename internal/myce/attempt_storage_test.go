package myce

import (
	"io/fs"
	"os"
	"path/filepath"
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
