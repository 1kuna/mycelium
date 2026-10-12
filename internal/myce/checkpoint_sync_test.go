package myce

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckpointDirectorySyncCIFSOnly(t *testing.T) {
	for _, cifs := range []bool{false, true} {
		calls := 0
		err := syncCheckpointDirectory(t.TempDir(), func(*os.File) (bool, error) { return cifs, nil }, func(*os.File) error { calls++; return errors.New("sync failed") })
		if cifs {
			if err != nil || calls != 0 {
				t.Fatal(err, calls)
			}
		} else if err == nil || calls != 1 {
			t.Fatal(err, calls)
		}
	}
	calls := 0
	if err := syncCheckpointDirectory(t.TempDir(), func(*os.File) (bool, error) { return false, errors.New("unknown filesystem") }, func(*os.File) error { calls++; return nil }); err != nil || calls != 1 {
		t.Fatal("unknown filesystem must retain sync", err, calls)
	}
}

func TestCheckpointCopySyncsEachDirectoryOnceBeforePublication(t *testing.T) {
	root := filepath.Join(t.TempDir(), "job")
	source := filepath.Join(root, "attempt-1")
	os.MkdirAll(filepath.Join(source, "nested", "deep"), 0700)
	for _, name := range []string{"checkpoint.json", "a", "nested/b", "nested/deep/c"} {
		os.WriteFile(filepath.Join(source, name), []byte(`{"myce_checkpoint":{"generation":4},"value":42}`), 0600)
	}
	var synced []string
	j := Job{ID: "sync-test", Attempt: 2, Checkpoint: root}
	dest, err := prepareCheckpointCopySync(j, io.Copy, func(dir string) error {
		synced = append(synced, dir)
		if dir != root {
			if _, e := os.Stat(attemptCheckpoint(j)); !os.IsNotExist(e) {
				t.Fatal("published before directory durability")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(synced) != 4 || !strings.HasSuffix(synced[0], "nested/deep") || !strings.HasSuffix(synced[1], "nested") || synced[3] != root {
		t.Fatal(synced)
	}
	for _, name := range []string{"checkpoint.json", "a", "nested/b", "nested/deep/c"} {
		if _, err := os.Stat(filepath.Join(dest, name)); err != nil {
			t.Fatal(err)
		}
	}
	// A directory sync failure leaves the new attempt unpublished and source intact.
	j.Attempt = 3
	if _, err := prepareCheckpointCopySync(j, io.Copy, func(string) error { return errors.New("storage down") }); err == nil {
		t.Fatal("sync failure accepted")
	}
	if _, err := os.Stat(attemptCheckpoint(j)); !os.IsNotExist(err) {
		t.Fatal("failed preparation published", err)
	}
}
