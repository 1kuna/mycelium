package myce

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSharedInputsDevReusesTreeWithStoredSource(t *testing.T) {
	state := t.TempDir()
	if err := PrepareStateRoots(state); err != nil {
		t.Fatal(err)
	}
	nas := filepath.Join(state, "nas")
	if err := os.Mkdir(nas, 0700); err != nil {
		t.Fatal(err)
	}
	input := sharedArchive(t, nas)
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: nas, NASCheckpoints: true, FileStore: filepath.Join(state, "files"), GPU: "cpu", Workers: 1, Token: "test", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ref := sharedFixtureFile(t, t.TempDir(), "source", "source")
	source := filepath.Join(t.TempDir(), "source")
	if err = os.WriteFile(source, []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = d.Client().UploadFile(ctx, source, ref); err != nil {
		t.Fatal(err)
	}
	var previous string
	for i := 0; i < 3; i++ {
		privateFiles := FileManifest{"source": ref}
		if i == 2 {
			privateFiles = nil
		}
		j, e := d.Client().Submit(ctx, Spec{Name: "shared", Project: "test", Run: RunSpec{Command: []string{"sh", "-ec", `test "$(cat "$MYCE_INPUT_RUNTIME/bin/run")" = hello; printf '%s' "$MYCE_INPUT_RUNTIME" > result`}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}, Files: privateFiles, SharedInputs: map[string]SharedInput{"runtime": input}, Outputs: map[string]string{"result": "results"}})
		if e != nil {
			t.Fatal(e)
		}
		for {
			current, e := d.Client().GetJob(ctx, j.ID)
			if e != nil {
				t.Fatal(e)
			}
			if terminal(current.State) {
				if current.State != "succeeded" {
					t.Fatal(current.State, current.Reason)
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
		out := filepath.Join(t.TempDir(), "result")
		if e = d.Client().DownloadResult(ctx, j.ID, "result", out); e != nil {
			t.Fatal(e)
		}
		b, e := os.ReadFile(out)
		if e != nil {
			t.Fatal(e)
		}
		if i > 0 && string(b) != previous {
			t.Fatal("per-job copy", previous, string(b))
		}
		previous = string(b)
		if i == 0 {
			if e = os.Remove(filepath.Join(nas, "runtime.tar")); e != nil {
				t.Fatal(e)
			}
		}
	}
	cacheRoot := filepath.Join(state, "workers", "fake-1", "file-cache", "shared-inputs")
	t.Cleanup(func() { _ = removeSharedEntry(cacheRoot) })
}
