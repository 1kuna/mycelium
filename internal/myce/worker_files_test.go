package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type sharedCleanupDeniedFS struct{ fakeMemoryFS }

func (f sharedCleanupDeniedFS) WriteFile(path string, data []byte) error {
	if filepath.Base(path) == "cgroup.kill" {
		return os.ErrPermission
	}
	return f.fakeMemoryFS.WriteFile(path, data)
}

func TestSharedInputsRemainProtectedUntilCleanupRetrySucceeds(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		name := "command"
		if grouped {
			name = "native_group"
		}
		t.Run(name, func(t *testing.T) {
			nas := t.TempDir()
			input := sharedArchive(t, nas)
			cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20}}
			t.Cleanup(func() { _ = removeSharedEntry(cache.cache.Root) })
			w := recoveryWorker(t)
			w.CheckpointRoot, w.sharedInputs = nas, cache
			w.memoryCeilings = fakeCeilings(t)
			w.memoryCeilings.fs = sharedCleanupDeniedFS{}
			w.HostMemorySource = func(context.Context) (float64, error) { return 31, nil }
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			j := recoveryJob(t, []string{"sh", "-ec", `test "$(cat "$MYCE_INPUT_RUNTIME/bin/run")" = hello`})
			j.Spec.Resumable = false
			j.Spec.Needs.HostMemoryGB = 1
			j.Spec.SharedInputs = map[string]SharedInput{"runtime": input}
			if grouped {
				j.Spec.Run.MemoryGB = 1
			}
			var p *process
			// Reuse pending input preparation before exercising cleanup custody.
			await(t, func() bool {
				p, err = w.start(store, j)
				return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
			})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case f := <-p.done:
				if f.err != nil || !f.result.ReleasePending {
					t.Fatal("expected unconfirmed cleanup", f)
				}
				store.state.Result, store.state.PID = &f.result, 0
			case <-time.After(10 * time.Second):
				p.stop(w.Clock.Now(), 0, true)
				t.Fatal("command did not finish")
			}
			path := filepath.Join(cache.cache.Root, input.key(), "data")
			cache.cache.Limit = 1
			protected := func() {
				t.Helper()
				if err := cache.admit(context.Background(), "other", 10); err == nil {
					t.Fatal("evicted input before cleanup confirmation")
				}
				if _, err := os.Stat(path); err != nil {
					t.Fatal("shared input removed", err)
				}
			}
			protected()
			if err = store.cleanupOrphan(w.Clock, sharedCleanupDeniedFS{}); err == nil {
				t.Fatal("cleanup retry ignored permission failure")
			}
			protected()
			if err = store.cleanupOrphan(w.Clock, fakeMemoryFS{}); err != nil {
				t.Fatal(err)
			}
			_ = cache.admit(context.Background(), "other", 10)
			if _, err = os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("confirmed cleanup did not release cache protection", err)
			}
		})
	}
}

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
		// Published scratch is disposable even for the shared-only third job.
		// The next job must still reuse the shared tree after that scratch is gone.
		scratch := filepath.Join(state, "workers", "fake-1", "job-files", j.ID)
		for {
			_, e := os.Stat(scratch)
			if os.IsNotExist(e) {
				break
			}
			if e != nil {
				t.Fatal(e)
			}
			select {
			case <-ctx.Done():
				t.Fatal("published attempt scratch retained", j.ID, ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
		if i == 0 {
			if e = os.Remove(filepath.Join(nas, "runtime.tar")); e != nil {
				t.Fatal(e)
			}
		}
	}
	cacheRoot := filepath.Join(state, "workers", "fake-1", "file-cache", "shared-inputs")
	t.Cleanup(func() { _ = removeSharedEntry(cacheRoot) })
}

// A cancelled running attempt keeps its local work for the recovery grace.
func TestCancelledAttemptPartialFilesRemainRecoverable(t *testing.T) {
	state := t.TempDir()
	if err := PrepareStateRoots(state); err != nil {
		t.Fatal(err)
	}
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), GPU: "cpu", Workers: 1, Token: "test", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ref := sharedFixtureFile(t, t.TempDir(), "source", "source")
	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := d.Client().UploadFile(ctx, source, ref); err != nil {
		t.Fatal(err)
	}
	j, err := d.Client().Submit(ctx, Spec{Name: "partial", Project: "test", Run: RunSpec{Command: []string{"sh", "-ec", "printf partial > result; exec sleep 60"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}, Files: FileManifest{"source": ref}, Outputs: map[string]string{"result": "results"}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "workers", "fake-1", "job-files", j.ID, "1", "result")
	for {
		if b, e := os.ReadFile(path); e == nil && string(b) == "partial" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("partial output not written", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err := d.Client().Cancel(ctx, j.ID); err != nil {
		t.Fatal(err)
	}
	for {
		current, e := d.Client().GetJob(ctx, j.ID)
		if e != nil {
			t.Fatal(e)
		}
		if current.State == "cancelled" && current.ReleaseConfirmed {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("release not confirmed", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	workerDir := filepath.Join(state, "workers", "fake-1")
	now := time.Now()
	for {
		if err := workerRetention(workerDir, now, DefaultLogRetention); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(filepath.Dir(path), unclaimedMarker)); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("worker custody did not clear", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "partial" {
		t.Fatalf("partial output unavailable: %q %v", b, err)
	}
	if err := workerRetention(workerDir, now.Add(24*time.Hour), DefaultLogRetention); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("partial output retained beyond grace: %v", err)
	}
}
