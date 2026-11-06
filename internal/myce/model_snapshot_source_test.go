package myce

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Samba/CIFS mfsymlinks encode an otherwise ordinary symlink as a 1067-byte
// regular file on volumes without symlink support. No SMB server is needed.
func emulateModelSymlink(t *testing.T, file string) string {
	t.Helper()
	target, err := os.Readlink(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf("XSym\n%04d\n%x\n%s", len(target), md5.Sum([]byte(target)), target)
	content += strings.Repeat(" ", 1067-len(content))
	if err := os.WriteFile(file, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return target
}

func TestModelSnapshotEmulatedSymlinkBytes(t *testing.T) {
	root := t.TempDir()
	repo := tinyModel(t, root)
	for _, name := range []string{"model.safetensors", "config.json"} {
		emulateModelSymlink(t, filepath.Join(repo, "snapshots", tinyRevision, name))
	}
	w := snapshotWorker(t, root, &fakeClock{now: time.Unix(1000, 0)})
	paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for name, want := range map[string]string{"model.safetensors": tinyWeights, "config.json": tinyConfig} {
		got, err := os.ReadFile(filepath.Join(paths["MYCE_MODEL_BASE"], name))
		if err != nil || string(got) != want {
			t.Errorf("%s: got %q, error %v; want real blob bytes %q", name, got, err, want)
		}
	}
}

func TestModelSnapshotEmulatedSymlinkRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, predicate string
		change          func(*testing.T, string, string)
	}{
		{"changed blob", "model_snapshot_mismatch", func(t *testing.T, file, blob string) {
			// Equal size makes this a hash failure, not merely a size failure.
			if err := os.WriteFile(blob, []byte(strings.Repeat("x", len(tinyWeights))), 0644); err != nil {
				t.Fatal(err)
			}
		}},
		{"broken format checksum", "model_snapshot_mismatch", func(t *testing.T, file, blob string) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			data[10] = 'z'
			if err := os.WriteFile(file, data, 0644); err != nil {
				t.Fatal(err)
			}
		}},
		{"escape", "model_snapshot_escape", func(t *testing.T, file, blob string) {
			if err := os.Remove(file); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("../../../../private", file); err != nil {
				t.Fatal(err)
			}
			emulateModelSymlink(t, file)
		}},
		{"missing blob", "model_snapshot_mismatch", func(t *testing.T, file, blob string) {
			if err := os.Remove(blob); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			repo := hubRepo(t, root, "org/tiny", tinyRevision, map[string]string{"model.safetensors": tinyWeights})
			file := filepath.Join(repo, "snapshots", tinyRevision, "model.safetensors")
			target := emulateModelSymlink(t, file)
			tc.change(t, file, filepath.Join(filepath.Dir(file), target))
			w := snapshotWorker(t, root, &fakeClock{now: time.Unix(1000, 0)})
			paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
			for _, release := range releases {
				release()
			}
			if err == nil || runtimeDiagnostic(err).Predicate != tc.predicate || len(paths) != 0 {
				t.Fatalf("unverified bytes exposed: paths=%v diagnostic=%+v error=%v", paths, runtimeDiagnostic(err), err)
			}
		})
	}
}

func TestModelSnapshotEmulatedSymlinkRestartCustody(t *testing.T) {
	if root := os.Getenv("MYCE_STUB_CUSTODY_HELPER"); root != "" {
		clock := &fakeClock{now: time.Unix(1000, 0)}
		w := Worker{ModelRoot: filepath.Join(root, "models"), modelSnapshots: &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "cache"), Limit: 1 << 20, Clock: clock}}}
		owner, err := newAttemptStore(&workerStore{dir: filepath.Join(root, "attempt")}, Job{ID: "stub-reader", Attempt: 1, Session: "session"})
		if err != nil {
			t.Fatal(err)
		}
		paths, _, err := w.acquireModelSnapshots(context.WithValue(context.Background(), sharedInputOwnerKey{}, owner), snapshotJob("org/tiny"))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(paths["MYCE_MODEL_BASE"], "model.safetensors"))
		if err != nil || string(got) != tinyWeights {
			t.Fatalf("not real bytes: %q %v", got, err)
		}
		os.Exit(0) // No release: process locks disappear, durable attempt custody stays.
	}
	root := t.TempDir()
	t.Cleanup(func() { _ = removeSharedEntry(root) })
	repo := tinyModel(t, filepath.Join(root, "models"))
	for _, name := range []string{"model.safetensors", "config.json"} {
		emulateModelSymlink(t, filepath.Join(repo, "snapshots", tinyRevision, name))
	}
	if err := os.Mkdir(filepath.Join(root, "attempt"), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestModelSnapshotEmulatedSymlinkRestartCustody$")
	cmd.Env = append(os.Environ(), "MYCE_STUB_CUSTODY_HELPER="+root)
	if data, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("helper: %v: %s", err, data)
	}
	clock := &fakeClock{now: time.Unix(10000, 0)}
	cache := &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(root, "cache"), Limit: 1, Clock: clock}}
	if err := cache.admit(context.Background(), "another", 10); err == nil {
		t.Fatal("restart evicted active model bytes")
	}
	w := Worker{ModelRoot: filepath.Join(root, "models"), modelSnapshots: cache}
	paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
	if err != nil {
		t.Fatal(err)
	}
	for _, release := range releases {
		release()
	}
	model := filepath.Join(paths["MYCE_MODEL_BASE"], "model.safetensors")
	if data, err := os.ReadFile(model); err != nil || string(data) != tinyWeights {
		t.Fatalf("restart bytes: %q %v", data, err)
	}
	// Metadata change while retained must refuse, never bless it by re-hashing.
	if err := os.Chtimes(model, clock.Now(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	paths, releases, err = w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
	for _, release := range releases {
		release()
	}
	if err == nil || runtimeDiagnostic(err).Predicate != "model_snapshot_mismatch" || len(paths) != 0 {
		t.Fatalf("changed retained view accepted: %v %v", paths, err)
	}
	if err := cache.admit(context.Background(), "another", 10); err == nil {
		t.Fatal("refusal released retained custody")
	}
}

func TestModelSnapshotEmulatedSymlinkJob(t *testing.T) {
	state := t.TempDir()
	if err := PrepareStateRoots(state); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(state, "models")
	repo := tinyModel(t, root)
	for _, name := range []string{"model.safetensors", "config.json"} {
		emulateModelSymlink(t, filepath.Join(repo, "snapshots", tinyRevision, name))
	}
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), ModelRoot: root, GPU: "cpu", Workers: 1, Token: "test", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	t.Cleanup(func() {
		_ = removeSharedEntry(filepath.Join(state, "workers", "fake-1", "file-cache", "model-snapshots"))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := d.Client()
	setModelPolicy(t, ctx, c, &ModelPolicy{Sources: []string{"org/tiny"}, MaxBytes: 1 << 20})
	s := snapshotJob("org/tiny").Spec
	s.Name, s.Project = "emulated-link", "test"
	s.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}
	s.Run.Command = []string{"sh", "-ec", `test "$(cat "$MYCE_MODEL_BASE/model.safetensors")" = 'synthetic weights'; test "$(cat "$MYCE_MODEL_BASE/config.json")" = '{"layers": 1}'; printf real-bytes > result`}
	s.Outputs = map[string]string{"result": "results"}
	j, err := c.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if got := awaitTerminal(t, ctx, c, j.ID); got.State != "succeeded" {
		t.Fatalf("%s: %s", got.State, got.Reason)
	}
	out := filepath.Join(t.TempDir(), "result")
	if err := c.DownloadResult(ctx, j.ID, "result", out); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(out); err != nil || string(data) != "real-bytes" {
		t.Fatalf("result: %q %v", data, err)
	}
	// Staging must leave the shared emulation files as they were.
	data, err := os.ReadFile(filepath.Join(repo, "snapshots", tinyRevision, "model.safetensors"))
	if err != nil || len(data) != 1067 || !strings.HasPrefix(string(data), "XSym\n") {
		t.Fatal("source hub changed", err)
	}
}

func TestModelSnapshotEmulatedSymlinkNestedStub(t *testing.T) {
	root := t.TempDir()
	repo := tinyModel(t, root)
	file := filepath.Join(repo, "snapshots", tinyRevision, "model.safetensors")
	target := emulateModelSymlink(t, file)
	stub, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	// A second content-addressed file is itself an emulated link. Its hash is
	// valid for the stub, so copy verification alone cannot reject these bytes.
	sum := sha256.Sum256(stub)
	nested := filepath.Join(repo, "blobs", fmt.Sprintf("%x", sum))
	if err := os.WriteFile(nested, stub, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(filepath.Dir(target), filepath.Base(nested)), file); err != nil {
		t.Fatal(err)
	}
	emulateModelSymlink(t, file)
	w := snapshotWorker(t, root, &fakeClock{now: time.Unix(1000, 0)})
	paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
	for _, release := range releases {
		release()
	}
	if err == nil || runtimeDiagnostic(err).Predicate != "model_snapshot_mismatch" || len(paths) != 0 {
		t.Fatalf("nested stub exposed: paths=%v error=%v", paths, err)
	}
}
