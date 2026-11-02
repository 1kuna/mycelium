package myce

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	tinyRevision  = "0123456789abcdef0123456789abcdef01234567"
	tinyWeights   = "synthetic weights"
	tinyConfig    = `{"layers": 1}`
	otherRevision = "fedcba9876543210fedcba9876543210fedcba98"
)

// hubRepo writes a small Hugging Face hub-cache repository: snapshot files are
// relative links into blobs/, named by SHA-256 (LFS) or Git blob SHA-1.
func hubRepo(t *testing.T, modelRoot, repo, revision string, files map[string]string) string {
	t.Helper()
	dir := filepath.Join(modelRoot, "hub", "models--"+strings.ReplaceAll(repo, "/", "--"))
	snapshot := filepath.Join(dir, "snapshots", revision)
	for _, path := range []string{filepath.Join(dir, "blobs"), snapshot, filepath.Join(dir, "refs")} {
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatal(err)
		}
	}
	for name, content := range files {
		var blob string
		if strings.HasSuffix(name, ".json") {
			sum := sha1.Sum([]byte(fmt.Sprintf("blob %d\x00%s", len(content), content)))
			blob = hex.EncodeToString(sum[:])
		} else {
			sum := sha256.Sum256([]byte(content))
			blob = hex.EncodeToString(sum[:])
		}
		if err := os.WriteFile(filepath.Join(dir, "blobs", blob), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..", "..", "blobs", blob), filepath.Join(snapshot, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "refs", "main"), []byte(revision), 0644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func tinyModel(t *testing.T, modelRoot string) string {
	return hubRepo(t, modelRoot, "org/tiny", tinyRevision, map[string]string{"model.safetensors": tinyWeights, "config.json": tinyConfig})
}

func setModelPolicy(t *testing.T, ctx context.Context, c Client, models *ModelPolicy) {
	t.Helper()
	var record PriorityPolicyRecord
	if err := c.Call(ctx, "GET", "/api/policy", nil, &record); err != nil {
		t.Fatal(err)
	}
	record.Policy.Models = models
	request := struct {
		ExpectedRevision uint64         `json:"expected_revision"`
		Policy           PriorityPolicy `json:"policy"`
	}{record.Revision, record.Policy}
	if err := c.Call(ctx, "PUT", "/api/policy", request, &record); err != nil {
		t.Fatal(err)
	}
}

func awaitTerminal(t *testing.T, ctx context.Context, c Client, id string) Job {
	t.Helper()
	for {
		j, err := c.GetJob(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if terminal(j.State) {
			return *j
		}
		select {
		case <-ctx.Done():
			t.Fatal("job did not finish", id, j.State, j.Reason)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestModelSnapshotsNativeDevJourney(t *testing.T) {
	state := t.TempDir()
	if err := PrepareStateRoots(state); err != nil {
		t.Fatal(err)
	}
	modelRoot := filepath.Join(state, "models")
	repo := tinyModel(t, modelRoot)
	// A second revision of the same repository exists; the declared one does not.
	hubRepo(t, modelRoot, "org/moved", otherRevision, map[string]string{"model.safetensors": tinyWeights})
	outside := filepath.Join(modelRoot, "private.txt")
	if err := os.WriteFile(outside, []byte("not a model blob"), 0600); err != nil {
		t.Fatal(err)
	}
	escape := hubRepo(t, modelRoot, "org/escape", tinyRevision, map[string]string{"config.json": tinyConfig})
	if err := os.Symlink(outside, filepath.Join(escape, "snapshots", tinyRevision, "extra.txt")); err != nil {
		t.Fatal(err)
	}
	changed := hubRepo(t, modelRoot, "org/changed", tinyRevision, map[string]string{"model.safetensors": tinyWeights})
	blob, err := filepath.EvalSymlinks(filepath.Join(changed, "snapshots", tinyRevision, "model.safetensors"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blob, []byte("tampered weights!"), 0644); err != nil {
		t.Fatal(err)
	}
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), ModelRoot: modelRoot, GPU: "cpu", Workers: 1, Token: "test", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	t.Cleanup(func() {
		_ = removeSharedEntry(filepath.Join(state, "workers", "fake-1", "file-cache", "shared-inputs"))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := d.Client()
	declare := func(repo, revision, command string, files ...string) Spec {
		return Spec{Name: "model", Project: "test", Run: RunSpec{Command: []string{"sh", "-ec", command}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}, ModelSnapshots: map[string]ModelSnapshot{"base": {Repo: repo, Revision: revision, Files: files}}, Outputs: map[string]string{"result": "results"}}
	}
	reads := `test "$(cat "$MYCE_MODEL_BASE/model.safetensors")" = "` + tinyWeights + `"
test "$(cat "$MYCE_MODEL_BASE/config.json")" = '` + tinyConfig + `'
if (printf changed > "$MYCE_MODEL_BASE/model.safetensors") 2>/dev/null; then exit 21; fi
if touch "$MYCE_MODEL_BASE/new" 2>/dev/null; then exit 22; fi
printf '%s' "$MYCE_MODEL_BASE" > result`

	if _, err := c.Submit(ctx, declare("org/tiny", tinyRevision, reads)); err == nil || !strings.Contains(err.Error(), "not permitted by local model policy") {
		t.Fatal("model accepted without local policy", err)
	}
	setModelPolicy(t, ctx, c, &ModelPolicy{Sources: []string{"org/*"}, MaxBytes: 1 << 20})
	if _, err := c.Submit(ctx, declare("org/tiny", "main", reads)); err == nil || !strings.Contains(err.Error(), "full 40-character lowercase commit") {
		t.Fatal("movable revision accepted", err)
	}
	if _, err := c.Submit(ctx, declare("elsewhere/tiny", tinyRevision, reads)); err == nil || !strings.Contains(err.Error(), "not permitted") {
		t.Fatal("unlisted source accepted", err)
	}

	t.Run("read-only snapshot", func(t *testing.T) {
		j, err := c.Submit(ctx, declare("org/tiny", tinyRevision, reads))
		if err != nil {
			t.Fatal(err)
		}
		if got := awaitTerminal(t, ctx, c, j.ID); got.State != "succeeded" {
			t.Fatal(got.State, got.Reason)
		}
		out := filepath.Join(t.TempDir(), "result")
		if err := c.DownloadResult(ctx, j.ID, "result", out); err != nil {
			t.Fatal(err)
		}
		path, _ := os.ReadFile(out)
		if strings.HasPrefix(string(path), repo) {
			t.Fatal("job received the shared hub instead of a verified copy", string(path))
		}
		if data, _ := os.ReadFile(filepath.Join(repo, "snapshots", tinyRevision, "model.safetensors")); string(data) != tinyWeights {
			t.Fatal("shared hub changed", string(data))
		}
	})
	t.Run("allow-listed files only", func(t *testing.T) {
		j, err := c.Submit(ctx, declare("org/tiny", tinyRevision, `test -f "$MYCE_MODEL_BASE/config.json"; test ! -e "$MYCE_MODEL_BASE/model.safetensors"; : > result`, "config.json"))
		if err != nil {
			t.Fatal(err)
		}
		if got := awaitTerminal(t, ctx, c, j.ID); got.State != "succeeded" {
			t.Fatal(got.State, got.Reason)
		}
	})
	for _, refused := range []struct{ name, repo, revision, want string }{
		{"wrong revision", "org/moved", tinyRevision, "has this repository but not the declared revision"},
		{"missing repository", "org/absent", tinyRevision, "acquire it with a CPU download job"},
		{"symlink escape", "org/escape", tinyRevision, "links outside its repository blob store"},
		{"changed blob", "org/changed", tinyRevision, "failed size or hash verification"},
	} {
		t.Run(refused.name, func(t *testing.T) {
			j, err := c.Submit(ctx, declare(refused.repo, refused.revision, `: > launched; : > result`))
			if err != nil {
				t.Fatal(err)
			}
			got := awaitTerminal(t, ctx, c, j.ID)
			if got.State != "failed" || !strings.Contains(got.Reason, refused.want) || strings.Contains(got.Reason, modelRoot) || got.StartedAt != nil {
				t.Fatalf("state=%s reason=%q started=%v", got.State, got.Reason, got.StartedAt)
			}
		})
	}
	if data, _ := os.ReadFile(outside); string(data) != "not a model blob" {
		t.Fatal("escape target changed")
	}
}

func TestModelSnapshotsDockerMountMatchesNativePath(t *testing.T) {
	w, s, _, root := dockerFixtureCommand(t, func(root string) func(context.Context, ...string) ([]byte, error) {
		daemon := fakeDockerGroupCommand(root)
		return func(ctx context.Context, args ...string) ([]byte, error) {
			switch args[0] {
			case "image":
				return []byte("null"), nil // The image declares no VOLUME mounts.
			case "create":
				// Record the daemon request, then refuse: no container ever runs.
				f, err := os.OpenFile(filepath.Join(root, "calls"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					return nil, err
				}
				fmt.Fprintln(f, strings.Join(args, " "))
				f.Close()
				return nil, &exec.ExitError{Stderr: []byte("Error response from daemon: test stops after create")}
			}
			return daemon(ctx, args...)
		}
	})
	modelRoot := t.TempDir()
	tinyModel(t, modelRoot)
	w.ModelRoot = modelRoot
	w.sharedInputs = &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: w.Clock}}
	t.Cleanup(func() { _ = removeSharedEntry(w.sharedInputs.cache.Root) })
	w.Machine.Capabilities = append(w.Machine.Capabilities, dockerFilesCapability)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoints")
	j.Spec.ModelSnapshots = map[string]ModelSnapshot{"base": {Repo: "org/tiny", Revision: tinyRevision}}
	j.ModelLimits = &ModelLimits{MaxBytes: 1 << 20}
	if err := j.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	// The native environment value is the verified copy the Docker mount must expose.
	native, release, err := w.acquireSharedInputs(context.Background(), j)
	if err != nil {
		t.Fatal(err)
	}
	release()
	source := native["MYCE_MODEL_BASE"]
	for name, want := range map[string]string{"model.safetensors": tinyWeights, "config.json": tinyConfig} {
		info, err := os.Lstat(filepath.Join(source, name))
		data, _ := os.ReadFile(filepath.Join(source, name))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0222 != 0 || string(data) != want {
			t.Fatalf("verified copy %s: %v %v %q", name, err, info, data)
		}
	}
	p, err := startDockerFixture(w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatal("refused create did not finish")
	}
	calls, _ := os.ReadFile(filepath.Join(root, "calls"))
	mount := "--mount type=bind,src=" + source + ",dst=/myce/models/base,readonly"
	if !strings.Contains(string(calls), mount) || !strings.Contains(string(calls), "--env MYCE_MODEL_BASE=/myce/models/base") {
		t.Fatalf("Docker create lacks the read-only model mount %q:\n%s", mount, calls)
	}
	if strings.Contains(string(calls), modelRoot) {
		t.Fatal("Docker received the shared hub path", string(calls))
	}

	// Without an admitting policy snapshot the worker refuses before Docker.
	j.ModelLimits = nil
	if _, _, err := w.acquireSharedInputs(context.Background(), j); err == nil || !strings.Contains(runtimeDiagnostic(err).String(), "no admitting local model policy") {
		t.Fatal("policy-less model accepted", err)
	}
}
