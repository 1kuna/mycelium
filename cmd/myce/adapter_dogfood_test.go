package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

const adapterDogfoodToken = "adapter-dogfood-fixture-token"

// One real adapter submission covers CLI -> API -> worker -> retained output.
// Conversion equivalence stays in adapter_convert_test.go; wrapper argv/exit and
// result markers stay in scripts/test-go-test-profile.py.
func TestAdapterDogfoodCommandDev(t *testing.T) {
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	a, err := decodeAdapter(adapterDogfoodRead(t, filepath.Join(repo, ".mycelium/adapter.yaml")))
	if err != nil {
		t.Fatal(err)
	}
	entry := a.Entries["mycelium-go-test"]
	adapterDogfoodTarget(t, entry.Targets, "cpu")
	root := t.TempDir()
	entry.Source, entry.Environment = profileSource{Directory: "source"}, myce.Environment{}
	// A shell payload exercises argument boundaries and the profile environment
	// without transporting/recompiling GOROOT inside a tiny command fixture.
	adapterDogfoodWrite(t, filepath.Join(root, "source", "fixture.sh"), []byte(`#!/bin/sh
set -eu
test "$#" -eq 1
test "$1" = 'dogfood argument with spaces'
test "$GOMAXPROCS" = 2
test -z "${MYCE_TOKEN:-}"
printf 'go test: exit=0\n' > go-test-result.txt
`), 0700)
	entry.Run.Command = []string{"sh", "fixture.sh"}
	entry.Outputs = map[string]string{"go-test-result.txt": "results"}
	adapter := filepath.Join(root, ".mycelium/adapter.yaml")
	adapterDogfoodYAML(t, adapter, projectAdapter{Schema: a.Schema, Project: a.Project, Default: "smoke", Entries: map[string]adapterEntry{"smoke": entry}})
	d, ctx, _ := adapterDogfoodDev(t, "cpu")
	job := adapterDogfoodRun(t, ctx, d, devTestBinary(t), root, "--adapter", adapter, "--entry", "smoke", "--key", "smoke", "--", "dogfood argument with spaces")
	if got := adapterDogfoodResult(t, ctx, d, job.ID, "go-test-result.txt"); string(got) != "go test: exit=0\n" {
		t.Fatalf("adapter result=%q", got)
	}
}

// The actual starter is deliberately unchanged: pinned uv environment, native
// replay of app.py, exact baseline, and the real HTTP sidecar/readiness contract.
// This comparison adds no warm repeat or synthetic replacement for those paths.
func TestAdapterDogfoodHTTPStageDev(t *testing.T) {
	adapterDogfoodTools(t, "uv", "tar")
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	source := filepath.Join(root, "examples/extensions/http-stage")
	files, err := os.ReadDir(filepath.Join(repo, "examples/extensions/http-stage"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if !file.Type().IsRegular() {
			t.Fatalf("unexpected starter source member %s", file.Name())
		}
		adapterDogfoodWrite(t, filepath.Join(source, file.Name()), adapterDogfoodRead(t, filepath.Join(repo, "examples/extensions/http-stage", file.Name())), 0600)
	}
	adapter := filepath.Join(root, "examples/adapters/http-stage.yaml")
	adapterDogfoodWrite(t, adapter, adapterDogfoodRead(t, filepath.Join(repo, "examples/adapters/http-stage.yaml")), 0600)
	e, err := myce.ReadExtension(source)
	if err != nil {
		t.Fatal(err)
	}
	targets := make([]myce.ExecutionTarget, 0, len(e.Variants))
	for _, variant := range e.Variants {
		targets = append(targets, variant.Target)
	}
	adapterDogfoodTarget(t, targets, "fake")
	binary := devTestBinary(t)
	d, ctx, state := adapterDogfoodDev(t, "fake")
	if err := d.Queue.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	registryBefore := adapterDogfoodJSON(t, d.Queue.Extensions())
	input := filepath.Join(source, "input.json")
	ref, err := myce.IdentifyFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Client().UploadJobInput(ctx, input, ref); err != nil {
		t.Fatal(err)
	}
	original, err := d.Client().Submit(ctx, myce.Spec{Name: "http-stage", Project: "adapter-example", SubmissionKey: "original-starter", Extension: e.Name + "@" + e.Version, ExtensionDigest: e.Digest, Files: myce.FileManifest{"input.json": ref}, Outputs: map[string]string{"result.json": "results"}})
	if err != nil {
		t.Fatal(err)
	}
	original = adapterDogfoodWait(t, ctx, d, original.ID)
	want := []byte("{\"value\": 3}\n")
	if got := adapterDogfoodResult(t, ctx, d, original.ID, "result.json"); !bytes.Equal(got, want) {
		t.Fatalf("original HTTP result=%q, want %q", got, want)
	}
	oldCheck := adapterDogfoodNativeCheck(t, d, original)
	// Compare immutable accepted fields, excluding changing status projections.
	frozen := func(j *myce.Job) []byte {
		return adapterDogfoodJSON(t, struct {
			Spec       myce.Spec
			Extensions []myce.Extension
			Outputs    myce.FileManifest
		}{j.Spec, j.Extensions, j.OutputRefs})
	}
	jobBefore, checkBefore := frozen(original), frozen(oldCheck)

	// An explicitly located adapter resolves its source from the invocation
	// root, not examples/adapters or the Go package's cmd/myce working directory.
	converted := adapterDogfoodRun(t, ctx, d, binary, root, "--adapter", adapter, "--entry", "http-stage", "--key", "converted-starter", "--input", "input.json=examples/extensions/http-stage/input.json")
	if got := adapterDogfoodResult(t, ctx, d, converted.ID, "result.json"); !bytes.Equal(got, want) {
		t.Fatalf("converted HTTP result=%q, want %q", got, want)
	}
	if converted.Spec.ExtensionDigest == "" || converted.Spec.ExtensionDigest == e.Digest {
		t.Fatal("converted starter did not acquire a new frozen native digest")
	}
	if converted.Spec.Files["input.json"] != ref {
		t.Fatal("converted starter changed the caller's input identity")
	}
	newCheck := adapterDogfoodNativeCheck(t, d, converted)
	if newCheck.ID == oldCheck.ID {
		t.Fatal("converted digest reused the original native check")
	}
	for id, before := range map[string][]byte{original.ID: jobBefore, oldCheck.ID: checkBefore} {
		after := adapterDogfoodJob(t, d, id)
		if !bytes.Equal(before, frozen(after)) {
			t.Fatalf("conversion changed original accepted snapshot %s", id)
		}
	}
	var retained []myce.Extension
	for _, registered := range d.Queue.Extensions() {
		if registered.Name == e.Name && registered.Version == e.Version {
			retained = append(retained, registered)
		}
	}
	if !bytes.Equal(registryBefore, adapterDogfoodJSON(t, retained)) {
		t.Fatal("conversion changed the original registry snapshot")
	}
	installed, err := myce.ReadExtension(filepath.Join(state, "extensions/library", e.Name, e.Version))
	if err != nil || installed.Digest != e.Digest || !bytes.Equal(adapterDogfoodJSON(t, installed.Files), adapterDogfoodJSON(t, e.Files)) {
		t.Fatalf("original installed bundle changed: %v", err)
	}
}

func adapterDogfoodTools(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		if _, err := exec.LookPath(name); err != nil {
			t.Skipf("installed %s is required for the real dogfood fixture: %v", name, err)
		}
	}
}

func adapterDogfoodTarget(t *testing.T, targets []myce.ExecutionTarget, platform string) {
	t.Helper()
	for _, target := range targets {
		if target.OS == runtime.GOOS && target.Arch == runtime.GOARCH && target.Platform == platform {
			return
		}
	}
	t.Skipf("maintained recipe has no %s/%s/%s target", runtime.GOOS, runtime.GOARCH, platform)
}

func adapterDogfoodRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func adapterDogfoodWrite(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
}

func adapterDogfoodYAML(t *testing.T, path string, value any) {
	t.Helper()
	b, err := yaml.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	adapterDogfoodWrite(t, path, b, 0600)
}

func adapterDogfoodJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func adapterDogfoodDev(t *testing.T, gpu string) (*myce.Dev, context.Context, string) {
	t.Helper()
	state := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), ExtensionRoot: filepath.Join(state, "extensions"), Token: adapterDogfoodToken, GPU: gpu, MemoryGB: 4, Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	t.Cleanup(cancel)
	wait(t, ctx, func() bool { s, err := status(ctx, d.Client()); return err == nil && len(s.Machines) == 1 })
	return d, ctx, state
}

func adapterDogfoodRun(t *testing.T, ctx context.Context, d *myce.Dev, binary, root string, args ...string) *myce.Job {
	t.Helper()
	cmd := exec.CommandContext(ctx, binary, append([]string{"run", "--queue", d.URL}, args...)...)
	cmd.Dir, cmd.Env = root, append(os.Environ(), "MYCE_TOKEN="+adapterDogfoodToken, "MYCE_NOTIFY=")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("dogfood run: %v (context: %v)\n%s\n%s", err, context.Cause(ctx), out, stderr.String())
	}
	var receipt profileReceipt
	if err := json.Unmarshal(out, &receipt); err != nil {
		t.Fatalf("dogfood receipt: %v: %s", err, out)
	}
	j := adapterDogfoodWait(t, ctx, d, receipt.JobID)
	// Let the real receipt observer finish before its temporary state is removed.
	dir := filepath.Join(root, ".mycelium/receipts", profileHash([]string{receipt.Project, receipt.Key}))
	wait(t, ctx, func() bool {
		lock, err := profileLock(filepath.Join(dir, "watch.lock"))
		if err != nil {
			return false
		}
		lock.Close()
		return true
	})
	return j
}

func adapterDogfoodWait(t *testing.T, ctx context.Context, d *myce.Dev, id string) *myce.Job {
	t.Helper()
	j, err := d.Client().Wait(ctx, id)
	if err != nil || j.State != "succeeded" {
		logs, logErr := d.Queue.Logs(id, 0)
		for _, log := range logs {
			t.Logf("dogfood job %s attempt %d log: %s", id, log.Attempt, log.Text)
		}
		if logErr != nil {
			t.Logf("dogfood job %s logs: %v", id, logErr)
		}
		t.Fatalf("dogfood job %+v: %v", j, err)
	}
	// The public terminal reply deliberately omits execution/source identity.
	// Read the retained plan when comparing frozen recipes and qualification.
	return adapterDogfoodJob(t, d, id)
}

func adapterDogfoodJob(t *testing.T, d *myce.Dev, id string) *myce.Job {
	t.Helper()
	snapshot, err := d.Queue.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range snapshot.Jobs {
		if j.ID == id {
			return j
		}
	}
	t.Fatalf("dogfood job %s is absent from the retained queue snapshot", id)
	return nil
}

func adapterDogfoodResult(t *testing.T, ctx context.Context, d *myce.Dev, id, name string) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := d.Client().DownloadResult(ctx, id, name, path); err != nil {
		t.Fatal(err)
	}
	return adapterDogfoodRead(t, path)
}

func adapterDogfoodNativeCheck(t *testing.T, d *myce.Dev, useful *myce.Job) *myce.Job {
	t.Helper()
	snapshot, err := d.Queue.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var check *myce.Job
	count := 0
	for _, j := range snapshot.Jobs {
		if request := j.Spec.ExtensionCheck; request != nil && request.Automatic != nil && request.Automatic.Digest == useful.Spec.ExtensionDigest {
			check, count = j, count+1
		}
	}
	if count != 1 || check.State != "succeeded" || !check.ReleaseConfirmed || check.Machine != useful.Machine {
		t.Fatalf("native checks for %s: count=%d check=%+v", useful.Spec.ExtensionDigest, count, check)
	}
	for _, machine := range snapshot.Machines {
		for _, report := range machine.ExtensionReports {
			if machine.ID == useful.Machine && report.Digest == useful.Spec.ExtensionDigest && report.Work == "extension:http-stage" && report.Qualified && report.CheckID == check.Spec.SubmissionKey {
				return check
			}
		}
	}
	t.Fatal("useful HTTP result lacks its own accepted native qualification")
	return nil
}
