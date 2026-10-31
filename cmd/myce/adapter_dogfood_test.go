package main

import (
	"bufio"
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

// #253 compares the maintained wrappers through the original and converted
// lowerings. Only the fixture source and catalog environment are replaced,
// identically on both paths: installed tools avoid rebuilding the fleet's tools
// catalog. This proves the wrapper/result contract, not the full gate or its
// locked environment; the actual full gate is validated separately on M4.
func TestAdapterDogfoodOwnProfilesDev(t *testing.T) {
	adapterDogfoodTools(t, "bash", "go", "git", "python3", "uv", "grep", "tar", "tee")
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	a, err := decodeAdapter(adapterDogfoodRead(t, filepath.Join(repo, ".mycelium/adapter.yaml")))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range a.Entries {
		adapterDogfoodTarget(t, entry.Targets, "cpu")
	}
	binary := devTestBinary(t)
	runtimeInput := adapterDogfoodRuntime(t)
	d, _, _ := adapterDogfoodDev(t, "cpu")
	for _, tc := range []struct {
		profile, entry, result, want string
		args                         []string
	}{
		{"gate-full", "mycelium-gate-full", "gate-result.txt", "gate full: exit=0\n", nil},
		{"go-test", "mycelium-go-test", "go-test-result.txt", "go test: exit=0\n", []string{"-run=^TestAdapterDogfoodFixture$", "-count=1", ".", "-args", "dogfood argument with spaces"}},
	} {
		t.Run(tc.profile, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			for _, wrapper := range []string{"gate-profile.sh", "go-test-profile.sh"} {
				adapterDogfoodWrite(t, filepath.Join(source, "scripts", wrapper), adapterDogfoodRead(t, filepath.Join(repo, "scripts", wrapper)), 0700)
			}
			adapterDogfoodWrite(t, filepath.Join(source, "go.mod"), []byte("module dogfood.invalid/fixture\n\ngo 1.25.6\n"), 0600)
			// Deliberately validate only the wrapper's argv and environment. Running
			// the repository's real gate here would recursively launch full suites.
			adapterDogfoodWrite(t, filepath.Join(source, "scripts/gate.sh"), []byte(`#!/usr/bin/env bash
set -euo pipefail
test "$#" -eq 1
test "$1" = full
test "$GOMAXPROCS" = 2
test -z "${MYCE_TOKEN:-}"
`), 0700)
			adapterDogfoodWrite(t, filepath.Join(source, "fixture_test.go"), []byte(`package fixture

import (
    "flag"
    "os"
    "testing"
)

func TestAdapterDogfoodFixture(t *testing.T) {
    if os.Getenv("GOMAXPROCS") != "2" || os.Getenv("MYCE_TOKEN") != "" {
        t.Fatal("profile environment changed")
    }
    if args := flag.Args(); len(args) != 1 || args[0] != "dogfood argument with spaces" {
        t.Fatalf("appended argv changed: %q", args)
    }
}
`), 0600)
			var p executionProfile
			if err := yaml.Unmarshal(adapterDogfoodRead(t, filepath.Join(repo, ".mycelium/profiles", tc.profile+".yaml")), &p); err != nil {
				t.Fatal(err)
			}
			p.Source, p.Environment = profileSource{Directory: "source"}, myce.Environment{}
			// Installed tools need a writable attempt cache after the catalog
			// environment is removed; native isolation makes HOME's cache read-only.
			p.Run.Env["GOCACHE"] = "{work}/.go-build"
			if runtimeInput.Local != "" {
				// The outer gate's tools are outside this dev worker's grants.
				p.SharedInputs = map[string]myce.SharedInput{"runtime": runtimeInput}
				// Keep the cached tar read-only; nested Bash invocations reuse this
				// attempt's managed scratch copy after successful extraction.
				adapterDogfoodWrite(t, filepath.Join(source, "fixture-runtime.sh"), []byte(`export PATH="/usr/bin:/bin"
dogfood_runtime="$MYCE_SCRATCH_DIR/runtime"
if [ ! -f "$dogfood_runtime/.ready" ]; then
    mkdir -p "$dogfood_runtime" || exit
    tar -xf "$MYCE_INPUT_RUNTIME" -C "$dogfood_runtime" || exit
    touch "$dogfood_runtime/.ready" || exit
fi
export PATH="$dogfood_runtime/go/bin:$dogfood_runtime/bin:/usr/bin:/bin"
export GOROOT="$dogfood_runtime/go"
export LD_LIBRARY_PATH="$dogfood_runtime/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
`), 0600)
				p.Run.Env["BASH_ENV"] = "{work}/fixture-runtime.sh"
			}
			profile := filepath.Join(root, ".mycelium/profiles", tc.profile+".yaml")
			adapterDogfoodYAML(t, profile, p)
			entry := a.Entries[tc.entry]
			entry.Source, entry.Environment = p.Source, myce.Environment{}
			entry.Run.Env["GOCACHE"] = p.Run.Env["GOCACHE"]
			if runtimeInput.Local != "" {
				entry.SharedInputs = p.SharedInputs
				entry.Run.Env["BASH_ENV"] = p.Run.Env["BASH_ENV"]
			}
			adapter := filepath.Join(root, ".mycelium/adapter.yaml")
			adapterDogfoodYAML(t, adapter, projectAdapter{Schema: a.Schema, Project: a.Project, Default: tc.entry, Entries: map[string]adapterEntry{tc.entry: entry}})

			var original []byte
			for _, converted := range []bool{false, true} {
				// Each execution owns the normal dev budget. Previously the
				// startup context covered all four executions: a measured 35s
				// first profile left only 25s for go-test under load (#417).
				ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
				defer cancel()
				selector := []string{"--profile", profile, "--key", tc.profile + "-original"}
				if converted {
					selector = []string{"--adapter", adapter, "--entry", tc.entry, "--key", tc.profile + "-converted"}
				}
				selector = append(append(selector, "--"), tc.args...)
				job := adapterDogfoodRun(t, ctx, d, binary, root, selector...)
				got := adapterDogfoodResult(t, ctx, d, job.ID, tc.result)
				if string(got) != tc.want {
					t.Fatalf("converted=%v result=%q, want %q", converted, got, tc.want)
				}
				if !converted {
					original = got
				} else if !bytes.Equal(original, got) {
					t.Fatal("conversion changed result bytes")
				}
				if tc.profile == "go-test" {
					// Go's JSON timestamps/elapsed times vary. Check the real test's
					// passing event rather than comparing nondeterministic log bytes.
					log := adapterDogfoodResult(t, ctx, d, job.ID, "go-test-log.jsonl")
					scanner, passes := bufio.NewScanner(bytes.NewReader(log)), 0
					for scanner.Scan() {
						var event struct{ Action, Test string }
						if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
							t.Fatalf("go test JSON: %v: %s", err, scanner.Text())
						}
						if event.Action == "fail" {
							t.Fatalf("failed go test event: %s", scanner.Text())
						}
						if event.Action == "pass" && event.Test == "TestAdapterDogfoodFixture" {
							passes++
						}
					}
					if err := scanner.Err(); err != nil || passes != 1 {
						t.Fatalf("tiny Go test passes=%d: %v", passes, err)
					}
				}
			}
		})
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
