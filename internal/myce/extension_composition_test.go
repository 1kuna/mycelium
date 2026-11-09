package myce

import (
	"gopkg.in/yaml.v3"
)

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func compositionExtension(t *testing.T, withEnvironment bool) Extension {
	t.Helper()
	e := fakeExtension(t)
	v := ExtensionVariant{Target: ExecutionTarget{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "fake"}, Needs: ResourceNeeds{MemoryGB: e.MemoryGB, HostMemoryGB: e.HostMemoryGB}, Execution: ExecutionSpec{Main: ExecutionMember{Command: e.Run}}, Check: e.Check, Baselines: map[string]ExtensionBaseline{"kind:sample": e.Baselines["kind:sample"], "extension:" + e.Name: e.Baselines["kind:sample"]}}
	e.DefaultClass = "preview"
	e.Platforms = nil
	e.MemoryGB = 0
	e.HostMemoryGB = 0
	e.Run = nil
	e.Check = nil
	e.Baselines = nil
	e.ProgramTypes = nil
	if withEnvironment {
		v.Environments = map[string]EnvironmentRecipe{"main": {Backend: "uv", Python: "3.12", LockFile: "locks/requirements.txt"}}
		e.Files["locks/requirements.txt"] = []byte("# synthetic empty lock\n")
		v.Execution.Main.Environment = "main"
		v.Execution.Main.Command = []string{"{env.main.python}", "{extension}/run.sh"}
		v.Check = []string{"{env.main.python}", "{extension}/check.sh"}
	}
	e.Variants = []ExtensionVariant{v}
	return freezeComposition(t, e)
}
func freezeComposition(t *testing.T, e Extension) Extension {
	t.Helper()
	b, err := yaml.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	e.Files["extension.yaml"] = b
	e, err = extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestCompositionImmutableResolutionReplayAndRestart(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(dir, "queue.db"), dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if q != nil {
			q.Close()
		}
	}()
	e := compositionExtension(t, false)
	if err = q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	e.Files["run.sh"][0] = '!'
	e.Files["extra"] = []byte("caller mutation")
	s := Spec{Name: "wrap", Project: "test", Extension: e.Name, SubmissionKey: "fixed"}
	j := submit(t, q, s)
	if j.Spec.Extension != extensionKey(e) || j.Spec.ExtensionDigest != e.Digest || j.Spec.Class != "preview" {
		t.Fatal("resolution/default missing", j.Spec)
	}
	if j.Extensions[0].Files["run.sh"][0] == '!' || j.Extensions[0].Files["extra"] != nil {
		t.Fatal("caller mutated registered bytes")
	}
	actual, err := extensionFromFiles(j.Extensions[0].Files)
	if err != nil || actual.Digest != e.Digest {
		t.Fatal("snapshot is not immutable", err)
	}
	e = j.Extensions[0]
	newer := e
	newer.Version = "2"
	newer.DefaultClass = "other"
	newer = freezeComposition(t, newer)
	if err = q.AddExtension(newer); err != nil {
		t.Fatal(err)
	}
	replay := submit(t, q, s)
	if replay.ID != j.ID || replay.Spec.ExtensionDigest != e.Digest || replay.Spec.Class != "preview" {
		t.Fatal("replay re-resolved")
	}
	s.SubmissionKey = ""
	if _, err = q.Submit(s); err == nil {
		t.Fatal("ambiguous bare name accepted")
	}
	s.Extension = ""
	s.ExtensionDigest = e.Digest
	pinned := submit(t, q, s)
	if pinned.Spec.Extension != extensionKey(e) {
		t.Fatal("digest not resolved")
	}
	s.Extension = extensionKey(newer)
	if _, err = q.Submit(s); err == nil {
		t.Fatal("mismatched name/digest accepted")
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q = nil
	q, err = Open(filepath.Join(dir, "queue.db"), dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	replay = submit(t, q, Spec{Name: "wrap", Project: "test", Extension: e.Name, SubmissionKey: "fixed"})
	if replay.ID != j.ID || replay.Extensions[0].Digest != e.Digest || replay.Spec.ExtensionDigest != e.Digest {
		t.Fatal("restart lost frozen resolution")
	}

	{
		for _, field := range []string{"env", "execution"} {
			bad := Spec{Name: "x", Project: "test", Extension: extensionKey(e)}
			if field == "env" {
				bad.Run.Env = map[string]string{"X": "override"}
			} else {
				bad.Execution = &ExecutionSpec{Main: ExecutionMember{Command: []string{"main"}}}
			}
			if _, err := q.Submit(bad); err == nil {
				t.Fatal("caller override accepted", field)
			}
		}
		bad := spec("experiment")
		bad.Run.Env = map[string]string{"MYCE_TOKEN": "bad"}
		if _, err := q.Submit(bad); err == nil {
			t.Fatal("token override accepted")
		}
	}

	{
		// Isolate version qualification from unrelated queued work.
		q, _ := fixture(t)
		a := compositionExtension(t, true)
		b := compositionExtension(t, true)
		b.Version = "2-debug"
		b.Files["run.sh"] = []byte("#!/bin/sh\necho debug\n")
		b = freezeComposition(t, b)
		if err := q.AddExtension(a); err != nil {
			t.Fatal(err)
		}
		if err := q.AddExtension(b); err != nil {
			t.Fatal(err)
		}
		m := machine("target", "fake")
		m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
		m.Environments = []string{b.Variants[0].Environments["main"].Key}
		m.ExtensionFingerprint = "test-fingerprint"
		qualified := newExtensionReport(b, &m, "extension:"+b.Name)
		qualified.Setup, qualified.Qualified, qualified.CheckID = true, true, "check-b"
		m.ExtensionReports = []ExtensionReport{qualified}
		control := submit(t, q, Spec{Name: "qualification", Project: "test", Extension: extensionKey(b)})
		reply := check(t, q, m, nil)
		if reply.Job == nil || reply.Job.ID != control.ID {
			t.Fatal("qualified version B was not assigned")
		}
		finish(t, q, reply.Job, true)
		report := newExtensionReport(a, &m, "extension:"+a.Name)
		report.Setup, report.Qualified, report.CheckID = true, true, "check-a"
		m.ExtensionReports = []ExtensionReport{report}
		j := submit(t, q, Spec{Name: "qualification", Project: "test", Extension: extensionKey(b)})
		if reply := check(t, q, m, nil); reply.Job != nil && reply.Job.ID == j.ID {
			t.Fatal("shared prefix inherited another version's qualification")
		}
	}
}

func TestCompositionEnvironmentWorkerEndToEnd(t *testing.T) {
	e := compositionExtension(t, true)
	e.Files["run.sh"] = append([]byte(`test "$(command -v python)" = "$VIRTUAL_ENV/bin/python" || exit 96
# The helper's HTTP listener may become ready before its child records its PID.
while ! test -s child.pid; do sleep 0.01; done
`), e.Files["run.sh"]...)
	e.Variants[0].Execution.Main.Command[0] = "python"
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	e.Variants[0].Execution.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{executable, "-test.run=^TestNativeCPUHelper$"}, Environment: "main", Port: &zero, Readiness: &NativeReadiness{Path: "/ready", Timeout: "5s"}, MemoryGB: .05, Env: map[string]string{"NATIVE_TEST_MODE": "side", "NATIVE_TEST_DIR": "{work}", "GORACE": "atexit_sleep_ms=0", "HELPER_ROOT": "{env.main.root}"}}}
	e = freezeComposition(t, e)
	root := t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Run: func(_ context.Context, args []string, dir string, env []string, _, _ io.Writer) error {
		for _, v := range env {
			if strings.HasPrefix(v, "MYCE_TOKEN=") {
				t.Error("builder inherited token")
			}
		}
		bin := filepath.Join(dir, "venv", "bin")
		if err := os.MkdirAll(bin, 0700); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(bin, "python"), []byte("#!/bin/sh\nexec /bin/sh \"$@\"\n"), 0700)
	}}
	t.Setenv("MYCE_TOKEN", "must-not-inherit")
	for _, check := range []bool{false, true} {
		var log bytes.Buffer
		if _, err := PrepareExtension(context.Background(), root, e, "fake", "0", check, &log, cache); err != nil {
			t.Fatal(err, log.String())
		}
	}
	// A legacy extension-cache executable must not shadow the declared environment.
	shadow := filepath.Join(extensionCache(root, e, "fake"), "venv", "bin")
	if err := os.MkdirAll(shadow, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shadow, "python"), []byte("#!/bin/sh\nexit 97\n"), 0700); err != nil {
		t.Fatal(err)
	}
	q, _ := fixture(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	shared := t.TempDir()
	if err := os.WriteFile(filepath.Join(shared, "input"), []byte("existing-project-result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, Spec{Name: "existing", Project: "test", Extension: extensionKey(e), Inputs: map[string]string{"input": "input"}, Outputs: map[string]string{"output": "results"}})
	server := testServerForEnvironments(t, q)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	m := machine("composition-worker", "fake")
	m.Device = "0"
	m.CapabilityProfile.OS = runtime.GOOS
	m.CapabilityProfile.Arch = runtime.GOARCH
	w := Worker{StateDir: newTestStateRoot(t), ExtensionRoot: root, JobFileRoot: shared, EnvironmentRoot: cache.Root, EnvironmentLimit: 1 << 20, Client: Client{URL: server, Token: "test"}, Machine: m, Interval: 10 * time.Millisecond}
	// Retain exact helper evidence before the real completion API can acknowledge
	// publication and allow immediate work reclamation. No payload grant changes.
	nativeDir := t.TempDir()
	work := filepath.Join(w.StateDir, "job-files", j.ID, "1")
	evidence := make(chan error, 1)
	var evidenceOnce sync.Once
	w.Client.HTTP = &http.Client{Timeout: 10 * time.Second, Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/workers/complete" {
			evidenceOnce.Do(func() {
				evidence <- func() error {
					for _, name := range []string{"side.json", "child.pid"} {
						data, err := os.ReadFile(filepath.Join(work, name))
						if err != nil {
							return fmt.Errorf("required helper evidence %s: %w", name, err)
						}
						if err := os.WriteFile(filepath.Join(nativeDir, name), data, 0600); err != nil {
							return err
						}
					}
					return nil
				}()
			})
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	await(t, func() bool {
		snap, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, out := range snap.Jobs {
			if out.ID == j.ID && terminal(out.State) {
				if out.State != "succeeded" {
					t.Fatalf("%s: %s", out.State, out.Reason)
				}
				j = out
				return true
			}
		}
		return false
	})
	if err := <-evidence; err != nil {
		t.Fatal(err)
	}
	if j.Extension == nil || j.Extension.Digest != e.Digest {
		t.Fatal("attempt did not retain digest")
	}
	data, err := os.ReadFile(filepath.Join(shared, j.OutputFiles["output"]))
	if err != nil || string(data) != "existing-project-result\n" {
		t.Fatal("existing program output changed", string(data), err)
	}
	helperEnv := nativeEnv(t, nativeDir, "side")
	if helperEnv["HELPER_ROOT"] != helperEnv["MYCE_ENVIRONMENT_DIR"] || helperEnv["MYCE_TOKEN"] != "" {
		t.Fatal("extension helper environment mismatch")
	}
	// Require real positive PID records so nativeNoSurvivors cannot silently skip
	// missing or malformed helper/grandchild evidence after completion.
	var side struct{ PID int }
	data, err = os.ReadFile(filepath.Join(nativeDir, "side.json"))
	if err != nil || json.Unmarshal(data, &side) != nil || side.PID <= 0 {
		t.Fatal("invalid required sidecar evidence", string(data), err)
	}
	data, err = os.ReadFile(filepath.Join(nativeDir, "child.pid"))
	var childPID int
	if err != nil || json.Unmarshal(data, &childPID) != nil || childPID <= 0 {
		t.Fatal("invalid required child PID", string(data), err)
	}
	attempts, err := filepath.Glob(filepath.Join(w.StateDir, "attempt-*.json"))
	if err != nil || len(attempts) != 1 {
		t.Fatal("missing group custody", attempts, err)
	}
	record, err := os.ReadFile(attempts[0])
	if err != nil {
		t.Fatal(err)
	}
	var state workerState
	if err := json.Unmarshal(record, &state); err != nil {
		t.Fatal(err)
	}
	nativeNoSurvivors(t, &nativeTestRun{p: &process{cmd: &exec.Cmd{Process: &os.Process{Pid: state.PID}}}, dir: nativeDir})

	t.Run("missing-preparation-never-builds", func(t *testing.T) {
		snapshot, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		missing := recoveryWorker(t)
		for _, observed := range snapshot.Machines {
			if observed.ID == m.ID {
				missing.Machine = *observed
			}
		}
		if missing.Machine.ID != m.ID || len(missing.Machine.ExtensionReports) == 0 {
			t.Fatal("missing successful worker qualification control")
		}
		missing.ExtensionRoot, missing.JobFileRoot = root, shared
		missing.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
		missing.extensionFingerprint = missing.Machine.ExtensionFingerprint
		c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: missing.Clock, Run: func(context.Context, []string, string, []string, io.Writer, io.Writer) error {
			t.Error("execution rebuilt an unprepared environment")
			return fmt.Errorf("execution-side environment build forbidden")
		}}
		missing.environmentCache = c
		store, err := missing.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer store.close()
		store.state.Session = missing.Machine.Session
		// Snapshot records use blob references; deliver the same frozen bytes
		// that the successful worker received before exercising its start path.
		refused := cloneJob(j)
		if err := q.files.hydrateJob(&refused); err != nil {
			t.Fatal(err)
		}
		attempt, err := newAttemptStore(store, refused)
		if err != nil {
			t.Fatal(err)
		}
		p, err := startAfterFixtureInputs(t, missing, attempt, refused)
		if p != nil {
			defer p.stop(missing.Clock.Now(), 0, true)
		}
		if err == nil {
			t.Fatal("execution rebuilt an unprepared environment")
		}
		if !strings.Contains(err.Error(), "environment preparation required") {
			t.Fatal("execution refused for unrelated reason", err)
		}
		r := e.Variants[0].Environments["main"]
		if c.ready(r.Key) {
			t.Fatal("execution published readiness")
		}
	})

	t.Run("standalone-recipe-survives-expired-input-sweep", func(t *testing.T) {

		for _, kind := range []string{"inline", "execution-sidecar"} {
			t.Run(kind, func(t *testing.T) {
				q, clock := fixture(t)
				// Inspect the expired input after sweeping, before history retirement.
				if err := q.ConfigureLogRetention(2 * testFileRetention); err != nil {
					t.Fatal(err)
				}
				lock := filepath.Join(t.TempDir(), "requirements.txt")
				want := []byte("example==1.0\n")
				if err := os.WriteFile(lock, want, 0600); err != nil {
					t.Fatal(err)
				}
				r, err := snapshotEnvironment(EnvironmentRecipe{LockFile: lock, Python: "3.12"}, "fake")
				if err != nil {
					t.Fatal(err)
				}
				if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"fake": r}}}); err != nil {
					t.Fatal(err)
				}
				s := spec("experiment")
				s.SubmissionKey = "recipe"
				s.Environment = Environment{Name: "tools"}
				switch kind {
				case "inline":
					s.Environment = Environment{LockFile: lock, Python: "3.12", Platforms: []string{"fake"}}
				case "execution-sidecar":
					s.Environment = Environment{}
					s.Run = RunSpec{}
					s.Execution = &ExecutionSpec{Main: ExecutionMember{Command: []string{"true"}, Environment: "tools"}, Sidecars: []NativeSidecar{{Name: "helper", Command: []string{"true"}, Environment: "tools"}}}
				}
				j := submit(t, q, s)
				ref := q.s.Jobs[j.ID].EnvironmentRecipes["fake"].FilesRef
				if ref == nil {
					t.Fatal("submitted recipe has no blob reference")
				}

				// The catalog and delivered answer still own their original bytes.
				if !reflect.DeepEqual(j.EnvironmentRecipes["fake"].Files, r.Files) || !reflect.DeepEqual(q.environments.Environments["tools"]["fake"].Files, r.Files) {
					t.Fatal("freezing mutated catalog or response bytes")
				}
				m := machine("builder", "fake")
				m.EnvironmentBuilder = true
				reply := check(t, q, m, nil)
				if len(reply.EnvironmentPlan) != 1 || !reflect.DeepEqual(reply.EnvironmentPlan[0].Files, r.Files) {
					t.Fatal("worker preparation lost recipe bytes", reply.EnvironmentPlan)
				}
				m.Environments = []string{r.Key}
				reply = check(t, q, m, nil)
				if reply.Job == nil || !reflect.DeepEqual(reply.Job.EnvironmentRecipes["fake"].Files, r.Files) {
					t.Fatal("worker assignment lost recipe bytes")
				}
				if kind == "execution-sidecar" && !reflect.DeepEqual(reply.Job.SidecarEnvironmentRecipes["helper"]["fake"].Files, r.Files) {
					t.Fatal("worker sidecar lost recipe bytes")
				}
				// The same bytes are eligible for reclamation as an expired job's
				// input, but the submitted recipe retains independent custody.
				if err := q.update(func() error {
					finished := clock.Now()
					q.s.Jobs["expired-input"] = &Job{ID: "expired-input", State: "succeeded", FinishedAt: &finished, Spec: Spec{Files: FileManifest{"old-input": *ref}}}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				clock.Advance(testFileRetention + time.Nanosecond)
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if err := q.files.verify(context.Background(), *ref); err != nil {
					t.Fatal("sweep collected referenced recipe", err)
				}
				if !q.s.Jobs["expired-input"].FilesExpired {
					t.Fatal("sweep did not expire the old input")
				}
			})
		}

	})
}

// Versions and debug scripts must not multiply identical dependency prefixes.
