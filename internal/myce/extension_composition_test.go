package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
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
func TestCompositionRoundtripAndSharedLowering(t *testing.T) {
	e := compositionExtension(t, false)
	for _, format := range []string{"json", "yaml"} {
		var out Extension
		if format == "json" {
			b, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(b, &out); err != nil {
				t.Fatal(err)
			}
		} else {
			b, err := yaml.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if err = yaml.Unmarshal(b, &out); err != nil {
				t.Fatal(err)
			}
			out.Files = e.Files
			out.Digest = e.Digest
		}
		if !reflect.DeepEqual(e, out) {
			t.Fatalf("%s roundtrip changed description", format)
		}
	}
	m := machine("target", "fake")
	m.CapabilityProfile = &CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH}
	ordinary := spec("experiment")
	ordinary.Run = RunSpec{}
	execution := e.Variants[0].Execution
	ordinary.Execution = &execution
	extension := Spec{Extension: e.Name + "@1", Kind: "sample"}
	if err := ordinary.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := lowerExecution(&ordinary); err != nil {
		t.Fatal(err)
	}
	if err := e.lower(&extension, &m); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ordinary.Run, extension.Run) {
		t.Fatalf("different execution lowering: %+v / %+v", ordinary.Run, extension.Run)
	}
	legacy := spec("experiment")
	before := legacy.Run
	if err := lowerExecution(&legacy); err != nil || !reflect.DeepEqual(before, legacy.Run) {
		t.Fatal("old run changed", err)
	}
	q, _ := fixture(t)
	j := submit(t, q, ordinary)
	if j.Spec.Execution != nil || !reflect.DeepEqual(j.Spec.Run, ordinary.Run) {
		t.Fatal("ordinary adapter not persisted")
	}
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
	s := Spec{Name: "wrap", Project: "test", Extension: e.Name, SubmissionKey: "fixed"}
	j := submit(t, q, s)
	if j.Spec.Extension != extensionKey(e) || j.Spec.ExtensionDigest != e.Digest || j.Spec.Class != "preview" {
		t.Fatal("resolution/default missing", j.Spec)
	}
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
}
func TestCompositionTargetValidationAndQualification(t *testing.T) {
	e := compositionExtension(t, false)
	target := e.Variants[0].Target
	m := machine("target", "fake")
	m.CapabilityProfile = &CapabilityProfile{OS: target.OS, Arch: target.Arch}
	j := Job{Spec: Spec{Extension: extensionKey(e)}, Extensions: []Extension{e}}
	m.ExtensionReports = []ExtensionReport{{Digest: e.Digest, Platform: "fake", Work: "extension:" + e.Name, Qualified: true, Target: &target}}
	if qualifiedExtension(&j, &m) == nil {
		t.Fatal("matching target not selected")
	}
	m.CapabilityProfile.Arch = "other"
	if qualifiedExtension(&j, &m) != nil {
		t.Fatal("wrong architecture accepted")
	}
	m.CapabilityProfile.Arch = target.Arch
	wrong := target
	wrong.OS = "other"
	m.ExtensionReports[0].Target = &wrong
	if qualifiedExtension(&j, &m) != nil {
		t.Fatal("qualification from another target accepted")
	}
	e.Variants = append(e.Variants, e.Variants[0])
	if err := e.validate(); err == nil {
		t.Fatal("duplicate target accepted")
	}
	e.Variants = e.Variants[:1]
	e.Run = []string{"ignored"}
	if err := e.validate(); err == nil {
		t.Fatal("legacy/variant merge accepted")
	}
	e.Run = nil
	e.Variants[0].Execution.Main.Environment = "missing"
	if err := e.validate(); err == nil {
		t.Fatal("missing environment reference accepted")
	}
}
func TestCompositionEnvironmentSnapshotsAndBackend(t *testing.T) {
	e := compositionExtension(t, true)
	r := e.Variants[0].Environments["main"]
	// YAML authoring roundtrip excludes snapshots; reparsing restores them exactly.
	reparsed := freezeComposition(t, e)
	if !reflect.DeepEqual(r, reparsed.Variants[0].Environments["main"]) {
		t.Fatal("recipe roundtrip changed")
	}
	changed := e
	changed.Files = map[string][]byte{}
	for n, b := range e.Files {
		changed.Files[n] = append([]byte(nil), b...)
	}
	changed.Files["locks/requirements.txt"] = []byte("package==1\n")
	changed = freezeComposition(t, changed)
	if changed.Digest == e.Digest || changed.Variants[0].Environments["main"].Key == r.Key {
		t.Fatal("lock bytes omitted from identity")
	}
	if string(r.Files["requirements.txt"]) != "# synthetic empty lock\n" {
		t.Fatal("snapshot mutated")
	}
	changed = e
	changed.Variants = append([]ExtensionVariant(nil), e.Variants...)
	changed.Variants[0].Target.Arch = "other"
	changed = freezeComposition(t, changed)
	if changed.Variants[0].Environments["main"].Key == r.Key {
		t.Fatal("target omitted from cache identity")
	}
	r.Backend = "pixi"
	e.Variants[0].Environments["main"] = r
	b, _ := yaml.Marshal(e)
	e.Files["extension.yaml"] = b
	if _, err := extensionFromFiles(e.Files); err == nil {
		t.Fatal("pixi silently accepted")
	}
	c := &EnvironmentCache{}
	if _, _, err := c.Acquire(context.Background(), r); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatal("unsupported backend used uv", err)
	}
}
func TestCompositionSidecarSeamAndCallerFences(t *testing.T) {
	zero := 0
	execution := ExecutionSpec{Main: ExecutionMember{Command: []string{"main"}, Env: map[string]string{"HELPER_URL": "{sidecars.helper.url}"}}, Sidecars: []NativeSidecar{{Name: "helper", Command: []string{"server", "{port}"}, MemoryGB: 1, Port: &zero, Readiness: &NativeReadiness{Path: "/health", Timeout: "150s"}}}}
	if err := execution.validate(); err != nil {
		t.Fatal(err)
	}
	for _, marshal := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
		b, err := marshal(execution)
		if err != nil {
			t.Fatal(err)
		}
		var out ExecutionSpec
		if err = yaml.Unmarshal(b, &out); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(execution, out) {
			t.Fatal("group schema lost data")
		}
	}
	s := spec("experiment")
	s.Run = RunSpec{}
	s.Execution = &execution
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "D4") {
		t.Fatal("unimplemented group admitted", err)
	}
	s.Execution = nil
	s.Run = RunSpec{Command: []string{"main"}, Sidecars: execution.Sidecars}
	if s.Validate() == nil {
		t.Fatal("run.sidecars bypass")
	}
	s = Spec{Name: "x", Project: "test", Extension: "sample"}
	for _, field := range []string{"env", "execution", "digest"} {
		bad := s
		switch field {
		case "env":
			bad.Run.Env = map[string]string{"X": "override"}
		case "execution":
			bad.Execution = &ExecutionSpec{Main: execution.Main}
		case "digest":
			bad.ExtensionDigest = "invalid"
		}
		if bad.Validate() == nil {
			t.Fatal("caller override accepted", field)
		}
	}
	execution.Sidecars = nil
	execution.Main.Env = map[string]string{"MYCE_TOKEN": "bad"}
	if execution.validate() == nil {
		t.Fatal("token override accepted")
	}
}

func TestCompositionEnvironmentWorkerEndToEnd(t *testing.T) {
	e := compositionExtension(t, true)
	e.Files["run.sh"] = append([]byte(`test "$(command -v python)" = "$VIRTUAL_ENV/bin/python" || exit 96
`), e.Files["run.sh"]...)
	e.Variants[0].Execution.Main.Command[0] = "python"
	e = freezeComposition(t, e)
	root := t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Run: func(_ context.Context, args []string, dir string, env []string, _ io.Writer) error {
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
	if j.Extension == nil || j.Extension.Digest != e.Digest {
		t.Fatal("attempt did not retain digest")
	}
	data, err := os.ReadFile(filepath.Join(shared, j.OutputFiles["output"]))
	if err != nil || string(data) != "existing-project-result\n" {
		t.Fatal("existing program output changed", string(data), err)
	}
}

func TestCompositionRegistrationOwnsBytes(t *testing.T) {
	q, _ := fixture(t)
	e := compositionExtension(t, false)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	e.Files["run.sh"][0] = '!'
	e.Files["extra"] = []byte("caller mutation")
	j := submit(t, q, Spec{Name: "frozen", Project: "test", ExtensionDigest: e.Digest})
	if j.Extensions[0].Files["run.sh"][0] == '!' || j.Extensions[0].Files["extra"] != nil {
		t.Fatal("caller mutated registered bytes")
	}
	actual, err := extensionFromFiles(j.Extensions[0].Files)
	if err != nil || actual.Digest != e.Digest {
		t.Fatal("snapshot is not immutable", err)
	}
}

func TestCompositionDefaultClassUsesAdmissionValidation(t *testing.T) {
	for _, class := range []string{"deadline", "production", "preview"} {
		t.Run(class, func(t *testing.T) {
			q, clock := fixture(t)
			e := compositionExtension(t, false)
			e.DefaultClass = class
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			s := Spec{Name: "default", Project: "test", Extension: extensionKey(e)}
			if class == "deadline" {
				if _, err := q.Submit(s); err == nil {
					t.Fatal("default class bypassed deadline validation")
				}
				d := clock.Now().Add(time.Hour)
				s.Deadline = &d
			}
			if class == "production" {
				start := clock.Now().Add(time.Minute)
				s.StartBy = &start
			}
			j := submit(t, q, s)
			if j.Spec.Class != class {
				t.Fatal("class default lost")
			}
			s.Run.Env = map[string]string{"X": "override"}
			if _, err := q.Submit(s); err == nil {
				t.Fatal("resolution hid caller override")
			}
		})
	}
}
