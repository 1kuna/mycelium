package myce

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func fakeExtension(t *testing.T) Extension {
	t.Helper()
	files := map[string][]byte{
		"extension.yaml": []byte(`name: sample
version: '1'
purpose: Test real commands with fake resources.
platforms: [fake]
kinds: [sample]
program_types: [sample-script]
memory_gb: 1
host_memory_gb: 0.1
resumable: true
run: [sh, '{extension}/run.sh']
check: [sh, check.sh]
baselines:
  kind:sample:
    input: baseline/input
    output: baseline/output
    measure: numeric
    absolute: 0.01
    native: fake native fixture, not hardware qualification
    variation: fixture repeated outputs differ by at most 0.01
  program:sample-script:
    input: baseline/input
    output: baseline/output
    measure: numeric
    absolute: 0.01
    native: fake native fixture, not hardware qualification
    variation: fixture repeated outputs differ by at most 0.01
`),
		"check.sh": []byte(`test -z "$MYCE_TOKEN" || exit 99
cat "$MYCE_CHECK_INPUT" > "$MYCE_CHECK_OUTPUT"
`),
		"run.sh": []byte(`test -z "$MYCE_TOKEN" || exit 99
test "$MYCE_DEVICE" = cpu || exit 98
if test "$MYCE_WORK" = program:sample-script; then exec sh "$@"; fi
cat input > output
`),
		"baseline/input": []byte("[1,2]\n"), "baseline/output": []byte("[1,2]\n")}
	e, err := extensionFromFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func installQualified(t *testing.T, root string, e Extension, device string) []ExtensionReport {
	t.Helper()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareExtension(context.Background(), root, e, "fake", device, false, io.Discard); err != nil {
		t.Fatal(err)
	}
	reports, err := PrepareExtension(context.Background(), root, e, "fake", device, true, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return reports
}
func TestExtensionFormatFrozenAndMissingBaseline(t *testing.T) {
	e := fakeExtension(t)
	root := t.TempDir()
	reports := installQualified(t, root, e, "")
	if len(reports) != 2 || !reports[0].Qualified || !reports[1].Qualified {
		t.Fatal(reports)
	}
	for _, change := range []func(map[string][]byte){
		func(f map[string][]byte) { f["../escape"] = []byte("x") },
		func(f map[string][]byte) {
			f["extension.yaml"] = append(f["extension.yaml"], []byte("unknown: true\n")...)
		},
		func(f map[string][]byte) { delete(f, "baseline/output") },
		func(f map[string][]byte) { f["large"] = make([]byte, extensionMaxBytes) },
	} {
		files := map[string][]byte{}
		for n, b := range e.Files {
			files[n] = append([]byte(nil), b...)
		}
		change(files)
		if _, err := extensionFromFiles(files); err == nil {
			t.Fatal("accepted invalid folder")
		}
	}
	altered := e
	altered.Files = map[string][]byte{}
	for n, b := range e.Files {
		altered.Files[n] = b
	}
	altered.Files["run.sh"] = []byte("exit 1\n")
	altered, err := extensionFromFiles(altered.Files)
	if err != nil {
		t.Fatal(err)
	}
	if InstallExtension(root, altered) == nil {
		t.Fatal("changed frozen version")
	}
	q, _ := fixture(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	if q.AddExtension(altered) == nil {
		t.Fatal("queue accepted changed version")
	}
	e.Baselines = nil
	e.Files["extension.yaml"], err = yaml.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	// yaml excludes transported files/digest. A missing baseline is valid but can never qualify.
	e, err = extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	root = t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareExtension(context.Background(), root, e, "fake", "", false, io.Discard); err != nil {
		t.Fatal(err)
	}
	reports, err = PrepareExtension(context.Background(), root, e, "fake", "", true, io.Discard)
	if err == nil || reports[0].Qualified || reports[0].Reason != "no native baseline" {
		t.Fatal(reports, err)
	}
}
func TestExtensionQualificationFailureAndIdentity(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "must-not-inherit")
	e := fakeExtension(t)
	root := t.TempDir()
	reports := installQualified(t, root, e, "")
	fp, err := ExtensionFingerprint(context.Background(), "fake", "")
	if err != nil {
		t.Fatal(err)
	}
	if !LocalExtensionReports(root, "fake", fp)[0].Qualified {
		t.Fatal("qualification lost")
	}
	if LocalExtensionReports(root, "fake", "changed")[0].Qualified {
		t.Fatal("hardware change did not invalidate")
	}
	if _, err := PrepareExtension(context.Background(), root, e, "cuda", "", true, io.Discard); err == nil {
		t.Fatal("CPU/platform fallback")
	}
	// A different native reference exceeds the declared variation; failure clears qualification.
	e.Files["baseline/output"] = []byte("[1,20]\n")
	e.Version = "2"
	e.Files["extension.yaml"], _ = yaml.Marshal(e)
	e, err = extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareExtension(context.Background(), root, e, "fake", "", false, io.Discard); err != nil {
		t.Fatal(err)
	}
	reports, err = PrepareExtension(context.Background(), root, e, "fake", "", true, io.Discard)
	if err == nil || reports[0].Qualified || !strings.Contains(reports[0].Reason, "variation") {
		t.Fatal(reports, err)
	}
	if compareExtensionOutput([]byte("[1,2.005]"), []byte("[1,2]"), e.Baselines["kind:sample"]) != nil {
		t.Fatal("measured allowance rejected")
	}
	if compareExtensionOutput([]byte("[1]"), []byte("[1,2]"), e.Baselines["kind:sample"]) == nil {
		t.Fatal("shape mismatch accepted")
	}
}
func TestExtensionRoutingKindProgramAndWaiting(t *testing.T) {
	for _, program := range []bool{false, true} {
		t.Run(map[bool]string{false: "kind", true: "program"}[program], func(t *testing.T) {
			q, _ := fixture(t)
			e := fakeExtension(t)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			s := Spec{Name: "sample", Project: "test", Priority: "experiment", Kind: "sample"}
			if program {
				s.Kind = ""
				s.Program = &Program{Type: "sample-script", File: "script.sh"}
				s.Inputs = map[string]string{"script.sh": "scripts/example.sh"}
			}
			j := submit(t, q, s)
			m := machine("test", "fake")
			m.HostMemoryMeasured = true
			m.FreeHostMemoryGB = 8
			m.HostAvailableMemoryGB = 8
			if check(t, q, m, nil).Job != nil {
				t.Fatal("unqualified assignment")
			}
			snap, err := q.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(snap.Jobs[0].Reason, "qualified") {
				t.Fatal(snap.Jobs[0].Reason)
			}
			m.ExtensionReports = installQualified(t, t.TempDir(), e, "")
			reply := check(t, q, m, nil)
			if reply.Job == nil || reply.Job.ID != j.ID || reply.Job.Extension.Digest != e.Digest || reply.Job.Spec.Needs.MemoryGB != 1 {
				t.Fatal(reply)
			}
			m.ExtensionReports[0].Digest = "stale"
			if !program && extensionCanRun(reply.Job, &m) {
				t.Fatal("stale content accepted")
			}
		})
	}
	q, _ := fixture(t)
	unknown := submit(t, q, Spec{Name: "unknown", Project: "test", Priority: "experiment", Kind: "missing"})
	if check(t, q, machine("f", "fake"), nil).Job != nil {
		t.Fatal("unknown routed")
	}
	if extensionWaitingReason(unknown, nil) == "" {
		t.Fatal("missing explanation")
	}
}
func TestExtensionJobFilesStayRelativeAndAttemptSpecific(t *testing.T) {
	root := t.TempDir()
	state := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "input"), []byte("hello"), 0600); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "files", Project: "test", Priority: "experiment", Kind: "sample", Inputs: map[string]string{"input": "input"}, Outputs: map[string]string{"output": "results"}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"../bad", "/absolute", "a/../bad", "x\\bad"} {
		s.Inputs["input"] = bad
		if s.Validate() == nil {
			t.Fatal(bad)
		}
	}
	s.Inputs["input"] = "input"
	j := Job{ID: "job", Attempt: 1, Spec: s}
	w := Worker{JobFileRoot: root}
	if err := w.prepareExtensionFiles(&j, state); err != nil {
		t.Fatal(err)
	}
	if err := w.prepareExtensionFiles(&j, state); err != nil {
		t.Fatal("repeat preparation must reuse the attempt snapshot:", err)
	}
	if err := os.WriteFile(filepath.Join(j.Spec.Run.Workdir, "output"), []byte("result"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := w.publishExtensionFiles(j); err != nil {
		t.Fatal(err)
	}
	if err := w.publishExtensionFiles(j); err == nil {
		t.Fatal("output overwritten")
	}
	if _, err := os.Stat(filepath.Join(root, "results", "job", "1", "output")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := confinedPath(root, "escape/file"); err == nil {
		t.Fatal("symlink escape accepted")
	}
}

func TestExtensionDevEndToEnd(t *testing.T) {
	root := t.TempDir()
	shared := t.TempDir()
	e := fakeExtension(t)
	e.Run = []string{"extension-test-runtime"}
	e.Setup = []string{"sh", "setup.sh"}
	e.Files["run.sh"] = append([]byte("#!/bin/sh\n"), e.Files["run.sh"]...)
	e.Files["setup.sh"] = []byte(`mkdir -p "$MYCE_EXTENSION_CACHE/bin"
cp "$MYCE_EXTENSION_DIR/run.sh" "$MYCE_EXTENSION_CACHE/bin/extension-test-runtime"
chmod +x "$MYCE_EXTENSION_CACHE/bin/extension-test-runtime"
`)
	e.Files["extension.yaml"], _ = yaml.Marshal(e)
	e, err := extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	installQualified(t, root, e, "0")
	if err := os.WriteFile(filepath.Join(shared, "input"), []byte("unchanged input\n"), 0600); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(dir, "queue.db"), Checkpoints: filepath.Join(dir, "checkpoints"), Token: "throwaway", Workers: 1, MemoryGB: 8, ExtensionRoot: root, JobFileRoot: shared, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	c := Client{URL: d.URL, Token: "throwaway"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Call(ctx, "POST", "/api/extensions", e, nil); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "extension", Project: "test", Priority: "experiment", Kind: "sample", Inputs: map[string]string{"input": "input"}, Outputs: map[string]string{"output": "results"}}
	var j Job
	if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		var snap Snapshot
		if err := c.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
			t.Fatal(err)
		}
		for _, out := range snap.Jobs {
			if out.ID == j.ID && terminal(out.State) {
				if out.State != "succeeded" {
					t.Fatalf("extension failed: state=%s reason=%s", out.State, out.Reason)
				}
				j = *out
				return true
			}
		}
		return false
	})
	if j.Extension == nil || j.Extension.Version != "1" {
		t.Fatal("version not recorded", j)
	}
	data, err := os.ReadFile(filepath.Join(shared, j.OutputFiles["output"]))
	if err != nil || string(data) != "unchanged input\n" {
		t.Fatal(string(data), err)
	}
	// A program travels as an input and executes unchanged through its qualified extension.
	if err := os.WriteFile(filepath.Join(shared, "script"), []byte("printf program-result > output\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Kind = ""
	s.Program = &Program{Type: "sample-script", File: "script"}
	s.Inputs = map[string]string{"script": "script"}
	if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
		t.Fatal(err)
	}
	id := j.ID
	await(t, func() bool {
		var snap Snapshot
		if err := c.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
			t.Fatal(err)
		}
		for _, out := range snap.Jobs {
			if out.ID == id && terminal(out.State) {
				if out.State != "succeeded" {
					t.Fatalf("program failed: state=%s reason=%s", out.State, out.Reason)
				}
				j = *out
				return true
			}
		}
		return false
	})
	data, err = os.ReadFile(filepath.Join(shared, j.OutputFiles["output"]))
	if err != nil || string(data) != "program-result" {
		t.Fatal(string(data), err)
	}
}

func TestExtensionSetupAndCheckFailuresClearQualification(t *testing.T) {
	e := fakeExtension(t)
	e.Files["check.sh"] = append([]byte("test -z \"$FAIL_CHECK\" || exit 1\n"), e.Files["check.sh"]...)
	e.Setup = []string{"sh", "setup.sh"}
	e.Files["setup.sh"] = []byte("test -z \"$FAIL_SETUP\"\n")
	e.Files["extension.yaml"], _ = yaml.Marshal(e)
	var err error
	e, err = extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	installQualified(t, root, e, "")
	t.Setenv("FAIL_CHECK", "1")
	reports, err := PrepareExtension(context.Background(), root, e, "fake", "", true, io.Discard)
	if err == nil || reports[0].Qualified || reports[0].Reason != "check command failed" {
		t.Fatal(reports, err)
	}
	t.Setenv("FAIL_CHECK", "")
	if _, err := PrepareExtension(context.Background(), root, e, "fake", "", true, io.Discard); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAIL_SETUP", "1")
	reports, err = PrepareExtension(context.Background(), root, e, "fake", "", false, io.Discard)
	if err == nil || reports[0].Setup || reports[0].Qualified || reports[0].Reason != "setup failed" {
		t.Fatal(reports, err)
	}
	fp, err := ExtensionFingerprint(context.Background(), "fake", "")
	if err != nil {
		t.Fatal(err)
	}
	if LocalExtensionReports(root, "fake", fp)[0].Qualified {
		t.Fatal("failed setup retained qualified")
	}
}

func TestExtensionLibrarySurvivesQueueRestart(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "queue.db")
	clock := &fakeClock{now: time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)}
	q, err := Open(db, dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, Spec{Name: "persist", Project: "test", Priority: "experiment", Kind: "sample"})
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if len(q.Extensions()) != 1 || q.Extensions()[0].Digest != e.Digest {
		t.Fatal("library lost")
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Jobs) != 1 || snap.Jobs[0].ID != j.ID || snap.Jobs[0].Extensions[0].Digest != e.Digest {
		t.Fatal("frozen job extension lost")
	}
}

func TestExtensionUnqualifiedIsNotStructuralRejection(t *testing.T) {
	q, _ := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := machine("allowed", "fake")
	m.TotalMemoryGB = 8
	check(t, q, m, nil)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{"allowed"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Submit(Spec{Name: "waiting", Project: "test", Priority: "experiment", Kind: "sample"}); err != nil {
		t.Fatalf("qualification is temporary, not structural incompatibility: %v", err)
	}
}

func TestExtensionQualificationWithRelativeRoot(t *testing.T) {
	t.Chdir(t.TempDir())
	e := fakeExtension(t)
	installQualified(t, "relative-root", e, "")
}

func TestExtensionQualificationChangeBeforeStartRequeuesWithoutLoss(t *testing.T) {
	q, _ := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	reports := installQualified(t, t.TempDir(), e, "")
	a, b := machine("a", "fake"), machine("b", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.ExtensionReports = reports
		m.HostMemoryMeasured = true
		m.FreeHostMemoryGB = 8
		m.HostAvailableMemoryGB = 8
	}
	j := submit(t, q, Spec{Name: "qual-change", Project: "test", Priority: "experiment", Kind: "sample"})
	first := check(t, q, a, nil).Job
	if first == nil {
		t.Fatal("no first assignment")
	}
	result := Result{Machine: a.ID, Session: a.Session, Execution: Execution{ID: j.ID, Attempt: first.Attempt}, ExtensionUnavailable: true}
	if err := q.Complete(result); err != nil {
		t.Fatal(err)
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Jobs[0].State != "queued" || snap.Jobs[0].Losses != 0 {
		t.Fatal("qualification failure charged loss or failed job")
	}
	second := check(t, q, b, nil).Job
	if second != nil {
		for _, copy := range second.Copies {
			if copy.Extension == nil || copy.Extension.Digest != e.Digest {
				t.Fatal("attempt extension identity lost")
			}
		}
	}
	if second == nil || second.Attempt <= first.Attempt {
		t.Fatal("did not try another qualified hypha")
	}
	if err := q.Complete(result); err != ErrStale {
		t.Fatal("stale qualification failure affected current attempt:", err)
	}
}

func TestPlatformBaselinesSelectNativeReferenceWithoutFallback(t *testing.T) {
	e := fakeExtension(t)
	metal := e.Baselines["kind:sample"]
	cuda := metal
	cuda.Output = "baseline/cuda-output"
	e.Files[cuda.Output] = []byte("[9,10]\n")
	e.Platforms = []string{"fake", "cuda", "xpu"}
	e.PlatformBaselines = map[string]map[string]ExtensionBaseline{"fake": {"kind:sample": metal}, "cuda": {"kind:sample": cuda}}
	e.Baselines = nil
	if err := e.validate(); err != nil {
		t.Fatal(err)
	}
	b, ok := e.baseline("kind:sample", "cuda")
	if !ok || b.Output != cuda.Output {
		t.Fatal("CUDA reference not selected")
	}
	if _, ok := e.baseline("kind:sample", "xpu"); ok {
		t.Fatal("fell back to another platform")
	}
	if err := compareExtensionOutput(e.Files[metal.Output], e.Files[b.Output], b); err == nil {
		t.Fatal("accepted different platform output")
	}
	// Reparse a frozen folder, then run the actual check against the selected reference.
	description, err := yaml.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	e.Files["extension.yaml"] = description
	e, err = extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareExtension(context.Background(), root, e, "fake", "", false, io.Discard); err != nil {
		t.Fatal(err)
	}
	reports, err := PrepareExtension(context.Background(), root, e, "fake", "", true, io.Discard)
	if err == nil {
		t.Fatal("missing program reference did not fail check")
	}
	for _, r := range reports {
		if r.Work == "kind:sample" && !r.Qualified {
			t.Fatal(r)
		}
		if r.Work == "program:sample-script" && r.Qualified {
			t.Fatal("unreferenced work qualified")
		}
	}
}

func TestPlatformBaselineValidation(t *testing.T) {
	e := fakeExtension(t)
	baseline := e.Baselines["kind:sample"]
	e.PlatformBaselines = map[string]map[string]ExtensionBaseline{"fake": {"kind:sample": baseline}}
	if err := e.validate(); err == nil {
		t.Fatal("ambiguous fallback allowed")
	}
	e.Baselines = nil
	e.PlatformBaselines["cuda"] = map[string]ExtensionBaseline{"kind:sample": baseline}
	if err := e.validate(); err == nil {
		t.Fatal("unsupported platform reference accepted")
	}
	delete(e.PlatformBaselines, "cuda")
	baseline.Output = "missing"
	e.PlatformBaselines["fake"]["kind:sample"] = baseline
	if err := e.validate(); err == nil {
		t.Fatal("missing native output accepted")
	}
}
