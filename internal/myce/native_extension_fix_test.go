package myce

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// #123: an upgraded worker uses its saved NAS for declared files.
func TestNativeExtensionInstalledNASRoot(t *testing.T) {
	c, source := preserveWorkerFixture(t)
	c.Detection.OS = "linux"
	manager := &installMatrixManager{t: t, c: c, states: map[string]installUnitState{}, cached: map[string][]byte{}}
	if err := Install(context.Background(), c, source, "fixture", manager.run); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(c.Service)
	if err != nil {
		t.Fatal(err)
	}
	old := strings.ReplaceAll(string(before), " "+unitQuote("--job-file-root")+" "+unitQuote(c.NAS), "")
	if err := os.WriteFile(c.Service, []byte(old), 0600); err != nil {
		t.Fatal(err)
	}
	saved, err := ReadServiceConfig(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	if err := Install(context.Background(), saved, source, "fixture", manager.run); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(c.Service)
	if err != nil || !strings.Contains(string(after), unitQuote("--job-file-root")+" "+unitQuote(saved.NAS)) {
		t.Fatal("upgrade did not install saved NAS job-file root", err)
	}
	alias := filepath.Join(t.TempDir(), "nas-mount")
	if err := os.Symlink(saved.NAS, alias); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(saved.NAS, "input"), []byte("input"), 0600); err != nil {
		t.Fatal(err)
	}
	w := Worker{JobFileRoot: alias}
	j := Job{ID: "nas-files", Attempt: 1, Spec: Spec{Extension: "fixture@1", Inputs: map[string]string{"input": "input"}, Outputs: map[string]string{"result": "results"}}}
	if err := w.prepareExtensionFiles(&j, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(j.Spec.Run.Workdir, "result"), []byte("output"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := w.publishExtensionFiles(j); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(saved.NAS, outputRelative(j, "result", "results"))); err != nil || string(b) != "output" {
		t.Fatal("NAS output missing", err)
	}
	// Missing configuration must fail rather than silently publish under cwd.
	t.Chdir(t.TempDir())
	if err := (Worker{}).prepareExtensionFiles(&j, t.TempDir()); err == nil {
		t.Fatal("input accepted empty root")
	}
	if err := (Worker{}).publishExtensionFiles(j); err == nil {
		t.Fatal("output accepted empty root")
	}
}

// #121: the implemented main-member cap must not block a Linux uv target.
func TestNativeExtensionMainMemoryCap(t *testing.T) {
	e := compositionExtension(t, true)
	e.Variants[0].Target.OS = "linux"
	e.Variants[0].Execution.Main.MemoryGB = .5
	e.Variants[0].Needs.HostMemoryGB = 1
	e = freezeComposition(t, e)
	q, _ := fixture(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, Spec{Name: "capped", Project: "test", Extension: extensionKey(e)})
	m := machine("cap-worker", "fake")
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "linux", runtime.GOARCH
	m.ExtensionFingerprint = "fingerprint"
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 8
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	r := newExtensionReport(e, &m, "extension:"+e.Name)
	r.Setup, r.Qualified, r.CheckID = true, true, "successful-check"
	m.ExtensionReports = []ExtensionReport{r}
	for _, recipe := range e.Variants[0].Environments {
		m.Environments = append(m.Environments, recipe.Key)
	}
	check(t, q, m, nil)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("qualified capped uv extension did not place")
	}
}

func TestNativeExtensionPixiLazyAcquisition(t *testing.T) {
	builds := fakePixi(t)
	script, err := os.ReadFile(filepath.Join(filepath.Dir(builds), "pixi"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", "/usr/bin:/bin")
	e := compositionExtension(t, false)
	v := &e.Variants[0]
	v.Environments = map[string]EnvironmentRecipe{"main": {Backend: "pixi", LockFile: "env/pixi.lock"}}
	v.Execution.Main.Environment = "main"
	v.Execution.Main.Command = []string{"tool"}
	e.Files["env/pixi.lock"], e.Files["env/pixi.toml"] = []byte("locked"), []byte("[workspace]")
	e = freezeComposition(t, e)
	q, w := prepareTestWorker(t, e)
	w.Machine.Device = "0"
	w.Machine.ExtensionFingerprint, _ = ExtensionFingerprint(context.Background(), "fake", "0")
	w.extensionFingerprint = w.Machine.ExtensionFingerprint
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "pixi", Typeflag: tar.TypeReg, Mode: 0700, Size: int64(len(script))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(script); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(archive.Bytes())
	home, downloads := t.TempDir(), 0
	w.environmentCache.Tools = &EnvironmentToolSource{
		Home: func() (string, error) { return home, nil },
		Releases: func(osname, arch string) ([]ToolRelease, error) {
			r, err := PinnedEnvironmentTools(osname, arch)
			if err == nil {
				r[1].SHA256 = hex.EncodeToString(sum[:])
			}
			return r, err
		},
		Probe: ToolBootstrapProbe{
			Fetch: func(_ context.Context, r ToolRelease) (io.ReadCloser, error) {
				downloads++
				if r.Name != "pixi" {
					t.Fatal("unexpected tool")
				}
				return io.NopCloser(bytes.NewReader(archive.Bytes())), nil
			},
			Command: LocalMachineProbe().Command,
		},
	}
	j := submit(t, q, Spec{Name: "lazy", Project: "test", Extension: extensionKey(e)})
	plan := check(t, q, w.Machine, nil).EnvironmentPlan[0]
	if err := w.prepareRegisteredExtension(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if downloads != 1 {
		t.Fatalf("downloads = %d", downloads)
	}
	startNativeExtensionFixture(t, w)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	reports, err := CheckExtension(ctx, w.ExtensionRoot, e, "fake", w.Machine.Device, w.Machine.HostID, io.Discard, w.environmentCache, w.Client)
	if err != nil || len(reports) == 0 || !reports[0].Qualified {
		t.Fatal("lazy pixi runtime check", err)
	}
	await(t, func() bool {
		out := job(t, q, j.ID)
		if terminal(out.State) && out.State != "succeeded" {
			t.Fatal(out.State, out.Reason)
		}
		return out.State == "succeeded"
	})
	if downloads != 1 {
		t.Fatal("runtime rebuilt or reacquired pixi")
	}
}

func startNativeExtensionFixture(t *testing.T, w Worker) {
	t.Helper()
	w.EnvironmentRoot, w.EnvironmentLimit = w.environmentCache.Root, w.environmentCache.Limit
	w.Interval = 10 * time.Millisecond
	w.environmentRun = func(context.Context, []string, string, []string, io.Writer, io.Writer) error {
		return errors.New("prepared execution unexpectedly rebuilt environment")
	}
	if err := saveWorkerJSON(w.StateDir, "state.json", workerState{Session: w.Machine.Session, Fresh: true}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	await(t, func() bool {
		var snap Snapshot
		if err := w.Client.Call(context.Background(), "GET", "/api/status", nil, &snap); err != nil {
			t.Fatal(err)
		}
		for _, m := range snap.Machines {
			if m.ID == w.Machine.ID && m.MemoryEnforcement != "" {
				return true
			}
		}
		return false
	})
}

// #122: an ordinary sibling import retains qualification for an unpinned job.
func TestNativeExtensionPythonImportQualification(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("local Python required for sibling-import regression")
	}
	e := compositionExtension(t, true)
	e.Kinds = nil
	delete(e.Variants[0].Baselines, "kind:sample")
	v := &e.Variants[0]
	v.Execution.Main.Command = []string{"{env.main.python}", "{extension}/app.py"}
	v.Check = []string{"{env.main.python}", "{extension}/check.py"}
	e.Files["helper.py"] = []byte("value = 3\n")
	e.Files["app.py"] = []byte("import helper\nassert helper.value == 3\n")
	e.Files["check.py"] = []byte("import os, helper\nwith open(os.environ['MYCE_CHECK_OUTPUT'], 'w') as f: f.write('[%d]\\n' % helper.value)\n")
	e.Files["baseline/output"] = []byte("[3]\n")
	e = freezeComposition(t, e)
	q, w := prepareTestWorker(t, e)
	w.Machine.Device = "0"
	w.Machine.ExtensionFingerprint, _ = ExtensionFingerprint(context.Background(), "fake", "0")
	w.extensionFingerprint = w.Machine.ExtensionFingerprint
	// This suite never acquires a real tool or interpreter. The uv build seam
	// creates an isolated prefix pointing at an already installed local Python.
	w.environmentCache.Run = func(_ context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
		bin := filepath.Join(dir, "venv", "bin")
		if err := os.MkdirAll(bin, 0700); err != nil {
			return err
		}
		// Apple's python3 launcher dispatches by argv[0]; keep that name.
		quoted := "'" + strings.ReplaceAll(python, "'", "'\"'\"'") + "'"
		return os.WriteFile(filepath.Join(bin, "python"), []byte("#!/bin/sh\nexec "+quoted+" \"$@\"\n"), 0700)
	}
	j := submit(t, q, Spec{Name: "ordinary-import", Project: "test", Extension: extensionKey(e)})
	plans := check(t, q, w.Machine, nil).EnvironmentPlan
	if len(plans) != 1 {
		t.Fatal("uv extension did not receive preparation")
	}
	if err := w.prepareRegisteredExtension(context.Background(), plans[0]); err != nil {
		t.Fatal(err)
	}
	// Force bytecode into the library if the worker fails to prevent writes,
	// including on Apple's Python, whose default cache is outside the library.
	t.Setenv("PYTHONPYCACHEPREFIX", filepath.Join(extensionFolder(w.ExtensionRoot, e), "__pycache__"))
	t.Setenv("PYTHONDONTWRITEBYTECODE", "")
	startNativeExtensionFixture(t, w)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var checkLog bytes.Buffer
	reports, err := CheckExtension(ctx, w.ExtensionRoot, e, "fake", w.Machine.Device, w.Machine.HostID, &checkLog, w.environmentCache, w.Client)
	if err != nil || len(reports) != 1 || !reports[0].Qualified {
		t.Fatal("uv qualification", err, checkLog.String())
	}
	await(t, func() bool {
		out := job(t, q, j.ID)
		if terminal(out.State) && out.State != "succeeded" {
			t.Fatal(out.State, out.Reason)
		}
		return out.State == "succeeded"
	})
	actual, err := ReadExtension(extensionFolder(w.ExtensionRoot, e))
	if err != nil || actual.Digest != e.Digest {
		t.Fatal("qualification/import changed digest", err)
	}
}
