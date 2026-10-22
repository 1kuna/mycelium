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
	"slices"
	"strings"
	"testing"
	"time"
)

func TestNativeExtensionInstalledNASRoot(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		t.Run(osname, func(t *testing.T) {
			c, source := preserveWorkerFixture(t)
			c.Detection.OS = osname
			if osname == "darwin" {
				c.Service = filepath.Join(c.WorkerHome, "Library", "LaunchAgents", "local.mycelium.worker.plist")
			}
			manager := &installMatrixManager{t: t, c: c, states: map[string]installUnitState{}, cached: map[string][]byte{}}
			if err := Install(context.Background(), c, source, "fixture", manager.run); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(c.Service)
			if err != nil {
				t.Fatal(err)
			}
			// Simulate the old Linux unit, whose argv did not include job-file-root.
			if osname == "linux" {
				old := strings.ReplaceAll(string(before), " "+unitQuote("--job-file-root")+" "+unitQuote(c.NAS), "")
				if err := os.WriteFile(c.Service, []byte(old), 0600); err != nil {
					t.Fatal(err)
				}
			}
			saved, err := ReadServiceConfig(c.Config)
			if err != nil {
				t.Fatal(err)
			}
			if err = Install(context.Background(), saved, source, "fixture", manager.run); err != nil {
				t.Fatal(err)
			}
			saved, err = ReadServiceConfig(c.Config)
			if err != nil {
				t.Fatal(err)
			}
			args := saved.Args()
			i := slices.Index(args, "--job-file-root")
			if i < 0 || i+1 >= len(args) || args[i+1] != saved.NAS {
				t.Fatal("saved NAS root missing from worker argv")
			}
			after, err := os.ReadFile(c.Service)
			if err != nil {
				t.Fatal(err)
			}
			if osname == "linux" && !strings.Contains(string(after), unitQuote("--job-file-root")+" "+unitQuote(c.NAS)) {
				t.Fatal("upgrade did not rewrite Linux unit")
			}
			if osname == "darwin" && !bytes.Equal(before, after) {
				t.Fatal("Mac config update changed launcher identity")
			}
		})
	}
}

func TestNativeExtensionFilesRejectEmptyRoot(t *testing.T) {
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "result"), []byte("result"), 0600); err != nil {
		t.Fatal(err)
	}
	// Confine any pre-fix accidental publication to owned scratch.
	t.Chdir(t.TempDir())
	j := Job{ID: "empty-root", Attempt: 1, Spec: Spec{Extension: "fixture@1", Inputs: map[string]string{"input": "input"}, Outputs: map[string]string{"result": "results"}, Run: RunSpec{Workdir: work}}}
	if err := (Worker{}).prepareExtensionFiles(&j, t.TempDir()); err == nil {
		t.Fatal("input accepted empty root")
	}
	if err := (Worker{}).publishExtensionFiles(j); err == nil {
		t.Fatal("output accepted empty root and fell back to cwd")
	}
	if _, err := os.Stat("results"); !os.IsNotExist(err) {
		t.Fatal("empty root created local output")
	}
}

func TestNativeExtensionFilesCanonicalRoots(t *testing.T) {
	base := t.TempDir()
	actual := filepath.Join(base, "mounted")
	if err := os.Mkdir(actual, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(actual, "input"), []byte("input"), 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "Volumes")
	if err := os.Symlink(actual, alias); err != nil {
		t.Fatal(err)
	}
	roots := []string{actual, alias}
	if runtime.GOOS == "darwin" && strings.HasPrefix(base, "/private/") {
		roots = append(roots, strings.TrimPrefix(base, "/private")+"/Volumes")
	}
	// Exercise a filesystem-root prefix without writing outside owned scratch.
	volume := filepath.VolumeName(actual)
	rootRelative := strings.TrimPrefix(actual, volume+string(filepath.Separator))
	for i, root := range append(roots, volume+string(filepath.Separator)) {
		input, prefix := "input", "results"
		if i == len(roots) {
			input = filepath.Join(rootRelative, input)
			prefix = filepath.Join(rootRelative, prefix)
		}
		j := Job{ID: ID(), Attempt: 1, Spec: Spec{Extension: "fixture@1", Inputs: map[string]string{"input": input}, Outputs: map[string]string{"result": prefix}}}
		w := Worker{JobFileRoot: root}
		if err := w.prepareExtensionFiles(&j, t.TempDir()); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(j.Spec.Run.Workdir, "result"), []byte("output bytes"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := w.publishExtensionFiles(j); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(root, outputRelative(j, "result", prefix)))
		if err != nil || string(b) != "output bytes" {
			t.Fatal("output bytes not published through canonical root", err)
		}
	}
}

func TestNativeExtensionMainMemoryCapTargets(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		e := compositionExtension(t, true)
		e.Variants[0].Target.OS = osname
		e.Variants[0].Execution.Main.MemoryGB = .5
		e.Variants[0].Needs.HostMemoryGB = 1
		e = freezeComposition(t, e)
		m := machine("cap-worker", "fake")
		m.CapabilityProfile.OS, m.CapabilityProfile.Arch = osname, runtime.GOARCH
		m.ExtensionFingerprint = "fingerprint"
		if reason := e.targetUnavailableReason(machineTarget(&m)); reason != "" {
			t.Fatalf("%s: %s", osname, reason)
		}
		j := Job{Spec: Spec{Extension: extensionKey(e)}, Extensions: []Extension{e}}
		r := newExtensionReport(e, &m, "extension:"+e.Name)
		r.Setup, r.Qualified, r.CheckID = true, true, "successful-check"
		m.ExtensionReports = []ExtensionReport{r}
		if qualifiedExtension(&j, &m) == nil {
			t.Fatal("valid capped extension excluded")
		}
		j.Spec.Needs = e.Variants[0].Needs
		if err := e.lower(&j.Spec, &m); err != nil {
			t.Fatal(err)
		}
		if j.Spec.Run.MemoryGB != .5 {
			t.Fatal("cap lost in lowering")
		}
		q, _ := fixture(t)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		queued := submit(t, q, Spec{Name: "capped", Project: "test", Extension: extensionKey(e)})
		m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 8
		if osname == "linux" {
			m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
		} else {
			m.MemoryEnforcement = "memory ceiling not enforced on this host (requires Linux cgroup v2)"
		}
		for _, recipe := range e.Variants[0].Environments {
			m.Environments = append(m.Environments, recipe.Key)
		}
		check(t, q, m, nil)
		run := check(t, q, m, nil).Job
		if run == nil || run.ID != queued.ID {
			t.Fatalf("%s capped uv extension did not place", osname)
		}

	}
}

func TestNativeExtensionPythonImportIdentity(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("local Python required for sibling-import regression")
	}
	e := compositionExtension(t, false)
	e.Files["helper.py"] = []byte("value = 6\n")
	e.Files["app.py"] = []byte("import helper\nassert helper.value == 6\n")
	e = freezeComposition(t, e)
	root := t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	folder := extensionFolder(root, e)
	// Apple's system Python defaults to an external cache. Select the normal
	// in-library cache location explicitly, without changing application code.
	t.Setenv("PYTHONPYCACHEPREFIX", filepath.Join(folder, "__pycache__"))
	t.Setenv("PYTHONDONTWRITEBYTECODE", "")
	env, err := extensionEnvironment(root, e, "fake", "0")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join(folder, "app.py"))
	cmd.Env = cleanExtensionEnv(env)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ordinary import: %v %s", err, out)
	}
	actual, err := ReadExtension(folder)
	if err != nil || actual.Digest != e.Digest {
		t.Fatal("ordinary sibling import changed immutable digest", err)
	}
	if err := os.WriteFile(filepath.Join(folder, "helper.py"), []byte("value = 7\n"), 0600); err != nil {
		t.Fatal(err)
	}
	actual, err = ReadExtension(folder)
	if err != nil || actual.Digest == e.Digest {
		t.Fatal("authored source change escaped identity", err)
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
	r := newExtensionReport(e, &w.Machine, j.Spec.workType())
	r.Setup, r.Qualified, r.CheckID = true, true, "successful-check"
	if err := writeExtensionReports(w.ExtensionRoot, e, "fake", []ExtensionReport{r}); err != nil {
		t.Fatal(err)
	}
	reports := LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	if len(reports) == 0 || !reports[0].Qualified {
		t.Fatal("PATH-free prepared pixi lost qualification", reports)
	}
	startNativeExtensionFixture(t, w)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	reports, err = CheckExtension(ctx, w.ExtensionRoot, e, "fake", w.Machine.Device, w.Machine.HostID, io.Discard, w.environmentCache, w.Client)
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

func TestNativeExtensionUVImportQualificationNASAndMainCap(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("local Python required for sibling-import regression")
	}
	e := compositionExtension(t, true)
	e.Kinds = nil
	delete(e.Variants[0].Baselines, "kind:sample")
	v := &e.Variants[0]
	v.Needs.HostMemoryGB = 1
	v.Execution.Main.MemoryGB = .5
	v.Execution.Main.Command = []string{"{env.main.python}", "{extension}/app.py"}
	v.Check = []string{"{env.main.python}", "{extension}/check.py"}
	e.Files["helper.py"] = []byte("def transform(values):\n    return [sum(values)]\n")
	e.Files["app.py"] = []byte("import json, helper\nwith open('input') as f: values = json.load(f)\nwith open('output', 'w') as f: f.write(json.dumps(helper.transform(values)) + '\\n')\n")
	e.Files["check.py"] = []byte("import json, os, helper\nassert 'MYCE_TOKEN' not in os.environ\nwith open(os.environ['MYCE_CHECK_INPUT']) as f: values = json.load(f)\nwith open(os.environ['MYCE_CHECK_OUTPUT'], 'w') as f: f.write(json.dumps(helper.transform(values)) + '\\n')\n")
	e.Files["baseline/output"] = []byte("[3]\n")
	e = freezeComposition(t, e)
	q, w := prepareTestWorker(t, e)
	w.Machine.Device = "0"
	w.Machine.ExtensionFingerprint, _ = ExtensionFingerprint(context.Background(), "fake", "0")
	w.extensionFingerprint = w.Machine.ExtensionFingerprint
	w.JobFileRoot = t.TempDir()
	if err := os.WriteFile(filepath.Join(w.JobFileRoot, "input"), []byte("[1, 2, 3]\n"), 0600); err != nil {
		t.Fatal(err)
	}
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
	j := submit(t, q, Spec{Name: "ordinary-import", Project: "test", Extension: extensionKey(e), Inputs: map[string]string{"input": "input"}, Outputs: map[string]string{"output": "results"}})
	plans := check(t, q, w.Machine, nil).EnvironmentPlan
	if len(plans) != 1 {
		t.Fatal("capped uv extension did not receive preparation")
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
		t.Fatal("capped uv qualification", err, checkLog.String())
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
	if _, err := os.Stat(filepath.Join(extensionFolder(w.ExtensionRoot, e), "__pycache__")); !os.IsNotExist(err) {
		t.Fatal("runtime wrote bytecode into frozen library")
	}
	out := job(t, q, j.ID)
	b, err := os.ReadFile(filepath.Join(w.JobFileRoot, out.OutputFiles["output"]))
	if err != nil || string(b) != "[6]\n" {
		t.Fatal("ordinary job did not publish expected NAS bytes", err)
	}
	if runtime.GOOS == "darwin" {
		snap, _ := q.Snapshot()
		for _, m := range snap.Machines {
			if !strings.Contains(m.MemoryEnforcement, "not enforced") {
				t.Fatal("Mac falsely claimed memory enforcement")
			}
		}
	}
}
