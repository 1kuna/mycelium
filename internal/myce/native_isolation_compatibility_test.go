//go:build linux || darwin

package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Laptop proof uses tiny synthetic files, real Worker.start and real processes;
// queue authorization, host inventory and memory admission remain fixtures.
// MYCE_TEST_NATIVE_NAS_ROOT=/absolute/mounted/root opts into the same proof on
// NAS. Only a new MkdirTemp child is populated/removed; existing NAS data is
// never scanned or changed. There is no new direct-NAS payload declaration.
func TestNativeIsolationCompatibility(t *testing.T) {
	t.Run("staged-files-checkpoint", func(t *testing.T) {
		w, root, j := nativeCompatibilityFixture(t, "files")
		j.Attempt = 2
		// Exercise the existing piece-output transport and worker publisher,
		// without requiring a frozen extension or bypassing Worker.start.
		j.Spec.After = &PieceDependency{Job: "tiny-upstream", Piece: "text", Outputs: map[string]string{"input.txt": "source.txt"}}
		j.Dependency = &PieceBinding{Inputs: map[string]string{"input.txt": "inputs/source.txt"}}
		j.Spec.Outputs = map[string]string{"output.txt": "results"}
		nativeCompatibilityWrite(t, filepath.Join(w.JobFileRoot, "inputs/source.txt"), "native compatibility input\n")
		nativeCompatibilityWrite(t, filepath.Join(j.Checkpoint, "attempt-1/checkpoint.json"), nativeCompatibilitySave1)
		j.Spec.Run.Env["EXPECTED_CHECKPOINT"] = attemptCheckpoint(j)
		p := nativeCompatibilityRun(t, w, root, j, 30*time.Second)
		for name, path := range map[string]string{
			"staged-input":            filepath.Join(p.job.Spec.Run.Workdir, "input.txt"),
			"work-output":             filepath.Join(p.job.Spec.Run.Workdir, "output.txt"),
			"worker-published-output": filepath.Join(w.JobFileRoot, outputRelative(j, "output.txt", "results")),
		} {
			t.Run(name, func(t *testing.T) { nativeCompatibilityRead(t, path, "native compatibility input\n") })
		}
		t.Run("checkpoint", func(t *testing.T) {
			nativeCompatibilityRead(t, filepath.Join(attemptCheckpoint(j), "checkpoint.json"), nativeCompatibilitySave2)
			nativeCompatibilityRead(t, filepath.Join(j.Checkpoint, "attempt-1/checkpoint.json"), nativeCompatibilitySave1)
		})
		input, err := os.Stat(filepath.Join(w.JobFileRoot, "inputs/source.txt"))
		staged, stagedErr := os.Stat(filepath.Join(p.job.Spec.Run.Workdir, "input.txt"))
		if err != nil || stagedErr != nil || os.SameFile(input, staged) {
			t.Errorf("declared NAS input must be copied into work: source=%v staged=%v", err, stagedErr)
		}
	})
	t.Run("shared-cache", func(t *testing.T) {
		for _, configured := range []bool{false, true} {
			name := "default"
			if configured {
				name = "configured"
			}
			t.Run(name, func(t *testing.T) {
				w, root, j := nativeCompatibilityFixture(t, "cache")
				cache := filepath.Join(root.dir, "model-cache")
				if configured {
					w.CacheRoot = newTestStateRoot(t)
					cache = w.CacheRoot
				}
				w.ModelRoot = t.TempDir()
				paths := []string{filepath.Join(cache, "tiny-model"), filepath.Join(w.ModelRoot, "tiny-model")}
				for _, path := range paths {
					nativeCompatibilityWrite(t, path, "synthetic model bytes")
				}
				nativeCompatibilityPaths(t, &j, paths)
				nativeCompatibilityRun(t, w, root, j, 30*time.Second)
				for _, path := range paths {
					nativeCompatibilityRead(t, path, "synthetic model bytes")
					if _, err := os.Stat(path + ".new"); !os.IsNotExist(err) {
						t.Errorf("payload created shared cache content: %v", err)
					}
				}
			})
		}
	})
	t.Run("sibling-undeclared-nas", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("Linux content-grant proof; macOS has a targeted boundary")
		}
		w, root, j := nativeCompatibilityFixture(t, "denied")
		paths := []string{
			filepath.Join(root.dir, "job-files/sibling/1/secret"),
			filepath.Join(root.dir, "job-files/sibling/1/.tmp/secret"),
			filepath.Join(root.dir, "job-files/sibling/1/.scratch/secret"),
			filepath.Join(w.JobFileRoot, "undeclared/secret"),
			filepath.Join(w.JobFileRoot, "inputs/source.txt"),
			filepath.Join(j.Checkpoint, "attempt-1/secret"),
			filepath.Join(root.dir, "job-files", j.ID, "1/.scratch/secret"),
		}
		for _, path := range paths {
			nativeCompatibilityWrite(t, path, "synthetic private compatibility bytes")
			nativeCompatibilityRead(t, path, "synthetic private compatibility bytes")
		}
		j.Attempt = 2
		j.Spec.Inputs = map[string]string{"input.txt": "inputs/source.txt"}
		nativeCompatibilityPaths(t, &j, paths)
		nativeCompatibilityRun(t, w, root, j, 30*time.Second)
	})
	t.Run("external-runtime-model", func(t *testing.T) {
		w, root, j := nativeCompatibilityFixture(t, "external")
		nativeCompatibilityExternal(t, &w, &j)
		nativeCompatibilityRun(t, w, root, j, 30*time.Second)
	})
	t.Run("two-sidecars", func(t *testing.T) {
		w, root, j := nativeCompatibilityFixture(t, "sidecars")
		nativeCompatibilityExternal(t, &w, &j)
		nativeCompatibilitySidecars(&j)
		nativeCompatibilityRun(t, w, root, j, 30*time.Second)
	})
}

// Explicit operator opt-in only; no downloads, installation or model loads.
// MYCE_TEST_NATIVE_OFFICE must be an existing absolute LibreOffice executable.
// The already installed pdftotext command verifies text in the produced PDF.
// An opt-in failure (including a missing tool or Linux AF_UNIX refusal) FAILS;
// this test never weakens the boundary or substitutes an unconfined conversion.
func TestNativeIsolationLibreOffice(t *testing.T) {
	office, requested := os.LookupEnv("MYCE_TEST_NATIVE_OFFICE")
	if !requested {
		t.Skip("set MYCE_TEST_NATIVE_OFFICE to an operator-installed absolute LibreOffice executable")
	}
	t.Run("conversion", func(t *testing.T) {
		info, err := os.Stat(office)
		if !filepath.IsAbs(office) || err != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 {
			t.Fatal("MYCE_TEST_NATIVE_OFFICE must name an existing absolute executable")
		}
		w, root, j := nativeCompatibilityFixture(t, "office")
		office, err = filepath.EvalSymlinks(office)
		if err != nil {
			t.Fatal(err)
		}
		// The operator selects the installed program. Grant its installation,
		// not /Applications, /opt, the NAS, or an authored loader search path.
		installation := filepath.Dir(office)
		if filepath.Base(installation) == "MacOS" && filepath.Base(filepath.Dir(installation)) == "Contents" {
			installation = filepath.Dir(installation)
		}
		w.NativeExecuteRoots = append(w.NativeExecuteRoots, installation)
		nativeCompatibilityExternal(t, &w, &j)
		nativeCompatibilitySidecars(&j)
		j.Spec.Run.Env["NATIVE_COMPATIBILITY_OFFICE"] = office
		j.Spec.Run.Env["HOME"] = "{work}/home"
		for key, dir := range map[string]string{"XDG_CONFIG_HOME": "config", "XDG_CACHE_HOME": "cache", "XDG_DATA_HOME": "data"} {
			j.Spec.Run.Env[key] = "{work}/home/" + dir
		}
		p := nativeCompatibilityRun(t, w, root, j, time.Minute)
		pdf := filepath.Join(p.job.Spec.Run.Workdir, ".tmp/office-output/document.pdf")
		t.Run("pdf-bytes", func(t *testing.T) {
			data, err := os.ReadFile(pdf)
			if err != nil || !bytes.HasPrefix(data, []byte("%PDF-")) || !bytes.Contains(data, []byte("%%EOF")) {
				t.Errorf("LibreOffice did not produce complete PDF bytes: %v", err)
			}
		})
		t.Run("pdf-text", func(t *testing.T) {
			// Extraction is an external verifier, after the confined conversion.
			tool, err := exec.LookPath("pdftotext")
			if err != nil {
				t.Fatal("opt-in PDF text verification requires already-installed pdftotext; nothing is installed by this test")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, tool, pdf, "-").CombinedOutput()
			if err != nil || !strings.Contains(string(out), nativeCompatibilityOfficeText) {
				t.Errorf("converted PDF text: %v\n%s", err, out)
			}
		})
	})
}

const (
	nativeCompatibilitySave1      = `{"myce_checkpoint":{"generation":1},"value":"before"}`
	nativeCompatibilitySave2      = `{"myce_checkpoint":{"generation":2},"value":"after"}`
	nativeCompatibilityOfficeText = "Mycelium native office compatibility 177"
)

func nativeCompatibilityFixture(t *testing.T, mode string) (Worker, *workerStore, Job) {
	t.Helper()
	w := recoveryWorker(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 1, nil }
	if slices.Contains(w.nativeIsolationCapabilities(), "native_isolation_unavailable") {
		_, nas := os.LookupEnv("MYCE_TEST_NATIVE_NAS_ROOT")
		_, office := os.LookupEnv("MYCE_TEST_NATIVE_OFFICE")
		if nas || office {
			t.Fatal("explicit qualification requested but native confinement is unavailable")
		}
		t.Skip("native confinement unavailable on this laptop")
	}
	// Override TestMain's synthetic checkpoint preparation with the real NAS
	// contract: existing root, per-attempt copy, file fsync and atomic rename.
	w.checkpointPrepare = prepareCheckpoint
	nas := t.TempDir()
	if mount, requested := os.LookupEnv("MYCE_TEST_NATIVE_NAS_ROOT"); requested {
		info, err := os.Stat(mount)
		if !filepath.IsAbs(mount) || err != nil || !info.IsDir() {
			t.Fatal("MYCE_TEST_NATIVE_NAS_ROOT must name an existing absolute mounted directory")
		}
		nas, err = os.MkdirTemp(mount, ".myce-native-compatibility-")
		if err != nil {
			t.Fatal(err)
		}
		owned := nas
		t.Cleanup(func() {
			if err := os.RemoveAll(owned); err != nil {
				t.Errorf("remove owned NAS fixture %s: %v", filepath.Base(owned), err)
			}
		})
		t.Logf("mounted-NAS opt-in: owned fixture %s", filepath.Base(nas))
	}
	var err error
	nas, err = filepath.EvalSymlinks(nas)
	if err != nil {
		t.Fatal(err)
	}
	w.JobFileRoot, w.CheckpointRoot = filepath.Join(nas, "files"), filepath.Join(nas, "checkpoints")
	for _, path := range []string{w.JobFileRoot, w.CheckpointRoot} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(root.close)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	j := recoveryJob(t, []string{exe, "_native-compatibility-test", mode})
	j.Attempt = 2
	j.Checkpoint = filepath.Join(w.CheckpointRoot, j.ID)
	j.Spec.Logs = "keep"
	j.Spec.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: .125, Platform: Platform{Required: []string{"cpu"}}}
	j.Spec.Run.Env = map[string]string{"GORACE": "atexit_sleep_ms=0"}
	// Both members' authored overrides must lose to worker-owned paths.
	for _, key := range []string{"TMPDIR", "TMP", "TEMP"} {
		j.Spec.Run.Env[key] = filepath.Join(root.dir, "ignored-authored-"+key)
	}
	protected := []string{
		filepath.Join(root.dir, "job-files/sibling/1/secret"),
		filepath.Join(root.dir, "job-files/sibling/1/.tmp/secret"),
		filepath.Join(root.dir, "job-files/sibling/1/.scratch/secret"),
		filepath.Join(root.dir, "job-files", j.ID, "1/.scratch/secret"),
		filepath.Join(root.dir, "service/joining.json"),
		filepath.Join(root.dir, "control/authority"),
	}
	for _, path := range protected {
		nativeCompatibilityWrite(t, path, "synthetic private compatibility bytes")
	}
	w.ServiceConfig = &ServiceConfig{JoiningFile: protected[4]}
	data, err := json.Marshal(protected)
	if err != nil {
		t.Fatal(err)
	}
	j.Spec.Run.Env["NATIVE_COMPATIBILITY_PROTECTED"] = string(data)
	if runtime.GOOS == "linux" {
		nativeCompatibilityMetricDenials(t, &j)
		t.Log("real Worker.start has no delegated ceilings in this fixture: main and sidecars inherit the outer cgroup; exact-leaf metric reads do not prove distinct nested member cgroups")
	}
	return w, root, j
}

func nativeCompatibilitySidecars(j *Job) {
	for _, name := range []string{"llama", "sortformer"} {
		zero := 0
		// Sidecars deliberately do not inherit main's authored env. Supply
		// their own fixture targets explicitly, as a real adapter must.
		env := maps.Clone(j.Spec.Run.Env)
		for _, key := range []string{"TMPDIR", "TMP", "TEMP"} {
			env[key] = "/ignored-sidecar-temp"
		}
		j.Spec.Run.Sidecars = append(j.Spec.Run.Sidecars, NativeSidecar{
			Name: name, Command: []string{j.Spec.Run.Command[0], "_native-compatibility-test", "serve", name},
			Port: &zero, Readiness: &NativeReadiness{Path: "/ready", Timeout: "10s"},
			Env: env,
		})
	}
}

// Copy the current fixture executable outside every default runtime/cache
// grant. No compiler, package installation, model load or external process is
// needed to prepare this fixture. Writable owner modes make denied writes a
// confinement assertion rather than an ordinary DAC denial.
func nativeCompatibilityExternal(t *testing.T, w *Worker, j *Job) {
	t.Helper()
	runtimeRoot, modelRoot := t.TempDir(), t.TempDir()
	if runtime.GOOS == "linux" {
		// Exercise visibility after bubblewrap replaces /tmp even when the
		// outer scheduler's TMPDIR points elsewhere. Direct and sidecar cases
		// both require these model bytes, RO writes and direct-exec denial.
		var err error
		modelRoot, err = os.MkdirTemp("/tmp", "myce-native-models-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.RemoveAll(modelRoot); err != nil {
				t.Errorf("remove owned model fixture: %v", err)
			}
		})
	}
	copyInstalled := func(source, destination string) {
		t.Helper()
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		nativeCompatibilityWrite(t, destination, string(data))
		if err := os.Chmod(destination, 0700); err != nil {
			t.Fatal(err)
		}
	}
	// System-signed macOS executables may be killed when relocated, before
	// Seatbelt participates. The current Go fixture has no platform-binary
	// location requirement and supplies the same real external exec proof.
	source, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(runtimeRoot, "bin/runtime")
	copyInstalled(source, executable)
	forbidden := filepath.Join(modelRoot, "must-not-exec")
	copyInstalled(source, forbidden)
	model := filepath.Join(modelRoot, "tiny-model")
	nativeCompatibilityWrite(t, model, "synthetic model bytes")
	w.NativeExecuteRoots = append(w.NativeExecuteRoots, runtimeRoot)
	w.NativeReadRoots = append(w.NativeReadRoots, modelRoot)
	j.Spec.Run.Env["NATIVE_COMPATIBILITY_RUNTIME"] = executable
	j.Spec.Run.Env["NATIVE_COMPATIBILITY_MODEL"] = model
	j.Spec.Run.Env["NATIVE_COMPATIBILITY_NO_EXEC"] = forbidden
	// On current macOS most system dylibs exist only in the shared cache.
	// Exercise an actual installed on-disk library when one exists, rather
	// than claiming a fabricated library or dyld-cache path was loaded.
	candidates := []string{"/usr/lib/libSystem.B.dylib", "/opt/homebrew/opt/pcre2/lib/libpcre2-8.0.dylib", "/usr/local/opt/pcre2/lib/libpcre2-8.0.dylib", "/lib64/libc.so.6"}
	for _, pattern := range []string{"/lib/*/libc.so.6", "/usr/lib/*/libc.so.6"} {
		matches, _ := filepath.Glob(pattern)
		candidates = append(candidates, matches...)
	}
	for _, source := range candidates {
		if info, err := os.Stat(source); err == nil && info.Mode().IsRegular() {
			library := filepath.Join(runtimeRoot, "lib", filepath.Base(source))
			copyInstalled(source, library)
			f, err := os.Open(library)
			if err != nil {
				t.Fatal(err)
			}
			mapped, mapErr := unix.Mmap(int(f.Fd()), 0, 1, unix.PROT_READ|unix.PROT_EXEC, unix.MAP_PRIVATE)
			if mapErr == nil {
				mapErr = unix.Munmap(mapped)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if mapErr != nil {
				t.Logf("installed library %s cannot map executable even unconfined: %v", filepath.Base(source), mapErr)
				continue
			}
			j.Spec.Run.Env["NATIVE_COMPATIBILITY_LIBRARY"] = library
			t.Logf("external runtime library executable-mapping proof uses installed %s; this is not a dlopen/ABI qualification", filepath.Base(source))
			return
		}
	}
	t.Log("no known installed on-disk shared library: external executable RX is covered; shared-library mapping remains unverified")
}

// Resolve only existing cgroup2 membership. The fixture never creates cgroups,
// changes membership or accepts environment variables as policy authority.
func nativeCompatibilityCgroupPath() (string, error) {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", err
	}
	group := ""
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::/") {
			group = strings.TrimPrefix(line, "0::")
		}
	}
	if group == "" {
		return "", errors.New("no cgroup2 membership")
	}
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for _, line := range strings.Split(string(mounts), "\n") {
		before, after, ok := strings.Cut(line, " - ")
		fields := strings.Fields(before)
		if !ok || !strings.HasPrefix(after, "cgroup2 ") || len(fields) < 5 {
			continue
		}
		rel, err := filepath.Rel(unescape.Replace(fields[3]), group)
		if err == nil && (rel == "." || filepath.IsLocal(rel)) {
			return filepath.Join(unescape.Replace(fields[4]), rel), nil
		}
	}
	return "", errors.New("no cgroup2 mount for current membership")
}

var nativeCompatibilityMetrics = []string{"memory.peak", "memory.max", "memory.events", "memory.current", "memory.stat"}

func nativeCompatibilityMetricDenials(t *testing.T, j *Job) {
	t.Helper()
	leaf, err := nativeCompatibilityCgroupPath()
	if err != nil {
		t.Logf("cgroup2 leaf metric proof unavailable: %v; proc membership and meminfo are still required", err)
		return
	}
	// Authored test targets are negative assertions, never grant hints. Select
	// a real sibling when present; do not invent a synthetic nested hierarchy.
	var denied []string
	siblingFound := false
	parent := filepath.Dir(leaf)
	if parent != leaf {
		entries, err := os.ReadDir(parent)
		if err == nil {
			for _, entry := range entries {
				if entry.IsDir() && entry.Name() != filepath.Base(leaf) {
					path := filepath.Join(parent, entry.Name(), "memory.current")
					if _, err := os.ReadFile(path); err == nil {
						denied = append(denied, path)
						siblingFound = true
						break
					}
				}
			}
		}
		if path := filepath.Join(parent, "memory.current"); path != filepath.Join(leaf, "memory.current") {
			if _, err := os.ReadFile(path); err == nil {
				denied = append(denied, path)
			}
		}
	}
	if !siblingFound {
		t.Log("no readable existing sibling cgroup metric: other-attempt cgroup denial is not proven by this fixture; protected-spool sibling denial is still required")
	}
	data, err := json.Marshal(denied)
	if err != nil {
		t.Fatal(err)
	}
	j.Spec.Run.Env["NATIVE_COMPATIBILITY_CGROUP_DENIED"] = string(data)
}

func nativeCompatibilityRun(t *testing.T, w Worker, root *workerStore, j Job, budget time.Duration) *process {
	t.Helper()
	store, err := newAttemptStore(root, j)
	if err != nil {
		t.Fatal(err)
	}
	// The ordinary worker retries while its asynchronous NAS input copy is
	// pending. Reuse the same attempt store so a slow mount is not misreported
	// as confinement failure or copied twice.
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	var p *process
	for {
		p, err = w.start(store, j)
		if err == nil || !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending") {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("declared input copy did not finish within the tiny probe budget")
		case <-tick.C:
		}
	}
	if err != nil {
		t.Fatalf("real Worker.start: %v", err)
	}
	finished := false
	defer func() {
		// A bounded observation prevents a broken tiny probe retaining child
		// processes/reservations. Failure cleanup costs at most another 10s.
		if !finished {
			p.stopDirective(w.Clock.Now(), 0, true, true)
			select {
			case f := <-p.done:
				if f.err != nil || f.result.ReleasePending {
					t.Errorf("failure cleanup unconfirmed: %+v", f)
				}
			case <-time.After(10 * time.Second):
				t.Error("failure cleanup timed out")
			}
		}
		out, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
		if len(out) > 0 {
			t.Logf("confined payload log:\n%s", out)
		}
	}()
	select {
	case f := <-p.done:
		finished = true
		if f.err != nil || !f.result.Success || f.result.ReleasePending {
			t.Errorf("confined execution/publication/cleanup: %+v", f)
		}
	case <-ctx.Done():
		t.Fatal("tiny confined compatibility probe timed out")
	}
	t.Run("process-cleanup", func(t *testing.T) {
		members, err := workerGroupMembers(p.cmd.Process.Pid)
		if err != nil || len(members) != 0 {
			t.Errorf("attempt/main/sidecars still present: %v, %v", members, err)
		}
	})
	var protected []string
	if err := json.Unmarshal([]byte(j.Spec.Run.Env["NATIVE_COMPATIBILITY_PROTECTED"]), &protected); err != nil {
		t.Fatal(err)
	}
	for _, path := range protected {
		nativeCompatibilityRead(t, path, "synthetic private compatibility bytes")
		if _, err := os.Stat(path + ".new"); !os.IsNotExist(err) {
			t.Errorf("protected sibling/credential/control file created: %v", err)
		}
	}
	return p
}

func nativeCompatibilityWrite(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func nativeCompatibilityRead(t *testing.T, path, want string) {
	t.Helper()
	if data, err := os.ReadFile(path); err != nil || string(data) != want {
		t.Errorf("%s bytes: %q; %v", filepath.Base(path), data, err)
	}
}

func nativeCompatibilityPaths(t *testing.T, j *Job, paths []string) {
	t.Helper()
	data, err := json.Marshal(paths)
	if err != nil {
		t.Fatal(err)
	}
	j.Spec.Run.Env["NATIVE_COMPATIBILITY_PATHS"] = string(data)
}

// Confined subprocesses bypass TestMain's HOME/tool fixture provisioning.
func init() {
	if len(os.Args) > 1 && os.Args[1] == "_native-compatibility-test" {
		if err := nativeCompatibilityPayload(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
}

func nativeCompatibilityMemberChecks(work string) error {
	var failures []error
	check := func(err error) {
		if err != nil {
			failures = append(failures, err)
		}
	}
	for key, suffix := range map[string]string{"TMPDIR": ".tmp", "TMP": ".tmp", "TEMP": ".tmp", "MYCE_SCRATCH_DIR": ".scratch"} {
		path := os.Getenv(key)
		resolved, err := filepath.EvalSymlinks(path)
		if !filepath.IsAbs(path) || err != nil || resolved != filepath.Join(work, suffix) {
			check(fmt.Errorf("%s is not the exact absolute managed attempt path: %v", key, err))
		}
	}
	for _, key := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE", "SSH_AUTH_SOCK", "DBUS_SESSION_BUS_ADDRESS"} {
		if os.Getenv(key) != "" {
			check(fmt.Errorf("payload inherited worker credential/control variable %s", key))
		}
	}
	var protected []string
	check(json.Unmarshal([]byte(os.Getenv("NATIVE_COMPATIBILITY_PROTECTED")), &protected))
	for i, path := range protected {
		if _, err := os.ReadFile(path); err == nil {
			check(fmt.Errorf("protected sibling/scratch/credential/control %d readable", i))
		}
		for _, target := range []string{path, path + ".new"} {
			if err := os.WriteFile(target, []byte("escaped"), 0600); err == nil {
				check(fmt.Errorf("protected sibling/scratch/credential/control %d writable", i))
			}
		}
	}
	check(nativeCompatibilityExternalChecks())
	if runtime.GOOS == "linux" {
		check(nativeCompatibilityMetricChecks())
	}
	return errors.Join(failures...)
}

func nativeCompatibilityExternalChecks() error {
	executable := os.Getenv("NATIVE_COMPATIBILITY_RUNTIME")
	if executable == "" {
		return nil
	}
	var failures []error
	check := func(err error) {
		if err != nil {
			failures = append(failures, err)
		}
	}
	model, forbidden := os.Getenv("NATIVE_COMPATIBILITY_MODEL"), os.Getenv("NATIVE_COMPATIBILITY_NO_EXEC")
	data, err := os.ReadFile(model)
	if err != nil || string(data) != "synthetic model bytes" {
		check(fmt.Errorf("declared external model not readable: %v", err))
	}
	out, err := exec.Command(executable, "_native-compatibility-test", "runtime-executable").CombinedOutput()
	if err != nil || string(out) != "external-runtime-177" {
		check(fmt.Errorf("declared external runtime did not execute: %v: %s", err, out))
	}
	if out, err := exec.Command(forbidden, "_native-compatibility-test", "runtime-executable").CombinedOutput(); !os.IsPermission(err) {
		check(fmt.Errorf("direct exec from declared RO model root not permission-denied: %v: %s", err, out))
	}
	paths := []string{executable, model, forbidden}
	if library := os.Getenv("NATIVE_COMPATIBILITY_LIBRARY"); library != "" {
		paths = append(paths, library)
		f, err := os.Open(library)
		if err == nil {
			// A real installed library image is readable and can be mapped RX.
			// This tests file policy, not dlopen, ABI or application behavior.
			mapped, mapErr := unix.Mmap(int(f.Fd()), 0, 1, unix.PROT_READ|unix.PROT_EXEC, unix.MAP_PRIVATE)
			if mapErr == nil {
				mapErr = unix.Munmap(mapped)
			}
			err = errors.Join(mapErr, f.Close())
		}
		if err != nil {
			check(fmt.Errorf("installed external runtime library RX mapping failed: %w", err))
		}
	}
	for _, path := range paths {
		if data, err := os.ReadFile(path); err != nil || len(data) == 0 {
			check(fmt.Errorf("declared external content unreadable: %s: %v", filepath.Base(path), err))
		}
		for _, target := range []string{path, path + ".new"} {
			if err := os.WriteFile(target, []byte("poisoned"), 0600); !os.IsPermission(err) {
				check(fmt.Errorf("declared external runtime/model write not permission-denied: %s: %v", filepath.Base(target), err))
			}
		}
	}
	return errors.Join(failures...)
}

func nativeCompatibilityMetricChecks() error {
	var failures []error
	for _, path := range []string{"/proc/self/cgroup", "/proc/meminfo"} {
		if data, err := os.ReadFile(path); err != nil || len(data) == 0 {
			failures = append(failures, fmt.Errorf("own membership/host memory unreadable: %s: %v", path, err))
		}
	}
	leaf, err := nativeCompatibilityCgroupPath()
	if err != nil {
		if os.IsPermission(err) {
			failures = append(failures, fmt.Errorf("own cgroup membership/mount discovery denied: %w", err))
		}
		fmt.Fprintln(os.Stdout, "exact cgroup leaf metric proof unavailable:", err)
		return errors.Join(failures...)
	}
	// Opening without writing proves denial without ever altering a real
	// scheduler cgroup if the policy regresses. No PID/limit/control write is
	// sent to the kernel, even on a failing run.
	writeDenied := func(path string) {
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err == nil {
			_ = f.Close()
		}
		if !os.IsPermission(err) && !errors.Is(err, unix.EROFS) {
			failures = append(failures, fmt.Errorf("cgroup metric/control write open not permission-denied: %s: %v", filepath.Base(path), err))
		}
	}
	for _, name := range nativeCompatibilityMetrics {
		path := filepath.Join(leaf, name)
		data, err := os.ReadFile(path)
		if err != nil || len(data) == 0 {
			failures = append(failures, fmt.Errorf("exact current-leaf %s unreadable: %v", name, err))
		} else if name == "memory.peak" || name == "memory.current" || name == "memory.max" && strings.TrimSpace(string(data)) != "max" {
			if _, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64); err != nil {
				failures = append(failures, fmt.Errorf("invalid %s: %w", name, err))
			}
		} else if name == "memory.events" || name == "memory.stat" {
			fields := strings.Fields(string(data))
			if len(fields) < 2 || len(fields)%2 != 0 {
				failures = append(failures, fmt.Errorf("invalid %s counters", name))
			}
		}
		writeDenied(path)
	}
	for _, name := range []string{"cgroup.procs", "cgroup.subtree_control"} {
		writeDenied(filepath.Join(leaf, name))
	}
	var denied []string
	if encoded := os.Getenv("NATIVE_COMPATIBILITY_CGROUP_DENIED"); encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &denied); err != nil {
			failures = append(failures, err)
		}
	}
	for _, path := range denied {
		if _, err := os.ReadFile(path); !os.IsPermission(err) {
			failures = append(failures, fmt.Errorf("other/parent cgroup metric read not permission-denied: %v", err))
		}
		writeDenied(path)
	}
	return errors.Join(failures...)
}

func nativeCompatibilityPayload(args []string) error {
	if len(args) == 0 {
		return errors.New("missing compatibility probe mode")
	}
	if args[0] == "runtime-executable" {
		fmt.Fprint(os.Stdout, "external-runtime-177")
		return nil
	}
	work, err := os.Getwd()
	if err != nil {
		return err
	}
	tmp, err := filepath.EvalSymlinks(os.Getenv("TMPDIR"))
	if err != nil || tmp != filepath.Join(work, ".tmp") {
		return errors.New("temp is not the exact managed attempt directory")
	}
	var paths []string
	if encoded := os.Getenv("NATIVE_COMPATIBILITY_PATHS"); encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &paths); err != nil {
			return err
		}
	}
	var failures []error
	check := func(err error) {
		if err != nil {
			failures = append(failures, err)
		}
	}
	check(nativeCompatibilityMemberChecks(work))
	switch args[0] {
	case "files":
		checkpoint := os.Getenv("MYCE_CHECKPOINT_DIR")
		if checkpoint != os.Getenv("EXPECTED_CHECKPOINT") {
			check(errors.New("MYCE_CHECKPOINT_DIR differs from the exact assigned attempt"))
		}
		data, err := os.ReadFile("input.txt")
		check(err)
		if string(data) != "native compatibility input\n" {
			check(errors.New("declared input bytes not staged"))
		}
		check(os.WriteFile("output.txt", data, 0600))
		saved, err := os.ReadFile(filepath.Join(checkpoint, "checkpoint.json"))
		check(err)
		if string(saved) != nativeCompatibilitySave1 {
			check(errors.New("previous checkpoint generation not readable"))
		}
		// Same-directory save, file flush, rename and existing CIFS-aware dir sync.
		tmp := filepath.Join(checkpoint, "checkpoint.tmp")
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			_, writeErr := f.WriteString(nativeCompatibilitySave2)
			err = errors.Join(writeErr, f.Sync(), f.Close())
		}
		if err == nil {
			err = os.Rename(tmp, filepath.Join(checkpoint, "checkpoint.json"))
		}
		if err == nil {
			err = syncWorkerDir(checkpoint)
		}
		check(err)
	case "cache":
		for _, path := range paths {
			data, err := os.ReadFile(path)
			if err != nil || string(data) != "synthetic model bytes" {
				check(fmt.Errorf("shared model content unreadable: %s: %v", filepath.Base(path), err))
			}
			for _, target := range []string{path, path + ".new"} {
				if err := os.WriteFile(target, []byte("poisoned"), 0600); !os.IsPermission(err) {
					check(fmt.Errorf("shared cache content write not permission-denied: %v", err))
				}
			}
		}
	case "denied":
		for i, path := range paths {
			if _, err := os.ReadFile(path); !os.IsPermission(err) {
				check(fmt.Errorf("protected content %d read not permission-denied: %v", i, err))
			}
		}
	case "external":
		// Every member's common checks exercise the declared runtime and RO
		// model roots; a separate case also covers the non-group start path.
	case "serve":
		if len(args) != 2 {
			return errors.New("missing sidecar name")
		}
		if err := errors.Join(failures...); err != nil {
			return err
		}
		return nativeCompatibilityServe(work, args[1])
	case "sidecars":
		check(nativeCompatibilityExchange(work))
	case "office":
		check(nativeCompatibilityExchange(work))
		scratch, tmp := os.Getenv("MYCE_SCRATCH_DIR"), os.Getenv("TMPDIR")
		profileDir, outputDir := filepath.Join(scratch, "office-profile"), filepath.Join(tmp, "office-output")
		for _, dir := range []string{"home/config", "home/cache", "home/data", profileDir, outputDir} {
			if err := os.MkdirAll(dir, 0700); err != nil {
				return err
			}
		}
		// Real conversion consumes bytes that crossed main -> Sortformer ->
		// main through the shared scratch directory. No successful substitute
		// or unconfined retry exists if LibreOffice refuses this policy.
		document, err := os.ReadFile(filepath.Join(scratch, "sortformer-to-main.txt"))
		if err != nil || string(document) != nativeCompatibilityOfficeText+"\n" {
			check(fmt.Errorf("Office source did not complete sidecar scratch round trip: %v", err))
			return errors.Join(failures...)
		}
		documentPath := filepath.Join(scratch, "document.txt")
		if err := os.WriteFile(documentPath, document, 0600); err != nil {
			return err
		}
		profile := (&url.URL{Scheme: "file", Path: profileDir}).String()
		cmd := exec.Command(os.Getenv("NATIVE_COMPATIBILITY_OFFICE"),
			"--headless", "--nologo", "--nodefault", "--nolockcheck", "--norestore",
			"-env:UserInstallation="+profile, "--convert-to", "pdf:writer_pdf_Export", "--outdir", outputDir, documentPath)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		check(cmd.Run())
	default:
		return errors.New("unknown compatibility probe mode")
	}
	return errors.Join(failures...)
}

func nativeCompatibilityServe(work, name string) error {
	port := os.Getenv("MYCE_SIDECAR_PORT")
	if _, err := strconv.ParseUint(port, 10, 16); err != nil || port == "0" {
		return errors.New("sidecar has no actual reserved port")
	}
	tmp, scratch := os.Getenv("TMPDIR"), os.Getenv("MYCE_SCRATCH_DIR")
	for _, dir := range []string{tmp, scratch} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0600); err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:"+port)
	if err != nil {
		return err
	}
	defer listener.Close()
	return http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
		case "/exchange":
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			for _, dir := range []string{scratch, tmp} {
				data, err := os.ReadFile(filepath.Join(dir, "main-to-sidecar.txt"))
				if err != nil || string(data) != nativeCompatibilityOfficeText+"\n" {
					http.Error(w, "sidecar cannot read main's shared scratch/temp bytes", http.StatusInternalServerError)
					return
				}
				if err := os.WriteFile(filepath.Join(dir, name+"-to-main.txt"), data, 0600); err != nil {
					http.Error(w, "sidecar cannot publish shared scratch/temp reply", http.StatusInternalServerError)
					return
				}
			}
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"name": name, "port": port, "pid": strconv.Itoa(os.Getpid()), "work": work, "tmp": tmp, "scratch": scratch})
	}))
}

func nativeCompatibilityExchange(work string) error {
	var failures []error
	check := func(err error) {
		if err != nil {
			failures = append(failures, err)
		}
	}
	tmp, scratch := os.Getenv("TMPDIR"), os.Getenv("MYCE_SCRATCH_DIR")
	for _, dir := range []string{scratch, tmp} {
		check(os.WriteFile(filepath.Join(dir, "main-to-sidecar.txt"), []byte(nativeCompatibilityOfficeText+"\n"), 0600))
	}
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	ports, pids := map[string]bool{}, map[string]bool{}
	for _, name := range []string{"llama", "sortformer"} {
		address := os.Getenv("MYCE_SIDECAR_" + strings.ToUpper(name) + "_URL")
		u, err := url.Parse(address)
		if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.Port() == "" || ports[u.Port()] {
			check(fmt.Errorf("%s lacks a distinct reserved loopback URL", name))
			continue
		}
		ports[u.Port()] = true
		response, err := client.Post(address+"/exchange", "application/octet-stream", nil)
		if err != nil {
			check(err)
			continue
		}
		var receipt map[string]string
		err = json.NewDecoder(io.LimitReader(response.Body, 8192)).Decode(&receipt)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || receipt["name"] != name || receipt["port"] != u.Port() || receipt["pid"] == "" || receipt["pid"] == strconv.Itoa(os.Getpid()) || pids[receipt["pid"]] {
			check(fmt.Errorf("%s did not answer from its distinct real process/port: %v", name, err))
		}
		pids[receipt["pid"]] = true
		if receipt["work"] != work || receipt["tmp"] != tmp || receipt["scratch"] != scratch {
			check(fmt.Errorf("%s does not share main's exact absolute work/scratch/temp paths", name))
		}
		for _, dir := range []string{scratch, tmp} {
			if data, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(data) != name {
				check(fmt.Errorf("%s startup file not shared in %s: %v", name, filepath.Base(dir), err))
			}
			if data, err := os.ReadFile(filepath.Join(dir, name+"-to-main.txt")); err != nil || string(data) != nativeCompatibilityOfficeText+"\n" {
				check(fmt.Errorf("%s reply bytes not shared in %s: %v", name, filepath.Base(dir), err))
			}
		}
	}
	return errors.Join(failures...)
}
