package myce

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

func prepareTestWorker(t *testing.T, e Extension) (*Queue, Worker) {
	t.Helper()
	q, _ := fixture(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := machine("prepare-worker", "fake")
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
	m.HostID, _ = os.Hostname()
	m.ExtensionFingerprint, _ = ExtensionFingerprint(context.Background(), "fake", m.Device)
	m.EnvironmentBuilder = true
	server := testServerForEnvironments(t, q)
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 4 << 20}
	return q, Worker{StateDir: newTestStateRoot(t), ExtensionRoot: t.TempDir(), Machine: m, Clock: RealClock{}, environmentCache: c, Client: Client{URL: server, Token: "test"}, extensionFingerprint: m.ExtensionFingerprint}
}
func TestExtensionAutomaticPrepareTwoJobsOneBuildAndPlacement(t *testing.T) {
	e := compositionExtension(t, true)
	v := &e.Variants[0]
	r := v.Environments["main"]
	r.LockFile = "locks/uv.lock"
	v.Environments["main"] = r
	e.Files["locks/uv.lock"] = []byte("synthetic")
	e.Files["locks/pyproject.toml"] = []byte("synthetic")
	e = freezeComposition(t, e)
	q, w := prepareTestWorker(t, e)
	// Real HTTP/worker loop starts without a local extension, environment or qualification.
	var builds atomic.Int32
	w.environmentRun = func(_ context.Context, _ []string, dir string, _ []string, _ io.Writer) error {
		builds.Add(1)
		return os.MkdirAll(filepath.Join(dir, "venv", "bin"), 0700)
	}
	for range 2 {
		submit(t, q, Spec{Name: "prepare", Project: "test", Extension: extensionKey(e)})
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
		snap, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}

		if len(snap.Machines) == 0 {
			return false
		}
		m := snap.Machines[0]
		for _, r := range m.ExtensionReports {
			if r.Setup {
				if r.Qualified || r.CheckID != "" || r.Host == "" || r.EnvironmentIdentity == "" || r.CheckDigest == "" {
					t.Fatal("install qualified or lost identity", r)
				}
				for _, j := range snap.Jobs {
					if j.State != "queued" || qualifiedExtension(j, m) != nil {
						t.Fatal("unqualified job placed", j.State)
					}
				}
				return true
			}
		}
		return false
	})
	if builds.Load() != 1 {
		t.Fatalf("two jobs built %d times", builds.Load())
	}
	installed, err := ReadExtension(extensionFolder(w.ExtensionRoot, e))
	if err != nil || installed.Digest != e.Digest {
		t.Fatal("registry bytes not fetched by digest", err)
	}
}
func TestExtensionPrepareCustodyAndQualificationIdentity(t *testing.T) {
	e := compositionExtension(t, false)
	q, w := prepareTestWorker(t, e)
	j := submit(t, q, Spec{Name: "prepare", Project: "test", Extension: extensionKey(e)})
	reply := check(t, q, w.Machine, nil)
	if reply.Job != nil || len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Preparation.Digest != e.Digest {
		t.Fatal("missing preparation", reply)
	}
	plan := reply.EnvironmentPlan[0]
	q.mu.Lock()
	obligations := q.hostObligations(hostID(&w.Machine))
	q.mu.Unlock()
	if len(obligations) != 1 || obligations[0].Launch == nil || !obligations[0].Preparation {
		t.Fatal("preparation not counted", obligations)
	}
	if err := q.AuthorizeLaunch(*plan.Launch); err != nil {
		t.Fatal(err)
	}
	if err := w.prepareRegisteredExtension(withLaunchGate(context.Background(), func() error { return q.AuthorizeLaunch(*plan.Launch) }), plan); err != nil {
		t.Fatal(err)
	}
	r := newExtensionReport(e, &w.Machine, j.Spec.workType())
	r.Setup = true
	r.Qualified = true
	r.CheckID = "exact-check"
	w.Machine.ExtensionReports = []ExtensionReport{r}
	if qualifiedExtension(j, &w.Machine) == nil {
		t.Fatal("exact qualification excluded")
	}
	for _, field := range []string{"host", "fingerprint", "environment", "check", "invocation", "setup"} {
		changed := r
		switch field {
		case "host":
			changed.Host = "other"
		case "fingerprint":
			changed.Fingerprint = "stale"
		case "environment":
			changed.EnvironmentIdentity = "stale"
		case "check":
			changed.CheckDigest = "stale"
		case "invocation":
			changed.CheckID = ""
		case "setup":
			changed.Setup = false
		}
		w.Machine.ExtensionReports = []ExtensionReport{changed}
		if qualifiedExtension(j, &w.Machine) != nil {
			t.Fatal("stale qualification placed", field)
		}
	}
	// Drain invalidates an unstarted delayed offer through the same launch gate.
	w.Machine.ExtensionReports = nil
	if _, err := q.ChangeHostMaintenance(hostID(&w.Machine), HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	if err := q.AuthorizeLaunch(*plan.Launch); err == nil {
		t.Fatal("drained preparation launched")
	}
}
func TestExtensionPrepareMissingPixiVisibleFailure(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	e := compositionExtension(t, false)
	v := &e.Variants[0]
	v.Environments = map[string]EnvironmentRecipe{"main": {Backend: "pixi", LockFile: "env/pixi.lock"}}
	v.Execution.Main.Environment = "main"
	e.Files["env/pixi.lock"] = []byte("locked")
	e.Files["env/pixi.toml"] = []byte("[workspace]")
	e = freezeComposition(t, e)
	q, w := prepareTestWorker(t, e)
	submit(t, q, Spec{Name: "prepare", Project: "test", Extension: extensionKey(e)})
	plan := check(t, q, w.Machine, nil).EnvironmentPlan[0]
	err := w.prepareRegisteredExtension(context.Background(), plan)
	if err == nil || !strings.Contains(err.Error(), "pixi is not installed") {
		t.Fatal(err)
	}
	reports := LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	if len(reports) == 0 || reports[0].Qualified || !strings.Contains(reports[0].Reason, "pixi is not installed") {
		t.Fatal("missing prerequisite invisible", reports)
	}
	w.Machine.ExtensionReports = reports
	w.Machine.EnvironmentReports = []EnvironmentReport{{Key: plan.Key, State: "failed", Error: err.Error()}}
	reply := check(t, q, w.Machine, nil)
	if reply.Job != nil || len(reply.EnvironmentPlan) > 0 {
		t.Fatal("failed preparation retried or placed", reply)
	}
}
func TestExtensionPrepareAssetsSingleFlightAndUseProtection(t *testing.T) {
	e := compositionExtension(t, false)
	e.Assets = FileManifest{"model": {SHA256: strings.Repeat("a", 64), SizeBytes: 5}}
	e = freezeComposition(t, e)
	q, w := prepareTestWorker(t, e)
	submit(t, q, Spec{Name: "prepare", Project: "test", Extension: extensionKey(e)})
	var fetched atomic.Int32
	w.environmentCache.Fetch = func(_ context.Context, _ FileRef, p string) error {
		fetched.Add(1)
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			return err
		}
		return os.WriteFile(p, []byte("model"), 0600)
	}
	plan := check(t, q, w.Machine, nil).EnvironmentPlan[0]
	for range 2 {
		if err := w.prepareRegisteredExtension(context.Background(), plan); err != nil {
			t.Fatal(err)
		}
	}
	if fetched.Load() != 1 {
		t.Fatal("assets fetched repeatedly", fetched.Load())
	}
	root, release, err := w.environmentCache.Acquire(context.Background(), EnvironmentRecipe{Key: plan.Key})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	size, err := environmentSize(root)
	if err != nil {
		t.Fatal(err)
	}
	w.environmentCache.Limit = size + 1
	other := binaryRecipe(t, "fake")
	if _, r, err := w.environmentCache.Acquire(context.Background(), other); err == nil {
		r()
		t.Fatal("in-use extension assets evicted")
	}
	if !w.environmentCache.ready(plan.Key) {
		t.Fatal("active assets lost")
	}
}

// Keep a fake-clock queue check separate from the real worker scheduling loop.
func TestExtensionPrepareFingerprintChangesOffer(t *testing.T) {
	e := compositionExtension(t, false)
	q, w := prepareTestWorker(t, e)
	submit(t, q, Spec{Name: "prepare", Project: "test", Extension: extensionKey(e)})
	first := check(t, q, w.Machine, nil).EnvironmentPlan[0]
	w.Machine.EnvironmentReports = []EnvironmentReport{{Key: first.Key, State: "failed"}}
	if len(check(t, q, w.Machine, nil).EnvironmentPlan) != 0 {
		t.Fatal("failure was not negatively cached")
	}
	w.Machine.ExtensionFingerprint = "changed"
	next := check(t, q, w.Machine, nil).EnvironmentPlan
	if len(next) != 1 || next[0].Key == first.Key {
		t.Fatal("stale fingerprint reused")
	}
}

func TestEnvironmentActiveSidecarRetained(t *testing.T) {
	s := nativeSpec(t)
	recipe, err := snapshotEnvironment(EnvironmentRecipe{Build: []string{"sh", "-c", "printf prepared > '{root}/bin/prepared'"}}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	s.Run.Sidecars[0].Environment = "helper-tools"
	r := nativeStart(t, s, "fake", EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"helper-tools": {"fake": recipe}}})
	nativeFile(t, r.dir, "main-ready")
	root := filepath.Join(r.w.environmentCache.Root, recipe.Key)
	size, err := environmentSize(root)
	if err != nil {
		t.Fatal(err)
	}
	other := binaryRecipe(t, "fake")
	r.w.environmentCache.Limit = size + 1
	if _, release, err := r.w.environmentCache.Acquire(context.Background(), other); err == nil {
		release()
		t.Fatal("evicted active sidecar environment")
	}
	if !r.w.environmentCache.ready(recipe.Key) {
		t.Fatal("sidecar environment lost")
	}
	nativeTouch(t, r.dir, "finish")
	if result := nativeFinish(t, r); !result.Success {
		t.Fatal(result)
	}
}

func TestEnvironmentPreparedExecutionNeverBuilds(t *testing.T) {
	r := binaryRecipe(t, "fake")
	r.readyOnly = true
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20}
	if _, _, err := c.Acquire(context.Background(), r); err == nil {
		t.Fatal("execution rebuilt an unprepared environment")
	}
	if c.ready(r.Key) {
		t.Fatal("execution published readiness")
	}
}

func TestExtensionPixiPreparedQualifiedExecution(t *testing.T) {
	builds := fakePixi(t)
	e := compositionExtension(t, false)
	v := &e.Variants[0]
	v.Environments = map[string]EnvironmentRecipe{"main": {Backend: "pixi", LockFile: "env/pixi.lock"}}
	v.Execution.Main.Environment = "main"
	v.Execution.Main.Command = []string{"tool"}
	e.Files["env/pixi.lock"] = []byte("locked")
	e.Files["env/pixi.toml"] = []byte("[workspace]")
	e = freezeComposition(t, e)
	q, w := prepareTestWorker(t, e)
	j := submit(t, q, Spec{Name: "pixi", Project: "test", Extension: extensionKey(e)})
	plan := check(t, q, w.Machine, nil).EnvironmentPlan[0]
	if err := w.prepareRegisteredExtension(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	// E3 will own the check invocation. Here supply an exact successful record,
	// exercising E2's record contract and placement/activation through the real worker.
	r := newExtensionReport(e, &w.Machine, j.Spec.workType())
	r.Setup = true
	r.Qualified = true
	r.CheckID = "synthetic-exact-check"
	if err := writeExtensionReports(w.ExtensionRoot, e, "fake", []ExtensionReport{r}); err != nil {
		t.Fatal(err)
	}
	w.EnvironmentRoot = w.environmentCache.Root
	// Adopt the session used for preparation, rather than impersonate a second
	// live worker with the same name during the handoff to its ordinary loop.
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
		out := job(t, q, j.ID)
		if terminal(out.State) {
			if out.State != "succeeded" {
				t.Fatal(out.State, out.Reason)
			}
			return true
		}
		return false
	})
	logs, err := q.Logs(j.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, l := range logs {
		if strings.Contains(l.Text, "activated:yes") {
			found = true
		}
	}
	if !found {
		t.Fatal("pixi job did not receive activation", logs)
	}
	b, err := os.ReadFile(builds)
	if err != nil || string(b) != "install\n" {
		t.Fatal("execution rebuilt pixi", string(b), err)
	}
}
