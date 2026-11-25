package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func prepareTestWorker(t *testing.T, e Extension) (*Queue, Worker) {
	t.Helper()
	q, _ := fixture(t)
	return q, prepareExtensionTestWorker(t, q, e)
}

func prepareExtensionTestWorker(t *testing.T, q *Queue, e Extension) Worker {
	t.Helper()
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
	return Worker{StateDir: newTestStateRoot(t), ExtensionRoot: t.TempDir(), Machine: m, Clock: RealClock{}, EnvironmentLimit: 4 << 20, environmentCache: c, Client: Client{URL: server, Token: "test"}, extensionFingerprint: m.ExtensionFingerprint}
}

func TestExtensionPreparationGetsFirstIdleCheckin(t *testing.T) {
	for _, priority := range []string{"experiment", "production"} {
		t.Run(priority, func(t *testing.T) {
			q, clock := fixture(t)
			e := compositionExtension(t, false)
			w := prepareExtensionTestWorker(t, q, e)
			m := w.Machine
			m.Devices = m3Machine().Devices
			batch := spec("experiment")
			batch.Needs.Platform = Platform{Required: []string{"fake"}}
			holder := submit(t, q, batch)
			running := assignedJob(machineCheck(t, q, m))
			if running == nil || running.ID != holder.ID {
				t.Fatal("missing accelerator holder", running)
			}
			demand := submit(t, q, Spec{Name: "cold extension", Project: batch.Project, Class: "experiment", Extension: extensionKey(e)})
			reply := machineCheck(t, q, m, running)
			if len(reply.EnvironmentPlan) != 0 {
				t.Fatal("extension preparation overlapped exact accelerator custody", reply.EnvironmentPlan)
			}
			held, err := q.jobSnapshot(demand.ID)
			if err != nil {
				t.Fatal(err)
			}
			if held.Hold == nil || held.Hold.Kind != "native_qualification" ||
				!strings.Contains(held.Hold.Reason, "accelerator attempt "+ShortID(running.ID)+"/1") {
				t.Fatalf("native qualification hold did not identify the accelerator holder: %+v", held.Hold)
			}
			clock.Advance(time.Second)
			batch.Priority = priority
			next := submit(t, q, batch)
			finish(t, q, running, true)
			reply = machineCheck(t, q, m)
			if priority == "production" {
				if got := assignedJob(reply); got == nil || got.ID != next.ID {
					t.Fatal("preparation delayed higher-tier accelerator work", got)
				}
				return
			}
			if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch.Demand != demand.ID || assignedJob(reply) != nil {
				got := storedTestJob(t, q, demand.ID)
				t.Fatalf("idle check-in did not give the older extension preparation a turn: offers=%d next=%s demand=%s (%s)", len(reply.EnvironmentPlan), storedTestJob(t, q, next.ID).State, got.State, got.Reason)
			}
			if err := q.AuthorizeLaunch(*reply.EnvironmentPlan[0].Launch); err != nil {
				t.Fatalf("preparation offer could not start: %v", err)
			}
		})
	}
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
	w.environmentRun = func(_ context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
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
				if r.Qualified || r.CheckID != "" && !strings.HasPrefix(r.CheckID, "auto-") || r.Host == "" || r.EnvironmentIdentity == "" || r.CheckDigest == "" {
					t.Fatal("install qualified or lost identity", r)
				}
				for _, j := range snap.Jobs {
					if j.Spec.ExtensionCheck != nil {
						continue // Prepared demand may now request its internal check.
					}
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
	if err := w.prepareRegisteredExtension(withLaunchGate(context.Background(), func(ctx context.Context) error { return q.AuthorizeLaunch(*plan.Launch) }), plan); err != nil {
		t.Fatal(err)
	}
	r := newExtensionReport(e, &w.Machine, j.Spec.workType())
	r.Setup = true
	r.Qualified = true
	r.CheckID = "exact-check"
	w.Machine.ExtensionReports = []ExtensionReport{r}
	w.Machine.qualificationMemo = map[extensionQualificationKey]string{}
	if qualifiedExtension(j, &w.Machine) == nil {
		t.Fatal("exact qualification excluded")
	}
	for _, field := range []string{"host", "fingerprint", "environment", "check", "invocation", "setup", "target", "digest", "platform", "work", "qualified"} {
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
		case "target":
			target := *changed.Target
			target.Arch = "other"
			changed.Target = &target
		case "digest":
			changed.Digest = "other"
		case "platform":
			changed.Platform = "other"
		case "work":
			changed.Work = "other"
		case "qualified":
			changed.Qualified = false
		}
		w.Machine.ExtensionReports = []ExtensionReport{changed}
		if qualifiedExtension(j, &w.Machine) != nil {
			t.Fatal("stale qualification placed", field)
		}
	}
	// Inventory copies used by capacity projection share the pass memo, but
	// a different host, target or device must still require its own check.
	w.Machine.ExtensionReports = []ExtensionReport{r}
	for _, field := range []string{"host", "fingerprint", "target", "device"} {
		changed := w.Machine
		switch field {
		case "host":
			changed.HostID = "other"
		case "fingerprint":
			changed.ExtensionFingerprint = "other"
		case "target":
			profile := *changed.CapabilityProfile
			profile.Arch = "other"
			changed.CapabilityProfile = &profile
		case "device":
			changed.Device = "other"
			changed.Devices = []MachineDevice{}
		}
		if qualifiedExtension(j, &changed) != nil {
			t.Fatal("qualification reused across inventory identity", field)
		}
	}
	// Report targets are compared by value, including an in-place change.
	target := *r.Target
	r.Target = &target
	w.Machine.ExtensionReports = []ExtensionReport{r}
	if qualifiedExtension(j, &w.Machine) == nil {
		t.Fatal("unchanged target excluded")
	}
	target.Arch = "other"
	if qualifiedExtension(j, &w.Machine) != nil {
		t.Fatal("mutated report target reused")
	}
	w.Machine.qualificationMemo = nil
	// Drain invalidates an unstarted delayed offer through the same launch gate.
	w.Machine.ExtensionReports = nil
	if _, err := q.ChangeHostMaintenance(hostID(&w.Machine), HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	if err := q.AuthorizeLaunch(*plan.Launch); err == nil {
		t.Fatal("drained preparation launched")
	}
}
func TestExtensionPreparePixiAcquisitionVisibleFailure(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	e := compositionExtension(t, false)
	v := &e.Variants[0]
	v.Environments = map[string]EnvironmentRecipe{"main": {Backend: "pixi", LockFile: "env/pixi.lock"}}
	v.Execution.Main.Environment = "main"
	e.Files["env/pixi.lock"] = []byte("locked")
	e.Files["env/pixi.toml"] = []byte("[workspace]")
	e = freezeComposition(t, e)
	q, w := prepareTestWorker(t, e)
	w.environmentCache.Tools = &EnvironmentToolSource{Home: func() (string, error) { return t.TempDir(), nil }, Probe: ToolBootstrapProbe{Fetch: func(context.Context, ToolRelease) (io.ReadCloser, error) {
		return nil, errors.New("synthetic pixi acquisition failure")
	}}}
	submit(t, q, Spec{Name: "prepare", Project: "test", Extension: extensionKey(e)})
	plan := check(t, q, w.Machine, nil).EnvironmentPlan[0]
	err := w.prepareRegisteredExtension(context.Background(), plan)
	if err == nil || !strings.Contains(err.Error(), "tool_download_failed") {
		t.Fatal(err)
	}
	reports := LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	if len(reports) == 0 || reports[0].Qualified || !strings.Contains(reports[0].Reason, "tool_download_failed") {
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
	size, err := environmentDiskUsage(root)
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

// Preparation belongs to the queue even before useful work has free RAM.
func TestExtensionPreparationAheadOfJobMemory(t *testing.T) {
	e := compositionExtension(t, false)
	q, w := prepareTestWorker(t, e)
	j := submit(t, q, Spec{Name: "prepare", Project: "test", Extension: extensionKey(e)})
	w.Machine.HostMemoryMeasured, w.Machine.HostAvailableMemoryGB = true, 0.01
	reply := check(t, q, w.Machine, nil)
	if reply.Job != nil || len(reply.EnvironmentPlan) != 1 {
		t.Fatal("missing prerequisite offer", reply)
	}
	plan := reply.EnvironmentPlan[0]
	if plan.Preparation == nil || plan.Preparation.Digest != e.Digest || plan.Launch.Demand != j.ID {
		t.Fatal("offer lost exact demand", plan)
	}
	if err := q.AuthorizeLaunch(*plan.Launch); err != nil {
		t.Fatal("prerequisite depends on useful job admission", err)
	}
	q.clock.(*fakeClock).Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "queued" {
		t.Fatal("busy host was treated as incapable", got.State, got.Reason)
	}
}
