package myce

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A heartbeat response can still offer a preparation that finished after its
// report snapshot. That stale offer must not erase the unacknowledged result.
func TestEnvironmentReadyReportSurvivesPreparationReplay(t *testing.T) {
	q, clock := fixture(t)
	r := binaryRecipe(t, "fake")
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}
	if _, release, err := cache.Acquire(context.Background(), r); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"ready": {"fake": r}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "ready"
	job := submit(t, q, s)
	m := machine("builder", "fake", "host")
	m.EnvironmentBuilder = true
	plan := check(t, q, m, nil).EnvironmentPlan
	if len(plan) != 1 || plan[0].Launch == nil {
		t.Fatal("missing exact preparation", plan)
	}
	attempt := plan[0].Launch.Attempt
	entered, finish := make(chan struct{}), make(chan struct{})
	replayRelease := make(chan struct{})
	var finishOnce sync.Once
	var calls atomic.Int32
	rt := &environmentRuntime{dir: t.TempDir(), cache: cache, reports: map[string]EnvironmentReport{}}
	rt.authorize = func(_ context.Context, l HostLaunch) error {
		if calls.Add(1) == 1 {
			err := q.AuthorizeLaunch(l)
			close(entered)
			<-finish
			return err
		}
		<-replayRelease
		return q.AuthorizeLaunch(l)
	}
	defer func() {
		finishOnce.Do(func() { close(finish) })
		close(replayRelease)
		rt.stop()
	}()
	done := func() <-chan struct{} {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		return rt.done
	}
	rt.sync(plan, false)
	<-entered
	m.Environments, m.EnvironmentReports, _, _ = rt.report()
	if len(m.EnvironmentReports) != 1 || m.EnvironmentReports[0].State != "building" {
		t.Fatal("missing building snapshot", m.EnvironmentReports)
	}
	stale := check(t, q, m, nil)
	if stale.Job != nil || len(stale.EnvironmentPlan) != 1 || stale.EnvironmentPlan[0].Launch.Attempt != attempt {
		t.Fatal("lost started preparation", stale)
	}
	completed := done()
	finishOnce.Do(func() { close(finish) })
	<-completed
	rt.sync(stale.EnvironmentPlan, false)
	clock.Advance(time.Second)
	m.Environments, m.EnvironmentReports, _, _ = rt.report()
	if len(m.EnvironmentReports) != 1 || m.EnvironmentReports[0].State != "ready" || m.EnvironmentReports[0].Attempt != attempt {
		t.Fatal("replay overwrote exact ready completion", m.EnvironmentReports)
	}
	if calls.Load() != 1 {
		t.Fatal("replay authorized preparation again", calls.Load())
	}
	reply := check(t, q, m, nil)
	if _, held := q.s.PreparationPlans[attempt]; held || reply.Job == nil || reply.Job.ID != job.ID {
		t.Fatal("ready report did not discharge custody and start job", reply)
	}
	rt.sync(reply.EnvironmentPlan, true)

	// A new attempt for the same cache key still needs its own completion.
	next := q.issuePreparation(&m, "environment", r.Key)
	if err := q.AuthorizeLaunch(next); err != nil {
		t.Fatal(err)
	}
	check(t, q, m, reply.Job)
	if _, held := q.s.PreparationPlans[next.Attempt]; !held {
		t.Fatal("old ready report discharged new attempt")
	}
	replacement := r
	replacement.Launch = &next
	rt.sync([]EnvironmentRecipe{replacement}, false)
	m.Environments, m.EnvironmentReports, _, _ = rt.report()
	if len(m.EnvironmentReports) != 1 || m.EnvironmentReports[0].State != "building" || m.EnvironmentReports[0].Attempt != next.Attempt {
		t.Fatal("new attempt reused old completion", m.EnvironmentReports)
	}
}

func binaryRecipe(t *testing.T, platform string) EnvironmentRecipe {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gpu-task")
	if e := os.WriteFile(p, []byte("#!/bin/sh\necho environment-command\n"), 0700); e != nil {
		t.Fatal(e)
	}
	r, e := snapshotEnvironment(EnvironmentRecipe{Binary: p}, platform)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestEnvironmentLRUEvictionProtectsUse(t *testing.T) {
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}}
	a := binaryRecipe(t, "cuda")
	b := binaryRecipe(t, "xpu")
	d := binaryRecipe(t, "metal")
	pa, release, e := c.Acquire(context.Background(), a)
	if e != nil {
		t.Fatal(e)
	}
	pb, rb, e := c.Acquire(context.Background(), b)
	if e != nil {
		t.Fatal(e)
	}
	rb()
	os.Chtimes(filepath.Join(pa, "ready.json"), time.Unix(1, 0), time.Unix(1, 0))
	os.Chtimes(filepath.Join(pb, "ready.json"), time.Unix(2, 0), time.Unix(2, 0))
	// Keep a two-installation budget, including the cache's .Trash directory
	// blocks. The third installation must evict the unused entry to fit.
	c.Limit, e = c.DiskUsage()
	if e != nil {
		t.Fatal(e)
	}
	_, rd, e := c.Acquire(context.Background(), d)
	if e != nil {
		t.Fatal(e)
	}
	rd()
	if !c.ready(a.Key) || c.ready(b.Key) || !c.ready(d.Key) {
		t.Fatal("evicted in-use entry instead of oldest available entry")
	}
	// An old entry a queued job still names is not evictable either: with
	// one entry in use and the other referenced, admission refuses capacity.
	c.references = func(_ context.Context, key string) (bool, error) { return key == d.Key, nil }
	os.Chtimes(filepath.Join(c.Root, d.Key, "ready.json"), time.Unix(3, 0), time.Unix(3, 0))
	if c.Limit, e = c.DiskUsage(); e != nil {
		t.Fatal(e)
	}
	if _, _, e = c.Acquire(context.Background(), binaryRecipe(t, "rocm")); e == nil || !c.ready(a.Key) || !c.ready(d.Key) {
		t.Fatal("evicted an in-use or queued environment", e)
	}
	release()
	trash, _ := os.ReadDir(filepath.Join(c.Root, ".Trash"))
	if len(trash) != 0 {
		t.Fatal("eviction did not free retired bytes")
	}
}
func TestEnvironmentFailedBuildLogs(t *testing.T) {
	q, clock := fixture(t)
	r := binaryRecipe(t, "fake")
	q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"broken": {"fake": r}}})
	s := spec("experiment")
	s.Environment.Name = "broken"
	j, e := q.Submit(s)
	if e != nil {
		t.Fatal(e)
	}
	report := Checkin{Machine: Machine{Capabilities: []string{"native_isolation_landlock"}, CapabilityProfile: machine("profile", "fake").CapabilityProfile, ID: "worker", Session: "session", GPU: "fake", FreeMemoryGB: 64, EnvironmentBuilder: true, EnvironmentReports: []EnvironmentReport{{Key: r.Key, State: "failed", Error: "build exit 9", Log: "compiler output\n"}}}}
	_, e = q.Checkin(report)
	if e != nil {
		t.Fatal(e)
	}
	// This headerless worker becomes an eligible build target on its second
	// accepted arrival. Keep the failed report present across qualification.
	clock.Advance(time.Second)
	if _, e = q.Checkin(report); e != nil {
		t.Fatal(e)
	}
	logs, e := q.Logs(j.ID, 0)
	if e != nil || len(logs) != 1 || !strings.Contains(logs[0].Text, "compiler output") {
		t.Fatalf("logs %v %v", logs, e)
	}
	snap, _ := q.Snapshot()
	for _, j := range snap.Jobs {
		if j.State != "queued" {
			t.Fatal(j.State)
		}
	}
}
func testServerForEnvironments(t *testing.T, q *Queue) string {
	t.Helper()
	s := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(s.Close)
	return s.URL
}

func TestEnvironmentPlacementBuildAndAssignmentUseSameOrdering(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tool": {"fake": r}}})
	warmSoftware := machine("software", "fake")
	warmSoftware.EnvironmentBuilder = true
	warmSoftware.Environments = []string{r.Key}
	warmModel := machine("model", "fake")
	warmModel.EnvironmentBuilder = true
	warmModel.Models = []string{"model"}
	warmModel.EnvironmentBuildSeconds = map[string]float64{r.Key: 1}
	check(t, q, warmSoftware, nil)
	check(t, q, warmModel, nil)
	s := spec("production")
	s.Environment.Name = "tool"
	s.Needs.Model = "model"
	j := submit(t, q, s)
	if check(t, q, warmSoftware, nil).Job != nil {
		t.Fatal("software-only worker stole preferred model-ready placement")
	}
	if len(check(t, q, warmModel, nil).EnvironmentPlan) != 1 {
		t.Fatal("cold environment placement deadlocked behind a software-ready peer")
	}
	warmModel.Environments = []string{r.Key}
	if got := check(t, q, warmModel, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("both-ready worker did not receive job", got)
	}
}

func TestBuildOnlyLauncherRelocation(t *testing.T) {
	r, e := snapshotEnvironment(EnvironmentRecipe{Build: []string{"test-build"}}, "fake")
	if e != nil {
		t.Fatal(e)
	}
	// Nested gate/job temporary roots can exceed Linux's shebang length limit.
	// Keep the fixture's own-bin interpreter path short enough to execute.
	cacheRoot, e := os.MkdirTemp("/tmp", "myce-launcher-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(cacheRoot); err != nil {
			t.Error(err)
		}
	})
	c := &EnvironmentCache{Root: cacheRoot, Limit: 1 << 20, Clock: RealClock{}, Run: func(_ context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
		// A local build can emit a launcher whose interpreter lives in its own bin.
		if e := os.Symlink("/bin/sh", filepath.Join(dir, "bin", "interpreter")); e != nil {
			return e
		}
		return os.WriteFile(filepath.Join(dir, "bin", "tool"), []byte("#!"+filepath.Join(dir, "bin", "interpreter")+"\necho relocated\n"), 0700)
	}}
	root, release, e := c.Acquire(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	b, e := exec.Command(filepath.Join(root, "bin", "tool")).Output()
	if e != nil || string(b) != "relocated\n" {
		t.Fatalf("published launcher broken: %q %v", b, e)
	}
}

func TestEnvironmentAdmissionCountsHardlinksOnce(t *testing.T) {
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}}
	if err := c.init(); err != nil {
		t.Fatal(err)
	}
	a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
	for _, key := range []string{a, b} {
		if err := os.Mkdir(filepath.Join(c.Root, key), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(c.Root, key, "ready.json"), []byte("ready"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	payload := filepath.Join(c.Root, a, "wheel")
	if err := os.WriteFile(payload, make([]byte, 1<<20), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(payload, filepath.Join(c.Root, b, "wheel")); err != nil {
		t.Fatal(err)
	}
	// A build links the same wheel; its bytes must also be deduplicated.
	partial := filepath.Join(c.Root, strings.Repeat("c", 64)+".partial")
	if err := os.Mkdir(partial, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(payload, filepath.Join(partial, "wheel")); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("du", "-sk", c.Root).Output()
	if err != nil {
		t.Fatal(err)
	}
	var kib int64
	if _, err = fmt.Sscan(string(out), &kib); err != nil {
		t.Fatal(err)
	}
	c.Limit = kib * 1024
	if err := c.admit(strings.Repeat("c", 64), 0); err != nil {
		t.Fatalf("shared wheel exceeds physical limit: %v", err)
	}
	// After one prefix is removed, its shared wheel still consumes space.
	old := time.Unix(1, 0)
	if err := os.Chtimes(filepath.Join(c.Root, a, "ready.json"), old, old); err != nil {
		t.Fatal(err)
	}
	c.Limit = 1
	if err := c.admit(strings.Repeat("c", 64), 0); err == nil {
		t.Fatal("evicting a link credited the shared wheel as freed")
	}
}

func TestEnvironmentFailedBuildFloorFreeAutoMemory(t *testing.T) {
	q, clock := fixture(t)
	r := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"broken": {"fake": r}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "broken"
	s.Needs.Memory, s.Needs.MemoryGB = "auto", 0
	j := submit(t, q, s)
	clock.Advance(time.Second)
	m := cpuMachine("builder", "builder-host", "fake", 8, 64)
	m.TotalMemoryGB = 8
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.EnvironmentBuilder = true
	m.EnvironmentReports = []EnvironmentReport{{Key: r.Key, State: "failed", Error: "build exit 9", Log: "compiler output\n"}}
	check(t, q, m, nil)
	if got := job(t, q, j.ID); got.State != "queued" || !strings.Contains(got.Reason, "build exit 9") {
		t.Errorf("failed environment was hidden: state=%s reason=%s", got.State, got.Reason)
	}
	logs, err := q.Logs(j.ID, 0)
	if err != nil || len(logs) != 1 || !strings.Contains(logs[0].Text, "compiler output") {
		t.Fatalf("build log not surfaced: %v %v", logs, err)
	}
}
