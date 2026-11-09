package myce

import (
	"context"

	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	m.Environments, m.EnvironmentReports, _ = rt.report()
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
	m.Environments, m.EnvironmentReports, _ = rt.report()
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
	m.Environments, m.EnvironmentReports, _ = rt.report()
	if len(m.EnvironmentReports) != 1 || m.EnvironmentReports[0].State != "building" || m.EnvironmentReports[0].Attempt != next.Attempt {
		t.Fatal("new attempt reused old completion", m.EnvironmentReports)
	}
}

func TestEnvironmentRetiredPurgeProtectsReferences(t *testing.T) {
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 8 << 20}
	r := binaryRecipe(t, "fake")
	path, release, err := c.Acquire(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { release() }()
	if err = os.WriteFile(filepath.Join(path, "payload"), make([]byte, 1<<20), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Unix(1, 0)
	if err = os.Chtimes(filepath.Join(path, "ready.json"), old, old); err != nil {
		t.Fatal(err)
	}
	if err = c.trash(path); err != nil {
		t.Fatal(err)
	}
	// A preparation can hold a build reference independently of an attempt.
	key := strings.Repeat("b", 64)
	build, err := c.lock(context.Background(), key+".build", syscall.LOCK_EX)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unlockEnvironment(build) }()
	preparing := filepath.Join(c.Root, key+".partial")
	if err = os.Mkdir(preparing, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(preparing, "payload"), make([]byte, 1<<20), 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.trash(preparing); err != nil {
		t.Fatal(err)
	}
	unreferenced := filepath.Join(c.Root, strings.Repeat("c", 64))
	if err = os.Mkdir(unreferenced, 0700); err != nil {
		t.Fatal(err)
	}
	const freed = int64(1 << 20)
	if err = os.WriteFile(filepath.Join(unreferenced, "payload"), make([]byte, freed), 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.trash(unreferenced); err != nil {
		t.Fatal(err)
	}
	trash := filepath.Join(c.Root, ".Trash")
	before, err := environmentSize(trash)
	if err != nil {
		t.Fatal(err)
	}
	allocatedBefore, err := c.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	c.Limit = 1
	err = c.admit("", 0)
	after, sizeErr := environmentSize(trash)
	if sizeErr != nil || before-after != freed {
		t.Fatalf("retired bytes freed = %d, want %d; err=%v", before-after, freed, sizeErr)
	}
	if err == nil || !strings.Contains(err.Error(), "in-use entries protected") {
		t.Fatalf("referenced retired bytes bypassed cache limit: %v", err)
	}
	release()
	release = func() {}
	remaining, err := environmentSize(trash)
	if err != nil || remaining != 1<<20 {
		t.Fatalf("attempt release must purge only its retired bytes: remaining=%d err=%v", remaining, err)
	}
	unlockEnvironment(build)
	build = nil
	if err = c.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	remaining, err = environmentSize(trash)
	if err != nil || remaining != 0 {
		t.Fatalf("released preparation retained %d bytes: %v", remaining, err)
	}
	entries, err := os.ReadDir(trash)
	if err != nil || len(entries) != 0 {
		t.Fatalf("released retired entries remain: %v; err=%v", entries, err)
	}
	allocatedAfter, err := c.DiskUsage()
	if err != nil || allocatedAfter >= allocatedBefore {
		t.Fatalf("purge did not free allocated cache bytes: before=%d after=%d err=%v", allocatedBefore, allocatedAfter, err)
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

func testServerForEnvironments(t *testing.T, q *Queue) string {
	t.Helper()
	s := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(s.Close)
	return s.URL
}

func TestEnvironmentRealUVRelocation(t *testing.T) {
	if _, e := exec.LookPath("uv"); e != nil {
		t.Skip("uv not installed; fake-runner tests remain portable")
	}
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Skip("Python not installed")
	}
	t.Setenv("UV_OFFLINE", "1")
	dir := t.TempDir()
	lock := filepath.Join(dir, "requirements.txt")
	if e = os.WriteFile(lock, nil, 0600); e != nil {
		t.Fatal(e)
	}
	r, e := snapshotEnvironment(EnvironmentRecipe{LockFile: lock, Python: python}, "fake")
	if e != nil {
		t.Fatal(e)
	}
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 20 << 20, Clock: RealClock{}}
	path, release, e := c.Acquire(context.Background(), r)
	if e != nil {
		log, _ := os.ReadFile(filepath.Join(c.Root, r.Key+".build.log"))
		t.Fatalf("%v\n%s", e, log)
	}
	defer release()
	output, e := exec.Command(filepath.Join(path, "venv", "bin", "python"), "-c", "import sys; print(sys.prefix)").CombinedOutput()
	if e != nil || strings.TrimSpace(string(output)) != filepath.Join(path, "venv") {
		t.Fatalf("relocated Python %s %v", output, e)
	}
}

func TestEnvironmentProductionBuildDoesNotStarveBehindReadyExperiments(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"production-tools": {"fake": r}}})
	production := spec("production")
	production.Environment.Name = "production-tools"
	j := submit(t, q, production)
	submit(t, q, spec("experiment"))
	m := machine("worker", "fake")
	m.EnvironmentBuilder = true
	reply := check(t, q, m, nil)
	if reply.Job != nil || len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key {
		t.Fatal("ready experiment starved production setup", reply)
	}
	m.Environments = []string{r.Key}
	reply = check(t, q, m, nil)
	if reply.Job == nil || reply.Job.ID != j.ID {
		t.Fatal("production did not start after build", reply)
	}
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
