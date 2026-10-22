package myce

import (
	"context"
	"errors"
	"fmt"
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
func TestEnvironmentHashInputs(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "requirements.txt")
	os.WriteFile(p, []byte("torch==2.0\n"), 0600)
	a, e := snapshotEnvironment(EnvironmentRecipe{LockFile: p, Python: "3.12"}, "cuda")
	if e != nil {
		t.Fatal(e)
	}
	other := t.TempDir()
	q := filepath.Join(other, "requirements.txt")
	os.WriteFile(q, a.Files["requirements.txt"], 0600)
	b, _ := snapshotEnvironment(EnvironmentRecipe{LockFile: q, Python: "3.12"}, "cuda")
	if a.Key != b.Key {
		t.Fatal("mount paths changed identity")
	}
	b, _ = snapshotEnvironment(EnvironmentRecipe{LockFile: q, Python: "3.12"}, "xpu")
	if a.Key == b.Key {
		t.Fatal("platform omitted")
	}
	b, _ = snapshotEnvironment(EnvironmentRecipe{LockFile: q, Python: "3.11"}, "cuda")
	if a.Key == b.Key {
		t.Fatal("Python omitted")
	}
	os.WriteFile(q, []byte("torch==2.1"), 0600)
	b, _ = snapshotEnvironment(EnvironmentRecipe{LockFile: q, Python: "3.12"}, "cuda")
	if a.Key == b.Key {
		t.Fatal("lock contents omitted")
	}
}
func TestEnvironmentBuildLockReuseAndCleanup(t *testing.T) {
	r := binaryRecipe(t, "fake")
	r.Build = []string{"pretend-build"}
	r.Key = strings.Repeat("a", 64)
	var count atomic.Int32
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}, Run: func(ctx context.Context, args []string, dir string, env []string, out, _ io.Writer) error {
		count.Add(1)
		time.Sleep(20 * time.Millisecond)
		return nil
	}}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release, e := c.Acquire(context.Background(), r)
			if e != nil {
				t.Error(e)
			}
			if release != nil {
				release()
			}
		}()
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatalf("built %d times", count.Load())
	}
	partial := filepath.Join(c.Root, strings.Repeat("b", 64)+".partial")
	os.Mkdir(partial, 0700)
	os.WriteFile(filepath.Join(partial, "half-written"), []byte("preserve me"), 0600)
	if e := c.Cleanup(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(partial); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("interrupted build remained")
	}
	trash, _ := os.ReadDir(filepath.Join(c.Root, ".Trash"))
	if len(trash) != 0 {
		t.Fatal("interrupted build retained")
	}
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
	sizeA, _ := environmentSize(pa)
	sizeB, _ := environmentSize(pb)
	c.Limit = sizeA + sizeB + 20
	_, rd, e := c.Acquire(context.Background(), d)
	if e != nil {
		t.Fatal(e)
	}
	rd()
	if !c.ready(a.Key) || c.ready(b.Key) || !c.ready(d.Key) {
		t.Fatal("evicted in-use entry instead of oldest available entry")
	}
	release()
	trash, _ := os.ReadDir(filepath.Join(c.Root, ".Trash"))
	if len(trash) != 0 {
		t.Fatal("eviction did not free retired bytes")
	}
}
func TestEnvironmentPlacementAndFiltering(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "cuda")
	j := &Job{State: "queued", EnvironmentRecipes: map[string]EnvironmentRecipe{"cuda": r}}
	j.Spec.Environment.Name = "torch-cuda"
	j.Spec.Priority = "experiment"
	j.Spec.Needs.Platform.Any = true
	j.Spec.Needs.MemoryGB = 1
	j.Spec.Needs.Model = "model"
	profile := machine("builder", "cuda")
	m := &profile
	m.EnvironmentBuilder = true
	if !fits(j, m, 8) || environmentReady(j, m) {
		t.Fatal("cold builder excluded or ready")
	}
	q.s.Jobs["j"] = j
	if q.pick(m) != nil {
		t.Fatal("assigned before build")
	}
	m.Environments = []string{r.Key}
	m.Models = []string{"model"}
	if affinity(j, m) != 2 {
		t.Fatal("both-ready affinity")
	}
	m.Models = nil
	if affinity(j, m) != 1 {
		t.Fatal("either-ready affinity")
	}
	m.Environments = nil
	if affinity(j, m) != 0 {
		t.Fatal("cold affinity")
	}
	m.GPU = "metal"
	if fits(j, m, 8) {
		t.Fatal("any-platform bypassed recipe filtering")
	}
	reason := waitingReason(j, map[string]*Machine{"mac": m}, nil)
	if !strings.Contains(reason, "torch-cuda") {
		t.Fatal(reason)
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
	_, e = q.Checkin(Checkin{Machine: Machine{CapabilityProfile: machine("profile", "fake").CapabilityProfile, ID: "worker", Session: "session", GPU: "fake", FreeMemoryGB: 64, EnvironmentBuilder: true, EnvironmentReports: []EnvironmentReport{{Key: r.Key, State: "failed", Error: "build exit 9", Log: "compiler output\n"}}}})
	if e != nil {
		t.Fatal(e)
	}
	_ = clock
	logs, e := q.Logs(j.ID, 0)
	if e != nil || len(logs) != 1 || !strings.Contains(logs[0].Text, "compiler output") {
		t.Fatalf("logs %v %v", logs, e)
	}
	snap, _ := q.Snapshot()
	for _, j := range snap.Jobs {
		if j.State != "failed" {
			t.Fatal(j.State)
		}
	}
}
func TestEnvironmentWorkerBuildAndExecute(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"gpu-tools": {"fake": r}}})
	server := testServerForEnvironments(t, q)
	s := spec("experiment")
	s.Environment.Name = "gpu-tools"
	s.Run.Command = []string{"gpu-task"}
	j, e := q.Submit(s)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	w := Worker{StateDir: newTestStateRoot(t), Client: Client{URL: server, Token: "test"}, Machine: Machine{CapabilityProfile: machine("profile", "fake").CapabilityProfile, ID: "env-worker", GPU: "fake", FreeMemoryGB: 64}, Interval: 10 * time.Millisecond}
	go func() { done <- w.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		snap, e := q.Snapshot()
		if e != nil {
			t.Fatal(e)
		}
		finished := false
		for _, job := range snap.Jobs {
			if job.ID == j.ID && terminal(job.State) {
				if job.State != "succeeded" {
					t.Fatalf("%s %s", job.State, job.Reason)
				}
				finished = true
			}
		}
		if finished {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("environment job timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	logs, _ := q.Logs(j.ID, 0)
	if len(logs) == 0 || !strings.Contains(fmt.Sprint(logs), "environment-command") {
		t.Fatal(logs)
	}
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

func TestEnvironmentBuildPlacementUsesMeasurements(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tool": {"fake": r}}})
	s := spec("experiment")
	s.Environment.Name = "tool"
	submit(t, q, s)
	slow := machine("slow", "fake")
	slow.EnvironmentBuilder = true
	slow.EnvironmentBuildSeconds = map[string]float64{r.Key: 120}
	fast := machine("fast", "fake")
	fast.EnvironmentBuilder = true
	fast.EnvironmentBuildSeconds = map[string]float64{r.Key: 10}
	check(t, q, slow, nil)
	check(t, q, fast, nil)
	if len(check(t, q, slow, nil).EnvironmentPlan) != 0 {
		t.Fatal("slower cold worker selected")
	}
	if len(check(t, q, fast, nil).EnvironmentPlan) != 1 {
		t.Fatal("faster builder not selected")
	}
}

func TestEnvironmentBuildLogsAreCompleteAndDeduplicated(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tool": {"fake": r}}})
	s := spec("experiment")
	s.Environment.Name = "tool"
	j := submit(t, q, s)
	m := machine("builder", "fake")
	m.EnvironmentBuilder = true
	check(t, q, m, nil)
	want := strings.Repeat("log line\n", 10000)
	for offset := 0; offset < len(want); offset += 16384 {
		end := min(offset+16384, len(want))
		l := EnvironmentLog{Machine: m.ID, Session: m.Session, Key: r.Key, Offset: int64(offset + 1), Text: want[offset:end]}
		if e := q.AppendEnvironmentLog(l); e != nil {
			t.Fatal(e)
		}
		if e := q.AppendEnvironmentLog(l); e != nil {
			t.Fatal(e)
		}
	}
	m.EnvironmentReports = []EnvironmentReport{{Key: r.Key, State: "failed", Error: "compiler failed"}}
	check(t, q, m, nil)
	logs, e := q.Logs(j.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	var got strings.Builder
	for _, l := range logs {
		got.WriteString(l.Text)
	}
	if !strings.HasPrefix(got.String(), want) || strings.Count(got.String(), "log line\n") != 10000 {
		t.Fatal("lost or duplicated build output")
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

func TestEnvironmentOversizedBuildFailsAndIsPurged(t *testing.T) {
	r, e := snapshotEnvironment(EnvironmentRecipe{Build: []string{"sh", "-c", `dd if=/dev/zero of="$1/bin/too-big" bs=1024 count=8 2>/dev/null`, "build", "{root}"}}, "fake")
	if e != nil {
		t.Fatal(e)
	}
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1024, Clock: RealClock{}}
	if _, release, e := c.Acquire(context.Background(), r); e == nil {
		if release != nil {
			release()
		}
		t.Fatal("oversized cache accepted")
	}
	if c.ready(r.Key) {
		t.Fatal("oversized build published")
	}
	if e := c.Cleanup(context.Background()); e != nil {
		t.Fatal(e)
	}
	entries, _ := os.ReadDir(filepath.Join(c.Root, ".Trash"))
	if len(entries) != 0 {
		t.Fatal("failed build retained retired bytes")
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
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}, Run: func(_ context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
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
