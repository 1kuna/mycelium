package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func syncFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	q, c := fixture(t)
	models := []Model{}
	for _, name := range []string{"a", "b"} {
		models = append(models, Model{Name: name, Path: name, Engine: "fake", SizeBytes: 40_000_000_000, SHA256: strings.Repeat("a", 64), MemoryGB: 8, Platforms: []string{"fake", "cuda"}})
	}
	if e := q.ConfigureModels(ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: models}); e != nil {
		t.Fatal(e)
	}
	m := machine("one", "fake")
	m.ModelConfigHash = modelConfigHash(q.models)
	m.ModelURL = "http://127.0.0.1:1234"
	check(t, q, m, nil)
	return q, c, m
}
func TestSyncTargetsCancelAndPersistence(t *testing.T) {
	for _, target := range []string{"one", "fake", "all"} {
		t.Run(target, func(t *testing.T) {
			q, _, m := syncFixture(t)
			if e := q.SyncModel(SyncRequest{Model: "a", To: target}); e != nil {
				t.Fatal(e)
			}
			reply := check(t, q, m, nil)
			if len(reply.CopyPlan) != 1 || reply.CopyPlan[0].Model != "a" {
				t.Fatal(reply)
			}
			db := filepath.Join(t.TempDir(), "persist.db")
			other, e := Open(db, q.root, q.clock)
			if e != nil {
				t.Fatal(e)
			}
			if e = other.ConfigureModels(q.models); e != nil {
				t.Fatal(e)
			}
			check(t, other, m, nil)
			if e = other.SyncModel(SyncRequest{Model: "a", To: target}); e != nil {
				t.Fatal(e)
			}
			other.Close()
			other, e = Open(db, q.root, q.clock)
			if e != nil {
				t.Fatal(e)
			}
			defer other.Close()
			if e = other.ConfigureModels(q.models); e != nil {
				t.Fatal(e)
			}
			if len(check(t, other, m, nil).CopyPlan) != 1 {
				t.Fatal("restart lost sync")
			}
			if e = q.SyncModel(SyncRequest{Model: "a", To: target, Cancel: true}); e != nil {
				t.Fatal(e)
			}
			if len(check(t, q, m, nil).CopyPlan) != 0 {
				t.Fatal("cancel ignored")
			}
		})
	}
}
func TestPinsPrecopyEveryLandingMachineOutsideWindow(t *testing.T) {
	q, _, m := syncFixture(t)
	p := ModelPin{Project: "atlas", Model: "a", Platform: "fake", Start: "01:00", End: "02:00", Timezone: "UTC"}
	if e := q.Pin(p); e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{"one", "two"} {
		m.ID = id
		m.Session = "session-" + id
		reply := check(t, q, m, nil)
		if len(reply.CopyPlan) != 1 || !contains(reply.CachePins, "a") {
			t.Fatal("standby not warmed/protected", reply)
		}
	}
	m.ID = "cuda"
	m.GPU = "cuda"
	m.Session = "cuda-session"
	if len(check(t, q, m, nil).CopyPlan) != 0 {
		t.Fatal("pin copied onto wrong platform")
	}
}
func TestIdlePrefetchMostNeededAndPauseForWork(t *testing.T) {
	q, _, m := syncFixture(t)
	// Missing environments prevent these jobs starting; their model demand is still useful.
	for _, name := range []string{"a", "b", "b"} {
		s := spec("experiment")
		s.Needs.Model = name
		s.Environment.Name = "not-ready"
		submit(t, q, s)
	}
	reply := check(t, q, m, nil)
	if len(reply.CopyPlan) != 2 || reply.CopyPlan[0].Model != "b" || reply.CopyPlan[0].Score != 2 {
		t.Fatal(reply.CopyPlan)
	}
	real := submit(t, q, spec("production"))
	reply = check(t, q, m, nil)
	if reply.Job == nil || reply.Job.ID != real.ID || len(reply.CopyPlan) != 0 {
		t.Fatal("copy competed with batch", reply)
	}
}
func TestLoadEstimatesAndMachinePlacement(t *testing.T) {
	q, _, slow := syncFixture(t)
	slow.CopyBytesPerSecond = 40_000_000
	fast := slow
	fast.ID = "two"
	fast.Session = "session-two"
	fast.CopyBytesPerSecond = 80_000_000
	check(t, q, slow, nil)
	check(t, q, fast, nil)
	if q.loadSeconds("a", &slow) != 1000 || q.loadSeconds("a", &fast) != 500 {
		t.Fatal("speed ignored")
	}
	fast.CopyReports = []CopyReport{{Model: "a", Bytes: 20_000_000_000, Total: 40_000_000_000, State: "paused"}}
	if q.loadSeconds("a", &fast) != 250 {
		t.Fatal("resumed bytes ignored")
	}
	check(t, q, fast, nil)
	s := spec("production")
	s.Needs.Model = "a"
	j := submit(t, q, s)
	if check(t, q, slow, nil).Job != nil {
		t.Fatal("slow worker took model work")
	}
	r := check(t, q, fast, nil).Job
	if r == nil || r.ID != j.ID {
		t.Fatal("fast worker not selected")
	}
	finish(t, q, r, true)
	slow.CachedModels = []string{"a"}
	check(t, q, slow, nil)
	c := &ModelCall{Model: "a", Project: "atlas", Priority: "production"}
	if e := q.addCall(c); e != nil {
		t.Fatal(e)
	}
	if q.s.ModelSlots[slotKey(slow.ID, "a")] == nil {
		t.Fatal("cached model not preferred")
	}
}
func TestCacheResumeSpeedAndVerifiedPublication(t *testing.T) {
	c, model := cacheFixture(t)
	c.Limit = 4 * 1024 * 1024
	c.Free = func(string) (int64, error) { return 10 * 1024 * 1024, nil }
	m := model("big", strings.Repeat("x", 3*1024*1024))
	ctx, cancel := context.WithCancel(context.Background())
	chunks := 0
	c.beforeChunk = func(context.Context) {
		chunks++
		c.Clock.(*fakeClock).Advance(time.Second)
		if chunks == 2 {
			cancel()
		}
	}
	if _, e := c.Precopy(ctx, m, 1); e == nil {
		t.Fatal("copy not interrupted")
	}
	reports, speed := c.CopyReports()
	if len(reports) != 1 || reports[0].State != "paused" || reports[0].Bytes != 1024*1024 || speed != 524288 {
		t.Fatal(reports, speed)
	}
	if names, _, _ := c.Report([]Model{m}); len(names) != 0 {
		t.Fatal("partial reported as cached")
	}
	restarted := &ModelCache{Root: c.Root, NASRoot: c.NASRoot, Limit: c.Limit, Clock: c.Clock, Free: c.Free}
	if e := restarted.init(); e != nil {
		t.Fatal(e)
	}
	restarted.beforeChunk = func(context.Context) { c.Clock.(*fakeClock).Advance(time.Second) }
	path, e := restarted.Precopy(context.Background(), m, 1)
	if e != nil {
		t.Fatal(e)
	}
	if e = verifyModel(path, m); e != nil {
		t.Fatal(e)
	}
	reports, speed = restarted.CopyReports()
	if reports[0].Bytes != m.SizeBytes || reports[0].State != "ready" || speed != float64(2*1024*1024)/3 {
		t.Fatal(reports, speed)
	}
}
func TestCacheDoesNotTrustUnverifiedSameSizeCopy(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", "good")
	if e := os.WriteFile(filepath.Join(c.Root, cacheKey(m)), []byte("evil"), 0600); e != nil {
		t.Fatal(e)
	}
	c.ValidateInventory([]Model{m})
	if names, _, _ := c.Report([]Model{m}); len(names) != 0 {
		t.Fatal("unverified file advertised")
	}
	path, e := c.Prepare(context.Background(), m)
	if e != nil {
		t.Fatal(e)
	}
	if e = verifyModel(path, m); e != nil {
		t.Fatal("corrupt cache used", e)
	}
}
func TestPrecopyNeverEvictsPinnedLoadedOrMoreUsed(t *testing.T) {
	for _, kind := range []string{"pinned", "loaded", "popular"} {
		t.Run(kind, func(t *testing.T) {
			c, model := cacheFixture(t)
			a, b := model("a", "aaaaaaaa"), model("b", "bbbbbbbb")
			if _, e := c.Prepare(context.Background(), a); e != nil {
				t.Fatal(e)
			}
			if kind == "popular" {
				c.Usage = func(key string) int {
					if key == cacheKey(a) {
						return 2
					}
					return 0
				}
			} else {
				c.Protected = func(key string) bool { return key == cacheKey(a) }
			}
			if _, e := c.Precopy(context.Background(), b, 1); e == nil {
				t.Fatal("protected copy evicted")
			}
			if e := verifyModel(filepath.Join(c.Root, cacheKey(a)), a); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestConcurrentCopiesReserveFullCacheAndDiskBytes(t *testing.T) {
	for _, diskLimited := range []bool{false, true} {
		t.Run(fmt.Sprint(diskLimited), func(t *testing.T) {
			c, model := cacheFixture(t)
			c.Parallel = 2
			if diskLimited {
				c.Limit = 100
				c.Free = func(string) (int64, error) { return 8, nil }
			}
			a, b := model("a", "aaaaaaaa"), model("b", "bbbbbbbb")
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			c.beforeChunk = func(context.Context) { once.Do(func() { close(entered); <-release }) }
			done := make(chan error, 1)
			go func() { _, e := c.Prepare(context.Background(), a); done <- e }()
			<-entered
			if _, e := c.Prepare(context.Background(), b); e == nil {
				t.Fatal("concurrent copies overspent")
			}
			close(release)
			if e := <-done; e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestWorkerPausesCopyBeforeRealWork(t *testing.T) {
	for _, kind := range []string{"assignment", "model-call"} {
		t.Run(kind, func(t *testing.T) {
			c, model := cacheFixture(t)
			c.Limit = 4 * 1024 * 1024
			c.Free = func(string) (int64, error) { return 10 * 1024 * 1024, nil }
			m := model("m", strings.Repeat("x", 3*1024*1024))
			entered, release := make(chan struct{}), make(chan struct{})
			chunks := 0
			c.beforeChunk = func(context.Context) {
				chunks++
				if chunks == 2 {
					close(entered)
					<-release
				}
			}
			rt := &modelRuntime{cache: c, models: map[string]*workerModel{}, copies: map[string]*workerCopy{}, copyParallel: 1, config: ModelConfig{Models: []Model{m}}}
			rt.syncCopies([]CopyTask{{Model: "m", Score: 1}}, nil, nil, false)
			<-entered
			if kind == "assignment" {
				rt.syncCopies(nil, nil, nil, true)
			} else {
				rt.token = "test"
				rt.session = "test-session"
				rt.models["ready"] = &workerModel{state: "ready", engine: &FakeEngine{Model: "ready"}}
				req := httptest.NewRequest("POST", "/v1/completions", strings.NewReader(`{"model":"ready"}`))
				req.Header.Set("Authorization", "Bearer test")
				req.Header.Set("X-Myce-Session", rt.session)
				response := httptest.NewRecorder()
				rt.serve(response, req)
				if response.Code != 200 {
					t.Fatal(response.Code, response.Body.String())
				}
			}
			close(release)
			rt.pauseCopies()
			reports, _ := c.CopyReports()
			if len(reports) != 1 || reports[0].State != "paused" || reports[0].Bytes != 1024*1024 {
				t.Fatal(reports)
			}
			rt.wg.Wait()
		})
	}
}

func TestResumeRejectsCorruptPrefix(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", "good")
	os.WriteFile(filepath.Join(c.Root, cacheKey(m)+".partial"), []byte("ev"), 0600)
	if _, e := c.Precopy(context.Background(), m, 1); e == nil {
		t.Fatal("corrupt prefix published")
	}
	if names, _, _ := c.Report([]Model{m}); len(names) > 0 {
		t.Fatal(names)
	}
	p, e := c.Prepare(context.Background(), m)
	if e != nil {
		t.Fatal(e)
	}
	data, _ := os.ReadFile(p)
	if fmt.Sprintf("%x", sha256.Sum256(data)) != m.SHA256 {
		t.Fatal("bad resumed hash")
	}
}

func TestPreemptedNonresumableRestartDoesNotChargeLoss(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Resumable = false
	j := submit(t, q, s)
	m := machine("one", "fake")
	old := check(t, q, m, nil).Job
	q.s.Jobs[j.ID].Losses = 2
	submit(t, q, spec("production"))
	m.Session = "new-session"
	_, e := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: old.Session})
	if e != nil {
		t.Fatal(e)
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || got.Losses != 2 {
		t.Fatal("preemption consumed infrastructure retry", got)
	}
	if e = q.Complete(result(old, true)); e != ErrStale {
		t.Fatal("stale preempted result accepted", e)
	}
}

func TestCancellationSurvivesFreshWorkerRestart(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	old := check(t, q, m, nil).Job
	if e := q.Cancel(j.ID); e != nil {
		t.Fatal(e)
	}
	m.Session = "new-session"
	reply, e := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: old.Session})
	if e != nil {
		t.Fatal(e)
	}
	if reply.Job != nil || job(t, q, j.ID).State != "cancelled" {
		t.Fatal("restart revived cancelled execution", reply)
	}
}

func TestSyncUsesRealAPIAndWorker(t *testing.T) {
	q, _ := fixture(t)
	dir := t.TempDir()
	nas := filepath.Join(dir, "nas")
	os.Mkdir(nas, 0700)
	data := []byte("verified local weights")
	os.WriteFile(filepath.Join(nas, "m"), data, 0600)
	model := Model{Name: "m", Path: "m", Engine: "fake", MemoryGB: 1, Platforms: []string{"fake"}, SizeBytes: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	cfg := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{model}}
	if e := q.ConfigureModels(cfg); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	if e := client.Call(context.Background(), "POST", "/api/models/sync", SyncRequest{Model: "m", To: "all"}, nil); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	w := Worker{Client: client, Machine: machine("sync-worker", "fake"), Clock: RealClock{}, Interval: 20 * time.Millisecond, StateDir: filepath.Join(dir, "worker"), ModelRoot: nas, CacheLimit: 1024, ModelConfig: cfg}
	go func() { done <- w.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case e := <-done:
			t.Fatal("worker exited before copying", e)
		case <-deadline:
			t.Fatal("sync did not reach verified cache")
		case <-tick.C:
			snap, e := q.Snapshot()
			if e != nil {
				t.Fatal(e)
			}
			if len(snap.Machines) == 1 && contains(snap.Machines[0].CachedModels, "m") {
				if len(snap.Machines[0].CopyReports) != 1 || snap.Machines[0].CopyReports[0].State != "ready" || snap.Machines[0].CopyBytesPerSecond <= 0 {
					t.Fatal("no progress/speed", snap.Machines[0])
				}
				cancel()
				if e := <-done; e != nil {
					t.Fatal(e)
				}
				if e := verifyModel(filepath.Join(dir, "worker", "model-cache", cacheKey(model)), model); e != nil {
					t.Fatal(e)
				}
				return
			}
		}
	}
}

func TestParallelCopiesActuallyRunTogether(t *testing.T) {
	c, model := cacheFixture(t)
	c.Limit = 16
	c.Parallel = 2
	a, b := model("a", "aaaaaaaa"), model("b", "bbbbbbbb")
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	c.beforeChunk = func(context.Context) {
		if calls.Add(1) <= 2 {
			entered <- struct{}{}
			<-release
		}
	}
	done := make(chan error, 2)
	for _, m := range []Model{a, b} {
		go func() { _, e := c.Prepare(context.Background(), m); done <- e }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("parallel limit still serialized copies")
		}
	}
	close(release)
	for range 2 {
		if e := <-done; e != nil {
			t.Fatal(e)
		}
	}
	if names, free, e := c.Report([]Model{a, b}); e != nil || len(names) != 2 || free != 0 {
		t.Fatal(names, free, e)
	}
}

func TestOversizedMasterNeverExceedsCopyReservation(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", strings.Repeat("x", 1024))
	m.SizeBytes = 8
	if _, e := c.Prepare(context.Background(), m); e == nil {
		t.Fatal("oversize accepted")
	}
	trash, _ := filepath.Glob(filepath.Join(c.NASRoot, ".Trash", "myce-model-cache", "*"))
	for _, path := range trash {
		st, e := os.Stat(path)
		if e != nil || st.Size() > m.SizeBytes {
			t.Fatal("copy wrote beyond reservation", e)
		}
	}
}

func TestRecentModelUseAndActiveBatchProtection(t *testing.T) {
	q, c, m := syncFixture(t)
	s := spec("experiment")
	s.Needs.Model = "a"
	j := submit(t, q, s)
	r := check(t, q, m, nil).Job
	plan := check(t, q, m, r)
	if !contains(plan.CachePins, "a") || len(plan.CopyPlan) > 0 {
		t.Fatal("active batch cache not protected", plan)
	}
	c.Advance(25 * time.Hour)
	// A long-running job's finish is recent even though its submission is old.
	if e := q.Complete(result(r, true)); e != nil {
		t.Fatal(e)
	}
	if got := job(t, q, j.ID); got.State != "succeeded" {
		t.Fatal(got)
	}
	if len(check(t, q, m, nil).CopyPlan) != 1 {
		t.Fatal("recently completed model forgotten")
	}
	c.Advance(25 * time.Hour)
	if len(check(t, q, m, nil).CopyPlan) != 0 {
		t.Fatal("old use never expired")
	}
}

func TestBatchPlacementPrefersLoadedOverCached(t *testing.T) {
	q, _, cached := syncFixture(t)
	cached.CachedModels = []string{"a"}
	loaded := cached
	loaded.ID, loaded.Session = "loaded", "loaded-session"
	loaded.Models = []string{"a"}
	check(t, q, cached, nil)
	// An external loaded-model hint is valid for batch work; no engine is required.
	loaded.ModelURL = ""
	check(t, q, loaded, nil)
	s := spec("production")
	s.Needs.Model = "a"
	j := submit(t, q, s)
	if check(t, q, cached, nil).Job != nil {
		t.Fatal("cached worker took work from loaded worker")
	}
	if got := check(t, q, loaded, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("loaded worker did not receive work", got)
	}
}
