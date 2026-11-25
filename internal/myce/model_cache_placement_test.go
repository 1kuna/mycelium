package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func modelCachePlacementFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
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

func TestLoadEstimatesAndMachinePlacement(t *testing.T) {
	q, _, slow := modelCachePlacementFixture(t)
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
	if _, e := c.Prepare(ctx, m); e == nil {
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
	path, e := restarted.Prepare(context.Background(), m)
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

func TestResumeRejectsCorruptPrefix(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", "good")
	os.WriteFile(filepath.Join(c.Root, cacheKey(m)+".partial"), []byte("ev"), 0600)
	if _, e := c.Prepare(context.Background(), m); e == nil {
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
		case <-time.After(testWaitTimeout):
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

func TestBatchPlacementPrefersLoadedOverCached(t *testing.T) {
	q, _, cached := modelCachePlacementFixture(t)
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
