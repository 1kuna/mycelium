package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWorkerDiskBudgetSharedVolumeReservations(t *testing.T) {
	_, clock := fixture(t)
	const total = uint64(100 << 30)
	floor := diskFreeFloor(total)
	b := newWorkerDiskBudget(clock, func(path string) (DiskReading, error) {
		volume := "shared"
		if path == "separate" {
			volume = "separate"
		}
		return DiskReading{Volume: volume, TotalBytes: total, FreeBytes: floor + 100}, nil
	})
	ctx := context.Background()
	start := make(chan struct{})
	results := make(chan func(), 2)
	var wg sync.WaitGroup
	for _, root := range []string{"model", "environment"} {
		wg.Add(1)
		go func(root string) {
			defer wg.Done()
			<-start
			release, err := b.reserve(ctx, root, 60, 0)
			if err != nil {
				results <- nil
			} else {
				results <- release
			}
		}(root)
	}
	close(start)
	wg.Wait()
	close(results)
	var releases []func()
	for release := range results {
		if release != nil {
			releases = append(releases, release)
		}
	}
	if len(releases) != 1 {
		t.Fatal("same free blocks promised twice", len(releases))
	}
	other, err := b.reserve(ctx, "separate", 60, 0)
	if err != nil {
		t.Fatal(err)
	}
	other()
	releases[0]()
	releases[0]()
	policy, err := b.reserve(ctx, "model", 10, floor+80)
	if err != nil {
		t.Fatal(err)
	}
	if release, err := b.reserve(ctx, "environment", 20, 0); err == nil {
		release()
		t.Fatal("concurrent writer ignored active model floor")
	} else {
		var capacity *stagingCapacityError
		if !errors.As(err, &capacity) || capacity.capacity != "disk_free_floor" || capacity.required != 20 || capacity.available != 10 {
			t.Fatal("pressure lost structured capacity evidence", err)
		}
	}
	policy()
	if release, err := b.reserve(ctx, "environment", 90, 0); err != nil {
		t.Fatal("release did not return headroom", err)
	} else {
		release()
	}
}

func TestWorkerDiskBudgetQueuedDemandReclaimsAndAdmits(t *testing.T) {
	q, clock := fixture(t)
	c, model := cacheFixture(t)
	c.Clock = clock
	c.Limit = 1 << 20
	c.Free = func(string) (int64, error) { return 1 << 30, nil }
	m := model("rebuildable", strings.Repeat("x", 16384))
	path, err := c.Prepare(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	before, err := collectionAllocatedBytes(context.Background(), c.Root)
	if err != nil {
		t.Fatal(err)
	}
	const total = uint64(100 << 30)
	floor := diskFreeFloor(total)
	probe := func(string) (DiskReading, error) {
		n, err := collectionAllocatedBytes(context.Background(), c.Root)
		return DiskReading{Volume: "local", TotalBytes: total, FreeBytes: floor + uint64(max(0, before-n))}, err
	}
	b := newWorkerDiskBudget(clock, probe)
	b.inventoryDir = c.Root
	c.budget = b
	b.add(diskBudgetRoot{name: "models", path: c.Root, candidates: c.budgetCandidates})
	worker := machine("disk-demand", "fake")
	worker.DiskVolumes = []DiskVolume{{Roots: []string{"data"}, TotalBytes: total, FreeBytes: floor, SampledAt: clock.Now()}}
	check(t, q, worker, nil)
	s := spec("experiment")
	s.Disk = &DiskNeeds{OutputBytes: 8192}
	j := submit(t, q, s)
	if got := check(t, q, worker, nil).Job; got != nil {
		t.Fatal("job not held for disk", got)
	}
	if got := q.workerDiskDemand(context.Background(), workerDiskDemandRequest{Machine: worker.ID, Session: "stale"}); len(got) != 0 {
		t.Fatal("stale worker got demand", got)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{Machine: worker, Client: Client{URL: server.URL, Token: "test"}, diskBudget: b}
	w.reclaimPendingDisk(context.Background())
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("queued demand did not reclaim inactive model", err)
	}
	r, err := probe(c.Root)
	if err != nil {
		t.Fatal(err)
	}
	worker.DiskVolumes[0].FreeBytes = r.FreeBytes
	if got := check(t, q, worker, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("cleanup did not admit pending work", got)
	}
}

// Actual allocated bytes drive the fake volume. Deleting one hardlink must not
// satisfy pending demand until every retained link to those blocks is gone.
func TestWorkerDiskBudgetHardlinksAndForeignGrowth(t *testing.T) {
	_, clock := fixture(t)
	root := t.TempDir()
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	if err := os.WriteFile(first, []byte(strings.Repeat("x", 16384)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(first, second); err != nil {
		t.Fatal(err)
	}
	used, err := collectionAllocatedBytes(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	const total = uint64(100 << 30)
	floor := diskFreeFloor(total)
	foreign := uint64(0)
	probe := func(string) (DiskReading, error) {
		n, err := collectionAllocatedBytes(context.Background(), root)
		return DiskReading{Volume: "local", TotalBytes: total, FreeBytes: floor + uint64(used-n) + 4096 - foreign}, err
	}
	b := newWorkerDiskBudget(clock, probe)
	b.add(diskBudgetRoot{name: "models", path: root, candidates: func(context.Context) ([]diskBudgetCandidate, error) {
		return []diskBudgetCandidate{
			{path: first, used: clock.Now().Add(-time.Hour), evict: func(context.Context) (bool, error) { return true, os.Remove(first) }},
			{path: second, used: clock.Now(), evict: func(context.Context) (bool, error) { return true, os.Remove(second) }},
		}, nil
	}})
	release, err := b.reserve(context.Background(), root, 8192, 0)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if _, err = os.Stat(second); !os.IsNotExist(err) {
		t.Fatal("hardlink mistaken for reclaimed blocks", err)
	}
	b.collect(context.Background())
	clock.Advance(time.Minute)
	foreign = 2048
	view := b.collect(context.Background())
	if len(view) != 1 || view[0].UnexplainedGrowthBytes != foreign || view[0].Warning != "foreign_disk_growth" || view[0].ExhaustionSeconds == nil {
		t.Fatal(view)
	}
	clock.Advance(time.Minute)
	foreign = 0
	if view = b.collect(context.Background()); view[0].Warning != "" || view[0].ExhaustionSeconds != nil {
		t.Fatal("foreign cleanup failed to clear warning", view)
	}
}

func TestWorkerDiskBudgetModelLRUSourceLossAndPins(t *testing.T) {
	c, model := cacheFixture(t)
	c.Limit = 1 << 20
	a, b, d, e := model("a", "aaaa"), model("b", "bbbb"), model("d", "dddd"), model("e", "eeee")
	ctx := context.Background()
	for _, m := range []Model{a, b, d, e} {
		if _, err := c.Prepare(ctx, m); err != nil {
			t.Fatal(err)
		}
		c.Clock.(*fakeClock).Advance(time.Second)
	}
	if err := os.Remove(filepath.Join(c.NASRoot, a.Path)); err != nil {
		t.Fatal(err)
	}
	c.Protected = func(key string) bool { return key == cacheKey(b) }
	before, err := collectionAllocatedBytes(ctx, c.Root)
	if err != nil {
		t.Fatal(err)
	}
	const total = uint64(100 << 30)
	floor := diskFreeFloor(total)
	budget := newWorkerDiskBudget(c.Clock, func(string) (DiskReading, error) {
		n, err := collectionAllocatedBytes(ctx, c.Root)
		return DiskReading{Volume: "local", TotalBytes: total, FreeBytes: floor + uint64(before-n)}, err
	})
	c.budget = budget
	budget.add(diskBudgetRoot{name: "models", path: c.Root, candidates: c.budgetCandidates})
	release, err := budget.reserve(ctx, c.Root, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	release()
	for _, m := range []Model{a, b, e} {
		if _, err = os.Stat(filepath.Join(c.Root, cacheKey(m))); err != nil {
			t.Fatal("unique or pinned copy lost", m.Name, err)
		}
	}
	if _, err = os.Stat(filepath.Join(c.Root, cacheKey(d))); !os.IsNotExist(err) {
		t.Fatal("recoverable LRU not reclaimed", err)
	}
}

func TestWorkerDiskBudgetDetachedBuilderKeepsReservation(t *testing.T) {
	_, clock := fixture(t)
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "input", strings.Repeat("x", 16384))
	input := SharedInput{NAS: "input", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
	const total = uint64(100 << 30)
	b := newWorkerDiskBudget(clock, func(string) (DiskReading, error) {
		return DiskReading{Volume: "local", TotalBytes: total, FreeBytes: diskFreeFloor(total) + 20000}, nil
	})
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(unblock) })
	c := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Clock: clock, Limit: 1 << 20}, budget: b, beforeStage: func(context.Context) { close(entered); <-unblock }}
	t.Cleanup(func() { _ = removeSharedEntry(c.cache.Root) })
	b.add(diskBudgetRoot{name: "shared_inputs", path: c.cache.Root, candidates: c.budgetCandidates})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, done, err := c.Acquire(ctx, input, nas)
		if done != nil {
			done()
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(testWaitTimeout):
		t.Fatal("builder did not start")
	}
	c.mu.Lock()
	var joined <-chan struct{}
	for stage := range c.staging {
		joined = stage.done
	}
	c.mu.Unlock()
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled builder succeeded")
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("caller did not withdraw")
	}
	if release, err := b.reserve(context.Background(), c.cache.Root, uint64(ref.SizeBytes), 0); err == nil {
		release()
		t.Fatal("detached writer lost its volume reservation")
	}
	once.Do(func() { close(unblock) })
	select {
	case <-joined:
	case <-time.After(testWaitTimeout):
		t.Fatal("detached writer did not join")
	}
	if release, err := b.reserve(context.Background(), c.cache.Root, uint64(ref.SizeBytes), 0); err != nil {
		t.Fatal("joined writer kept reservation", err)
	} else {
		release()
	}
}

// A shared input on a nearly full host first reclaims local caches. That
// reclaim outlasted the 30s staging stall budget and timed out every retry with
// zero bytes copied. Slow reclaim is progress; it must not trip the detector.
func TestWorkerDiskBudgetSlowReclaimDoesNotStallSharedInput(t *testing.T) {
	defer func(d time.Duration) { ioProgressKeepalive = d }(ioProgressKeepalive)
	ioProgressKeepalive = time.Millisecond
	clock := &workerFakeClock{now: time.Date(2026, 11, 13, 4, 58, 0, 0, time.UTC)}
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "vod.mp4", strings.Repeat("v", 16384))
	input := SharedInput{NAS: "vod.mp4", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
	const total = uint64(100 << 30)
	var mu sync.Mutex
	freed := false
	b := newWorkerDiskBudget(clock, func(string) (DiskReading, error) {
		mu.Lock()
		defer mu.Unlock()
		free := diskFreeFloor(total)
		if freed {
			free += 1 << 20
		}
		return DiskReading{Volume: "local", TotalBytes: total, FreeBytes: free}, nil
	})
	evicted := false
	b.add(diskBudgetRoot{name: "environments", path: t.TempDir(), candidates: func(context.Context) ([]diskBudgetCandidate, error) {
		return []diskBudgetCandidate{{path: "slow-environment", evict: func(ctx context.Context) (bool, error) {
			// A long local tree delete: more than the stall budget passes.
			for step := time.Duration(0); step < checkpointAvailabilityStallBudget+15*time.Second; step += time.Second {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				clock.Advance(time.Second)
				time.Sleep(5 * time.Millisecond)
			}
			mu.Lock()
			freed, evicted = true, true
			mu.Unlock()
			return true, nil
		}}}, nil
	}})
	c := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Clock: clock, Limit: 1 << 20}, budget: b}
	t.Cleanup(func() { _ = removeSharedEntry(c.cache.Root) })
	b.add(diskBudgetRoot{name: "shared_inputs", path: c.cache.Root, candidates: c.budgetCandidates})
	path, done, err := c.Acquire(context.Background(), input, nas)
	if done != nil {
		defer done()
	}
	if err != nil || !evicted {
		t.Fatal("slow reclaim timed out staging", err, evicted)
	}
	if got, err := os.ReadFile(path); err != nil || len(got) != 16384 {
		t.Fatal("input not staged after reclaim", err)
	}
}

func TestWorkerDiskBudgetSnapshotReaderQueuePinAndCustody(t *testing.T) {
	q, clock := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	setModelPolicy(t, context.Background(), Client{URL: server.URL, Token: "test"}, &ModelPolicy{Sources: []string{"org/*"}})
	nas := t.TempDir()
	repo := tinyModel(t, nas)
	m := ModelSnapshot{Repo: "org/tiny", Revision: tinyRevision}
	input, _, err := resolveModelSnapshot(context.Background(), nas, m)
	if err != nil {
		t.Fatal(err)
	}
	input.model = &m
	c := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}, Client: Client{URL: server.URL, Token: "test"}}
	t.Cleanup(func() { _ = removeSharedEntry(c.cache.Root) })
	path, done, err := c.Acquire(context.Background(), input, repo)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	const total = uint64(100 << 30)
	floor := diskFreeFloor(total)
	before, err := collectionAllocatedBytes(context.Background(), c.cache.Root)
	if err != nil {
		t.Fatal(err)
	}
	b := newWorkerDiskBudget(clock, func(string) (DiskReading, error) {
		n, err := collectionAllocatedBytes(context.Background(), c.cache.Root)
		return DiskReading{Volume: "local", TotalBytes: total, FreeBytes: floor + uint64(max(0, before-n))}, err
	})
	c.budget = b
	b.add(diskBudgetRoot{name: "model_snapshots", path: c.cache.Root, candidates: c.budgetCandidates})
	attempt := func() {
		t.Helper()
		release, err := b.reserve(context.Background(), c.cache.Root, 1, 0)
		if err == nil {
			release()
			t.Fatal("protected snapshot was evicted")
		}
		if _, err = os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	attempt() // Active reader.
	done()
	s := spec("experiment")
	s.ModelSnapshots = map[string]ModelSnapshot{"weights": m}
	j := submit(t, q, s)
	attempt() // A queued reference survives local reader release.
	if err = q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	owner := &workerStore{dir: t.TempDir()}
	unpin, err := owner.pinSharedInput(c.cache.Root, input.key())
	if err != nil {
		t.Fatal(err)
	}
	attempt() // Restart-style durable pin without a process flock.
	unpin()
	weights := filepath.Join(repo, "blobs", digestBytes([]byte(tinyWeights)))
	if err = os.Remove(weights); err != nil {
		t.Fatal(err)
	}
	attempt() // The cache has become the last unique verified copy.
	if err = os.WriteFile(weights, []byte(tinyWeights), 0644); err != nil {
		t.Fatal(err)
	}
	release, err := b.reserve(context.Background(), c.cache.Root, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("recoverable unpinned snapshot retained", err)
	}
}

func TestWorkerCompilerCacheCustodyAndPublicBudget(t *testing.T) {
	_, clock := fixture(t)
	c := &workerCompilerCache{cache: EnvironmentCache{Root: t.TempDir(), Clock: clock}}
	owner := &workerStore{dir: t.TempDir()}
	j := Job{Spec: spec("experiment")}
	path, done, err := c.acquire(context.Background(), owner, j)
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		t.Fatal("no owned Go build cache")
	}
	if err = os.WriteFile(filepath.Join(path, "rebuildable"), []byte("object"), 0600); err != nil {
		t.Fatal(err)
	}
	entries, err := c.budgetCandidates(context.Background())
	if err != nil || len(entries) != 1 {
		t.Fatal(entries, err)
	}
	if removed, err := entries[0].evict(context.Background()); err != nil || removed {
		t.Fatal("active compiler cache evicted", err)
	}
	done()
	// Refresh mtime after last use; stale inventory cannot delete a newer entry.
	entries, err = c.budgetCandidates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if removed, err := entries[0].evict(context.Background()); err != nil || !removed {
		t.Fatal("idle compiler cache not reclaimed", err)
	}
	j.Spec.Run.Env = map[string]string{"GOCACHE": "/external/private"}
	path, done, err = c.acquire(context.Background(), owner, j)
	if err != nil {
		t.Fatal(err)
	}
	done()
	if path != "" || !c.external.Load() {
		t.Fatal("external compiler cache was managed")
	}
	m := &Machine{HygieneCollection: &WorkerHygieneCollection{DiskBudget: []DiskBudgetVolume{{ID: "/private/volume", Roots: []string{"/private/root"}, Warning: "private"}}, Scratch: &AttemptFilesCollection{Reasons: map[string]int64{"private-owner": 1}}, CompilerWarning: "private"}}
	publicWorkerCollection(m)
	data, _ := json.Marshal(m)
	if strings.Contains(string(data), "private") {
		t.Fatal("public budget leaked paths or identity", string(data))
	}
}

// #1104: on a shared host, other users' disk use swings between collections.
// The automatic cache allowance shrinks by the measured headroom, and collection
// evicts reproducible copies so free space stays at or above floor + headroom.
func TestWorkerDiskBudgetForeignHeadroom(t *testing.T) {
	_, clock := fixture(t)
	root := t.TempDir()
	const mib = 1 << 20
	var files []string
	for i := range 4 {
		path := filepath.Join(root, fmt.Sprintf("copy-%d", i))
		if err := os.WriteFile(path, []byte(strings.Repeat("x", mib)), 0600); err != nil {
			t.Fatal(err)
		}
		files = append(files, path)
	}
	initial, err := collectionAllocatedBytes(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	const total = uint64(100 << 30)
	floor := diskFreeFloor(total)
	foreign := uint64(0)
	probe := func(string) (DiskReading, error) {
		owned, err := collectionAllocatedBytes(context.Background(), root)
		return DiskReading{Volume: "shared", TotalBytes: total, FreeBytes: floor + 6*mib + uint64(initial-owned) - foreign}, err
	}
	b := newWorkerDiskBudget(clock, probe)
	b.add(diskBudgetRoot{name: "files", path: root, candidates: func(context.Context) ([]diskBudgetCandidate, error) {
		var out []diskBudgetCandidate
		for i, path := range files {
			if _, err := os.Stat(path); err == nil {
				out = append(out, diskBudgetCandidate{path: path, used: clock.Now().Add(time.Duration(i) * time.Second), evict: func(context.Context) (bool, error) { return true, os.Remove(path) }})
			}
		}
		return out, nil
	}})
	// Returns the automatic limit and the allocated bytes the cache holds now.
	allowance := func() (int64, int64) {
		t.Helper()
		owned, err := collectionAllocatedBytes(context.Background(), root)
		if err != nil {
			t.Fatal(err)
		}
		limit, _, err := inputCacheBudget(root, owned, 0, probe, b.foreignHeadroom(root))
		if err != nil {
			t.Fatal(err)
		}
		return limit, owned
	}
	view := b.collect(context.Background())
	if len(view) != 1 || view[0].ForeignHeadroomBytes != 0 {
		t.Fatal("first sample invented headroom", view)
	}
	before, _ := allowance()

	// Another user writes 4 MiB between samples: 2 MiB more than free above floor.
	clock.Advance(time.Minute)
	foreign = 4 * mib
	view = b.collect(context.Background())
	v := view[0]
	if v.ForeignPeakGrowthBytes != 4*mib || v.ForeignHeadroomBytes != 4*mib || v.FloorBytes != floor+4*mib {
		t.Fatalf("headroom basis: %+v", v)
	}
	if r, _ := probe(root); r.FreeBytes < floor+v.ForeignHeadroomBytes {
		t.Fatalf("eviction left free=%d below floor+headroom=%d", r.FreeBytes, floor+v.ForeignHeadroomBytes)
	}
	remaining := 0
	for _, path := range files {
		if _, err := os.Stat(path); err == nil {
			remaining++
		}
	}
	if remaining != 2 {
		t.Fatalf("evicted %d copies, want the 2 needed", 4-remaining)
	}
	// No free space is left above floor + headroom, so the allowance is what
	// the cache already holds (block allocation varies by filesystem).
	if after, held := allowance(); after != held || after >= before {
		t.Fatalf("automatic allowance %d did not shrink from %d to the %d bytes held", after, before, held)
	}
	// A new write is admitted only by evicting another copy, never from headroom.
	release, err := b.reserve(context.Background(), root, mib, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r, _ := probe(root); r.FreeBytes < floor+v.ForeignHeadroomBytes+mib {
		t.Fatalf("reservation took foreign headroom: free=%d", r.FreeBytes)
	}
	release()

	// The other user cleans up; room to return to their peak stays reserved.
	clock.Advance(time.Minute)
	foreign = 0
	v = b.collect(context.Background())[0]
	if v.ForeignBytes+4*mib != v.ForeignPeakBytes || v.ForeignHeadroomBytes != 4*mib {
		t.Fatalf("cleanup basis: %+v", v)
	}
}

// A sustained decline forecasts exhaustion from its average rate and names
// whether Mycelium's own allocation explains it; recovery ends the episode.
func TestWorkerDiskBudgetDeclineEpisode(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprint("owned=", owned), func(t *testing.T) {
			_, clock := fixture(t)
			root := t.TempDir()
			const total = uint64(1000 << 30)
			start := diskFreeFloor(total) + 100<<30
			free := start
			b := newWorkerDiskBudget(clock, func(string) (DiskReading, error) {
				return DiskReading{Volume: "local", TotalBytes: total, FreeBytes: free}, nil
			})
			b.add(diskBudgetRoot{name: "files", path: root})
			b.collect(context.Background())
			consume := func(bytes uint64) []DiskBudgetVolume {
				t.Helper()
				if owned {
					if err := os.WriteFile(filepath.Join(root, fmt.Sprint(clock.Now().UnixNano())), make([]byte, bytes), 0600); err != nil {
						t.Fatal(err)
					}
				}
				free -= bytes
				clock.Advance(time.Minute)
				return b.collect(context.Background())
			}
			step := uint64(1 << 20) // Small enough for real owned files.
			if !owned {
				step = 10 << 30
			}
			var v []DiskBudgetVolume
			for range 3 {
				v = consume(step)
			}
			free += step / 2 // A brief rise below the start continues the episode.
			clock.Advance(time.Minute)
			v = b.collect(context.Background())
			if v[0].DecliningSeconds != 240 || v[0].ExhaustionSeconds == nil {
				t.Fatal("episode did not continue through a partial rise", v[0])
			}
			wantRate := float64(start-free) / 240
			if got := float64(free-min(free, v[0].FloorBytes)) / wantRate; *v[0].ExhaustionSeconds != got {
				t.Fatalf("forecast used a step, not the episode average: %v want %v", *v[0].ExhaustionSeconds, got)
			}
			want := "foreign_disk_growth"
			if owned {
				want = "owned_disk_growth"
			}
			if v[0].DeclineCause != want {
				t.Fatalf("cause %q, want %q", v[0].DeclineCause, want)
			}
			free = start
			clock.Advance(time.Minute)
			v = b.collect(context.Background())
			if v[0].DecliningSeconds != 0 || v[0].ExhaustionSeconds != nil || v[0].DeclineCause != "" {
				t.Fatal("recovery kept the episode", v[0])
			}
		})
	}
}
