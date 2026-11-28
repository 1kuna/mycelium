package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// compilerFloorFixture is a worker-owned Go cache on a 200 GiB volume whose
// free space rises by each object's allocation as it is unlinked.
type compilerFloorFixture struct {
	t       *testing.T
	clock   *fakeClock
	cache   *workerCompilerCache
	budget  *workerDiskBudget
	dir     string
	objects []string
	unit    uint64
	deficit atomic.Int64 // Units below the cache floor before any trim.
}

const compilerFloorTotal = 200 << 30

func newCompilerFloorFixture(t *testing.T) *compilerFloorFixture {
	_, clock := fixture(t)
	f := &compilerFloorFixture{t: t, clock: clock, cache: &workerCompilerCache{cache: EnvironmentCache{Root: t.TempDir(), Clock: clock}}}
	if err := f.cache.cache.initStorage(); err != nil {
		t.Fatal(err)
	}
	f.dir = filepath.Join(f.cache.cache.Root, identityHash("go-build"))
	if err := os.MkdirAll(filepath.Join(f.dir, "aa"), 0700); err != nil {
		t.Fatal(err)
	}
	f.budget = newWorkerDiskBudget(clock, func(string) (DiskReading, error) {
		free := int64(cacheFreeFloor(compilerFloorTotal)) - f.deficit.Load()*int64(f.unit)
		for _, path := range f.objects {
			if _, err := os.Lstat(path); os.IsNotExist(err) {
				free += int64(f.unit)
			}
		}
		return DiskReading{Volume: "local", TotalBytes: compilerFloorTotal, FreeBytes: uint64(free)}, nil
	})
	f.budget.add(diskBudgetRoot{name: "compiler", path: f.cache.cache.Root, candidates: f.cache.budgetCandidates, cacheFloor: true})
	return f
}

// object writes a Go cache data entry last modified age before now.
func (f *compilerFloorFixture) object(age time.Duration) string {
	f.t.Helper()
	path := filepath.Join(f.dir, "aa", fmt.Sprintf("aa%062x-d", len(f.objects)))
	if err := os.WriteFile(path, make([]byte, 4096), 0600); err != nil {
		f.t.Fatal(err)
	}
	at := f.clock.Now().Add(-age)
	if err := os.Chtimes(path, at, at); err != nil {
		f.t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		f.t.Fatal(err)
	}
	if allocated := uint64(info.Sys().(*syscall.Stat_t).Blocks) * 512; f.unit == 0 {
		f.unit = allocated
	} else if allocated != f.unit {
		f.t.Fatalf("fixture allocation %d, want %d", allocated, f.unit)
	}
	f.objects = append(f.objects, path)
	return path
}

func (f *compilerFloorFixture) present(want map[string]bool) {
	f.t.Helper()
	for i, path := range f.objects {
		_, err := os.Lstat(path)
		if got := err == nil; got != want[path] {
			f.t.Errorf("object %d present = %t (%v), want %t", i, got, err, want[path])
		}
	}
}

// #1836: a cache that holds free space under the cache floor is trimmed
// oldest first until the floor is met, periodically and at admission, while
// objects inside the grace period survive and admission itself still admits.
func TestCompilerCacheKeepsVolumeAboveCacheFloor(t *testing.T) {
	for total, want := range map[uint64]uint64{2 << 40: 40 << 30, 100 << 30: 25 << 30, 512 << 20: 256 << 20} {
		if got := cacheFreeFloor(total); got != want {
			t.Fatalf("cacheFreeFloor(%d) = %d, want %d", total, got, want)
		}
	}
	f := newCompilerFloorFixture(t)
	f.object(5 * time.Hour)
	f.object(4 * time.Hour)
	old := f.object(3 * time.Hour)
	aged := f.object(compilerCacheGrace + time.Minute)
	fresh := f.object(compilerCacheGrace - time.Minute)
	f.deficit.Store(2)

	f.budget.reclaimPressure(context.Background()) // The hygiene lane's periodic pass.
	f.present(map[string]bool{old: true, aged: true, fresh: true})

	// Builds consume three more units. Admission trims before reserving, keeps
	// the object inside its grace period, and does not refuse for this floor.
	f.deficit.Store(5)
	release, err := f.budget.reserve(context.Background(), f.cache.cache.Root, 1, 0)
	if err != nil {
		t.Fatalf("admission refused for the cache floor: %v", err)
	}
	release()
	f.present(map[string]bool{fresh: true})
}

// #1836: an attempt holding the cache protects every object it could have
// looked up (Go refreshes a used object's mtime once it is an hour old), but a
// long attempt that is not compiling no longer protects older objects. A
// holder of the use lock without a pin protects the last hour as well.
func TestCompilerCacheFloorKeepsLockedAndPinnedObjects(t *testing.T) {
	f := newCompilerFloorFixture(t)
	owner := &workerStore{dir: t.TempDir()}
	start := f.clock.Now()
	f.object(goCacheUseRefresh + compilerCacheGrace + time.Minute) // Unused by the attempt.
	looked := f.object(goCacheUseRefresh - time.Minute)
	_, release, err := f.cache.acquire(context.Background(), owner, Job{Spec: spec("experiment")})
	if err != nil {
		t.Fatal(err)
	}
	pins := filepath.Join(f.cache.cache.Root, "pins", identityHash("go-build"))
	entries, err := os.ReadDir(pins)
	if err != nil || len(entries) != 1 {
		t.Fatalf("attempt pin = %v, %v", entries, err)
	}
	if err := os.Chtimes(filepath.Join(pins, entries[0].Name()), start, start); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(5 * time.Hour) // A long attempt that is not compiling.
	written := f.object(2 * time.Hour)
	f.deficit.Store(int64(len(f.objects)))

	f.budget.reclaimPressure(context.Background())
	f.present(map[string]bool{looked: true, written: true})

	// Unpinned but still locked: a holder may just have started.
	release()
	use, err := f.cache.cache.lock(context.Background(), identityHash("go-build")+".use", syscall.LOCK_SH)
	if err != nil {
		t.Fatal(err)
	}
	recent := f.object(goCacheUseRefresh - 2*time.Minute)
	f.deficit.Store(int64(len(f.objects)))
	f.clock.Advance(workerHygieneMinInterval)
	f.budget.reclaimPressure(context.Background())
	f.present(map[string]bool{recent: true})

	unlockEnvironment(use)
	f.clock.Advance(workerHygieneMinInterval)
	f.budget.reclaimPressure(context.Background())
	f.present(map[string]bool{})
}

// #2276: one floor on the worker's volume, with a single eviction order across
// real cache entries. A predicted write reserves headroom at the floor.
func TestWorkerCacheFloorReclaimsAcrossCaches(t *testing.T) {
	q, clock := fixture(t)
	client := retentionClient(t, q)
	ctx := context.Background()
	const total = 228 << 30
	const floor = 40 << 30
	const block = 4096
	dir := t.TempDir()
	owner := &workerStore{dir: dir}
	type cacheEntry struct {
		path  string
		bytes uint64
		gone  bool
	}
	var entries []*cacheEntry
	var order []string
	probe := func(string) (DiskReading, error) {
		free := uint64(floor)
		for _, entry := range entries {
			if _, err := os.Lstat(entry.path); os.IsNotExist(err) {
				if !entry.gone {
					entry.gone = true
					order = append(order, entry.path)
				}
				free += entry.bytes
			}
		}
		return DiskReading{Volume: "local", TotalBytes: total, FreeBytes: free, BlockBytes: block}, nil
	}
	fileRoot := filepath.Join(dir, "file-cache")
	if err := PrepareStateRoots(fileRoot); err != nil {
		t.Fatal(err)
	}
	w := Worker{Clock: clock, Client: client, EnvironmentLimit: 1 << 20, FileCacheRoot: fileRoot, DiskProbe: probe}
	if err := w.initAttemptRuntime(ctx, owner); err != nil {
		t.Fatal(err)
	}
	env := w.environmentCache
	old := 72 * time.Hour
	pinnedEnv := pruneFixture(t, env, 1, old+time.Hour)
	inUseEnv := pruneFixture(t, env, 2, old+time.Hour)
	oldEnv := pruneFixture(t, env, 3, old)
	freshEnv := pruneFixture(t, env, 4, time.Hour)
	useLock, err := env.lock(ctx, inUseEnv+".use", syscall.LOCK_SH)
	if err != nil {
		t.Fatal(err)
	}
	defer unlockEnvironment(useLock)
	recipe := binaryRecipe(t, "fake")
	recipe.Key = pinnedEnv
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"needed": {"fake": recipe}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "needed"
	submit(t, q, s)

	compilerDir := filepath.Join(w.compilerCache.cache.Root, identityHash("go-build"), "aa")
	if err := os.MkdirAll(compilerDir, 0700); err != nil {
		t.Fatal(err)
	}
	unpin, err := owner.pinSharedInput(w.compilerCache.cache.Root, identityHash("go-build"))
	if err != nil {
		t.Fatal(err)
	}
	defer unpin()
	write := func(path string, age time.Duration) string {
		t.Helper()
		if err := os.WriteFile(path, make([]byte, 4096), 0600); err != nil {
			t.Fatal(err)
		}
		at := clock.Now().Add(-age)
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
		return path
	}
	oldGo := write(filepath.Join(compilerDir, fmt.Sprintf("aa%062x-d", 1)), 48*time.Hour)
	pinnedGo := write(filepath.Join(compilerDir, fmt.Sprintf("aa%062x-d", 2)), 50*time.Minute)
	unknownGo := write(filepath.Join(compilerDir, "unknown"), 100*time.Hour)
	oldFile := write(filepath.Join(w.fileCache.Root, fmt.Sprintf("%064x", 1)), 36*time.Hour)
	newerFile := write(filepath.Join(w.fileCache.Root, fmt.Sprintf("%064x", 2)), 24*time.Hour)

	// Account the fixture's real allocated size to exercise the measured cap.
	victims := []string{filepath.Join(env.Root, oldEnv), oldGo, oldFile}
	var need uint64
	for _, path := range append(slices.Clone(victims), newerFile) {
		bytes, err := collectionAllocatedBytes(ctx, path)
		if err != nil || bytes <= 0 {
			t.Fatalf("measure fixture allocation: %d, %v", bytes, err)
		}
		entries = append(entries, &cacheEntry{path: path, bytes: uint64(bytes)})
		if path != newerFile {
			need += uint64(bytes)
		}
	}

	// The ordinary pressure pass restores idle headroom at exactly the floor.
	w.diskBudget.reclaimPressure(ctx)
	if !slices.Equal(order, victims[:1]) {
		t.Fatalf("idle eviction order = %v, want %v", order, victims[:1])
	}
	clock.Advance(workerHygieneMinInterval)

	// Before the pending write, trim across the remaining cache types in LRU
	// order, leaving at least one available allocation block above the floor.
	release, err := w.diskBudget.reserve(ctx, dir, need-block, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if got, _ := probe(dir); got.FreeBytes != floor+need {
		t.Fatalf("free = %d, want %d before writing. Evicted: %v", got.FreeBytes, floor+need, order)
	}
	if !slices.Equal(order, victims) {
		t.Fatalf("eviction order = %v, want %v", order, victims)
	}
	for _, path := range []string{filepath.Join(env.Root, pinnedEnv), filepath.Join(env.Root, inUseEnv),
		filepath.Join(env.Root, freshEnv), pinnedGo, unknownGo, newerFile} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("protected or unneeded entry removed: %s: %v", path, err)
		}
	}
}
