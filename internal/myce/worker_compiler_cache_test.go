package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
