package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func fileStoreCompilerObjects(t *testing.T, clock *fakeClock, n int) (*workerCompilerCache, []string, uint64) {
	t.Helper()
	cache := &workerCompilerCache{cache: EnvironmentCache{Root: t.TempDir(), Clock: clock}}
	if err := cache.cache.initStorage(); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(cache.cache.Root, identityHash("go-build"), "aa")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	paths := make([]string, n)
	var unit uint64
	for i := range paths {
		path := filepath.Join(root, fmt.Sprintf("aa%062x-d", i))
		if err := os.WriteFile(path, []byte(strings.Repeat("test", 1024)), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, clock.Now(), clock.Now()); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		allocated := uint64(info.Sys().(*syscall.Stat_t).Blocks) * 512
		if i == 0 {
			unit = allocated
		}
		if allocated == 0 || allocated != unit {
			t.Fatalf("unexpected allocation for equal-sized fixture objects: %d, want %d", allocated, unit)
		}
		clock.Advance(time.Second)
		paths[i] = path
	}
	return cache, paths, unit
}

func TestWorkerFileStorePressureBoundsCompilerTrim(t *testing.T) {
	_, clock := fixture(t)
	cache, paths, unit := fileStoreCompilerObjects(t, clock, 3)
	units := 2 * unit
	const volume = "shared"
	const free = uint64(10 << 30)
	b := newWorkerDiskBudget(clock, func(string) (DiskReading, error) {
		// Lost space is consumed by another writer during the trim. The
		// measured request still cannot remove more than its initial deficit.
		return DiskReading{Volume: volume, TotalBytes: 100 << 30, FreeBytes: free}, nil
	})
	pressure := workerFileStorePressure{TargetFreeBytes: free + 2*units, ShortfallBytes: units}
	owner := &workerStore{dir: t.TempDir()}
	unpin, err := owner.pinSharedInput(cache.cache.Root, identityHash("go-build"))
	if err != nil {
		t.Fatal(err)
	}
	b.trimForFileStore(context.Background(), cache, volume, pressure, units)
	if _, err := os.Stat(paths[0]); err != nil {
		t.Fatalf("queue pressure bypassed compiler custody: %v", err)
	}
	unpin()
	b.trimForFileStore(context.Background(), cache, volume, pressure, units)
	for _, path := range paths[:2] {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("old compiler object was not trimmed: %v", err)
		}
	}
	if _, err := os.Stat(paths[2]); err != nil {
		t.Fatalf("trim exceeded the measured deficit: %v", err)
	}
}

func TestWorkerFileStorePressureRetriesAfterPinAndCoversConcurrentAllocation(t *testing.T) {
	q, clock := fixture(t)
	cache, paths, unit := fileStoreCompilerObjects(t, clock, 4)
	const reserve, total = uint64(64 << 10), uint64(10 << 30)
	const volume = "shared"
	q.fileStoreProductionReserve = int64(reserve)
	q.diskHost = "store"
	c := retentionClient(t, q)
	m := machine("worker", "fake")
	m.HostID = q.diskHost
	check(t, q, m, nil)

	floor := diskFreeFloor(total)
	var allocated atomic.Uint64
	probe := func(string) (DiskReading, error) {
		free := floor + reserve + 2*unit - allocated.Load()
		for _, path := range paths {
			if _, err := os.Stat(path); os.IsNotExist(err) {
				free += unit
			} else if err != nil {
				return DiskReading{}, err
			}
		}
		return DiskReading{Volume: volume, TotalBytes: total, FreeBytes: free, BlockBytes: unit}, nil
	}
	q.files.diskProbe = probe
	w := Worker{Machine: m, Client: c, Clock: clock, compilerCache: cache, diskBudget: newWorkerDiskBudget(clock, probe)}
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "ordinary", Class: "experiment"})
	files := FileManifest{"input": {SHA256: strings.Repeat("a", 64), SizeBytes: int64(2 * unit)}}
	w.reclaimPendingDisk(ctx)

	clock.Advance(workerHygieneMinInterval)
	allocated.Add(unit)
	assertFilePolicyRefusal(t, c.PreflightFileStore(ctx, files), "production reserve")
	owner := &workerStore{dir: t.TempDir()}
	unpin, err := owner.pinSharedInput(cache.cache.Root, identityHash("go-build"))
	if err != nil {
		t.Fatal(err)
	}
	w.reclaimPendingDisk(ctx)
	if _, err := os.Stat(paths[0]); err != nil {
		t.Fatalf("compiler cache changed while pinned: %v", err)
	}
	clock.Advance(12 * workerHygieneMinInterval)
	w.reclaimPendingDisk(ctx)
	if _, err := os.Stat(paths[0]); err != nil {
		t.Fatalf("compiler cache changed while the pin remained held: %v", err)
	}
	unpin()

	// The next sample observes one allocation unit per hygiene interval.
	// The worker needs two units to clear the deficit and a third for slack.
	clock.Advance(workerHygieneMinInterval)
	allocated.Add(unit)
	w.reclaimPendingDisk(ctx)
	for _, path := range paths[:3] {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("second poll failed to reclaim the deficit and measured slack: %v", err)
		}
	}
	if _, err := os.Stat(paths[3]); err != nil {
		t.Fatalf("second poll exceeded the measured trim target: %v", err)
	}
	allocated.Add(unit) // Another writer allocates between trim and retry.
	if err := c.PreflightFileStore(ctx, files); err != nil {
		t.Fatalf("ordinary retry could not fit after the concurrent allocation: %v", err)
	}
}
