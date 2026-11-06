package myce

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCacheHostIndependentLedgersAndAbandonedClaim(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	first, disk := syntheticCacheBudget(t, clock, 200<<30, 100<<30)
	second := newCacheBudgets()
	second.hostRoot, second.probe = first.hostRoot, first.probe
	held, err := first.reserve("worker", 80<<30, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = second.reserve("queue", 80<<30, clock); err == nil {
		t.Fatal("independent ledger promised the same physical blocks")
	}
	// Closing the owner descriptor models kernel release on process death. No
	// lease expiry, deletion, clock advance, or peer-process cancellation is used.
	held.host.Close()
	recovered, err := second.reserve("queue", 80<<30, clock)
	if err != nil {
		t.Fatal("inactive owner prevented recovery", err)
	}
	recovered.release()
	disk.FreeBytes = 1
	if _, err = first.reserve("worker", 1, clock); err == nil {
		t.Fatal("real disk pressure ignored")
	}
}
func TestCacheHostManagedWriteDoesNotBecomeExternalTrend(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, disk := syntheticCacheBudget(t, clock, 10<<30, 5<<29)
	allocated := int64(0)
	b.allocation = func(paths []string) (int64, error) {
		if len(paths) > 0 {
			return allocated, nil
		}
		return 0, nil
	}
	if _, err := b.limit("files", 0, 0, clock.Now()); err != nil {
		t.Fatal(err)
	}
	held, err := b.reserve("files", 1<<30, clock, "blob")
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(3 * time.Second)
	allocated = 1 << 30
	disk.FreeBytes -= 1 << 30
	held.release()
	// The inventory is deliberately stale. Only the completed writer knows its
	// allocation. A second destination on the same volume still has room.
	if _, err = b.limit("attempt", 0, 0, clock.Now()); err != nil {
		t.Fatal(err)
	}
	copy, err := b.reserve("attempt", 1<<30, clock)
	if err != nil {
		t.Fatal("managed download invented external consumption", err)
	}
	copy.release()
}
func TestCacheHostNewDemandReclaimsOtherColdDomain(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, disk := syntheticCacheBudget(t, clock, 10<<30, 512<<20)
	cold := int64(2 << 30)
	if _, err := b.limit("shared", cold, 0, clock.Now()); err != nil {
		t.Fatal(err)
	}
	b.register("shared", func(context.Context) {
		limit, err := b.limit("shared", cold, 0, clock.Now())
		if err != nil {
			t.Fatal(err)
		}
		freed := max(0, cold-limit)
		cold -= freed
		disk.FreeBytes += uint64(freed)
	})
	release, err := b.borrow(context.Background(), "files", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if cold == 2<<30 {
		t.Fatal("new demand never reached the cold cache")
	}
	held, err := b.reserve("files", 1<<30, clock)
	if err != nil {
		t.Fatal("cold capacity could not be borrowed", err)
	}
	held.release()
}
func TestCacheHostUnknownBuilderFencesOtherWriters(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(t, clock, 10<<30, 5<<30)
	other := newCacheBudgets()
	other.hostRoot, other.probe = b.hostRoot, b.probe
	held, err := b.explore("environments", clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.reserve("models", 1<<30, clock); err == nil {
		t.Fatal("copy consumed the unmeasured builder's exploration")
	}
	held.release()
	copy, err := other.reserve("models", 1<<30, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer copy.release()
	if _, err = b.explore("environments", clock); err == nil {
		t.Fatal("unknown installer overlaps promised copy capacity")
	}
}

func TestCacheHostManySmallFilesChargeSeparateAllocations(t *testing.T) {
	disk := DiskReading{AllocationBytes: 4096}
	files := FileManifest{}
	for _, name := range []string{"a", "b", "nested/c"} {
		files[name] = FileRef{SizeBytes: 1}
	}
	if charge := cacheManifestCharge(disk, files); charge < 3*4096 {
		t.Fatal("packed tiny files into one logical allocation", charge)
	}
}

func TestCacheHostControlGrowthAndRecovery(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, disk := syntheticCacheBudget(t, clock, 1<<30, 100<<20)
	database := filepath.Join(t.TempDir(), "control.db")
	header := append([]byte("SQLite format 3\x00"), 0x10, 0)
	if err := os.WriteFile(database, header, 0600); err != nil {
		t.Fatal(err)
	}
	data, wal := int64(10<<20), int64(64<<10)
	b.allocation = func(paths []string) (int64, error) {
		switch len(paths) {
		case 3:
			return data + wal, nil
		case 2:
			return wal, nil
		}
		return 0, nil
	}
	control, err := b.trackControl(database, clock, database, database+"-wal", database+"-shm")
	if err != nil {
		t.Fatal(err)
	}
	before, err := b.limit("cache", 0, 0, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	wal += 1 << 20
	disk.FreeBytes -= 1 << 20
	after, err := b.limit("cache", 0, 0, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if before-after > 3<<20 {
		t.Fatal("owned control growth became external consumption", before, after)
	}
	control.release()
	recovered, err := b.limit("cache", 0, 0, clock.Now())
	if err != nil || recovered <= after {
		t.Fatal("inactive control writer retained a rewrite reserve", recovered, after, err)
	}
}
