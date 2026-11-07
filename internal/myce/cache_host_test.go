package myce

import (
	"context"
	"errors"
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

func TestCacheHostBuilderCustodyOutlivesParent(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(t, clock, 10<<30, 5<<30)
	live := true
	b.helpers = func(group int) ([]int, error) {
		if group != 123 {
			t.Fatal("lost exact helper session", group)
		}
		if live {
			return []int{124}, nil
		}
		return nil, nil
	}
	held, err := b.explore("environments", clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := held.recordHelper(123); err != nil {
		t.Fatal(err)
	}
	held.host.Close() // Parent died; its separately launched helper is still live.
	if _, err := b.reserve("worker", 1, clock); err == nil {
		t.Fatal("parent death released a live helper's storage")
	}
	live = false
	next, err := b.reserve("worker", 1, clock)
	if err != nil {
		t.Fatal("confirmed helper exit did not recover storage", err)
	}
	next.release()
}

func TestCacheHostOverlappingAllocationsCountOnce(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(t, clock, 10<<30, 5<<30)
	root := t.TempDir()
	a, z := filepath.Join(root, "a"), filepath.Join(root, "z")
	first, err := b.reserve("first", 1<<20, clock, a)
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	second, err := b.reserve("second", 1<<20, clock, z)
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	if err := os.WriteFile(a, make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(a, z); err != nil {
		t.Fatal(err)
	}
	used, err := cacheFootprint([]string{a})
	if err != nil {
		t.Fatal(err)
	}
	view, err := b.hostView("local")
	if err != nil {
		t.Fatal(err)
	}
	written, pending := view.state.Written, view.pending
	view.close()
	if written != used || pending != first.bytes+second.bytes {
		t.Fatalf("shared inode credited twice or discharged pending writer: written=%d want=%d pending=%d", written, used, pending)
	}
}

func TestCacheHostRecoversRecordPublicationAndReleaseFailure(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(t, clock, 200<<30, 100<<30)
	held, err := b.reserve("worker", 80<<30, clock)
	if err != nil {
		t.Fatal(err)
	}
	// A killed atomic publisher can leave a truncated temporary file. A damaged
	// primary also has an independently synced, identical recovery publication.
	for _, path := range []string{held.host.Name() + ".json.tmp", held.host.Name() + ".json"} {
		if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.reserve("other", 80<<30, clock); err == nil {
		t.Fatal("recovery dropped an active promise")
	}
	allocation := b.allocation
	b.allocation = func([]string) (int64, error) { return 0, errors.New("scan unavailable") }
	held.release()
	if held.host == nil || !held.closing || len(b.reservations) != 1 {
		t.Fatal("failed release lost retry ownership")
	}
	b.allocation = allocation
	b.maintain(context.Background())
	if held.host != nil || len(b.reservations) != 0 {
		t.Fatal("maintenance did not retry failed release")
	}
	next, err := b.reserve("other", 80<<30, clock)
	if err != nil {
		t.Fatal("record recovery permanently disabled admission", err)
	}
	next.release()
}

func TestCacheHostDemandReclaimsMultipleColdDomains(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, disk := syntheticCacheBudget(t, clock, 10<<30, 512<<20)
	for _, root := range []string{"a", "b"} {
		cold := int64(2 << 30)
		if _, err := b.limit(root, cold, 0, clock.Now()); err != nil {
			t.Fatal(err)
		}
		b.register(root, func(context.Context) {
			limit, err := b.limit(root, cold, 0, clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			freed := max(0, cold-limit)
			cold -= freed
			disk.FreeBytes += uint64(freed)
		})
	}
	release, err := b.borrow(context.Background(), "files", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	held, err := b.reserve("files", 1<<30, clock)
	if err != nil {
		t.Fatal("one-pass fractional reclamation left satisfiable demand short", err)
	}
	held.release()
}
