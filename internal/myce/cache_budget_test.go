package myce

import (
	"context"
	"errors"
	"testing"
	"time"
)

func syntheticCacheBudget(t *testing.T, clock Clock, total, free uint64) (*cacheBudgets, *DiskReading) {
	disk := &DiskReading{Volume: "local", TotalBytes: total, FreeBytes: free}
	b := newCacheBudgets()
	b.hostRoot = t.TempDir()
	b.probe = func(string) (DiskReading, error) { return *disk, nil }
	return b, disk
}
func TestCacheBudgetPhysicalScaleAndSharedReservations(t *testing.T) {
	for _, total := range []uint64{8 << 20, 8 << 40} {
		t.Run(time.Duration(total).String(), func(t *testing.T) {
			clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			b, disk := syntheticCacheBudget(t, clock, total, total/2)
			limit, err := b.limit("environments", 0, 0, clock.Now())
			if err != nil || limit != int64(total/2)-4*cacheAllocationUnit(*disk) {
				t.Fatal(limit, err)
			}
			held, err := b.reserve("environments", limit-3*cacheAllocationUnit(*disk), clock)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = b.reserve("files", 1, clock); err == nil {
				t.Fatal("same physical free space promised twice")
			}
			held.release()
			// A large reused working set can borrow far past the retired 80/100 GiB ceilings.
			used := int64(total / 4)
			b.touch("models", "hot", used, clock.Now())
			clock.Advance(time.Minute)
			b.touch("models", "hot", used, clock.Now())
			limit, err = b.limit("models", used, 0, clock.Now())
			if err != nil || limit <= used {
				t.Fatal(limit, err)
			}
			if total > 1<<40 && limit <= 100<<30 {
				t.Fatal("retired size ceiling retained", limit)
			}
			disk.FreeBytes = total / 8
			clock.Advance(time.Minute)
			smaller, err := b.limit("models", used, 0, clock.Now())
			if err != nil || smaller >= limit {
				t.Fatal("shrinking disk did not reduce budget", smaller, limit, err)
			}
			disk.FreeBytes = total / 2
			clock.Advance(time.Minute)
			recovered, err := b.limit("models", used, 0, clock.Now())
			if err != nil || recovered <= smaller {
				t.Fatal("recovery did not restore borrowing", recovered, err)
			}
		})
	}
}
func TestCacheBudgetPressurePrefersColdDomain(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, disk := syntheticCacheBudget(t, clock, 1<<40, 1<<30)
	b.touch("hot", "model", 1<<30, clock.Now())
	clock.Advance(time.Minute)
	b.touch("hot", "model", 1<<30, clock.Now())
	if _, err := b.limit("hot", 1<<30, 0, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.limit("cold", 1<<30, 0, clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	disk.FreeBytes = 128 << 20
	cold, _ := b.limit("cold", 1<<30, 0, clock.Now())
	hot, _ := b.limit("hot", 1<<30, 0, clock.Now())
	if cold >= hot || hot != 1<<30 {
		t.Fatal("cold should pay pressure debt first", cold, hot)
	}
	// Missing evidence never invents free space; existing state remains unchanged.
	b.probe = func(string) (DiskReading, error) { return DiskReading{}, errors.New("unavailable") }
	if _, err := b.limit("cold", 1<<30, 0, clock.Now()); err == nil {
		t.Fatal("probe error admitted work")
	}
	if _, err := b.reserve("cold", 1, clock); err == nil {
		t.Fatal("probe error reserved work")
	}
}
func TestCacheBudgetExplicitIntentAndReuseLifecycle(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, _ := syntheticCacheBudget(t, clock, 1<<40, 1<<39)
	if limit, err := b.limit("cache", 0, 12345, clock.Now()); err != nil || limit != 12345 {
		t.Fatal(limit, err)
	}
	b.touch("cache", "key", 123, clock.Now())
	if b.recent("cache", "key", clock.Now()) {
		t.Fatal("single use invented reuse")
	}
	clock.Advance(time.Hour)
	b.touch("cache", "key", 123, clock.Now())
	if !b.recent("cache", "key", clock.Now()) {
		t.Fatal("actual reuse ignored")
	}
	clock.Advance(time.Hour)
	if b.recent("cache", "key", clock.Now()) {
		t.Fatal("cold entry retained forever")
	}
	b.forget("cache", "key")
	if len(b.reuse) != 0 {
		t.Fatal("eviction retained learning record")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := b.copy(ctx, "cache", clock, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
