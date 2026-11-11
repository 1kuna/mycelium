package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSharedInputCapacityCurrentHeadroom(t *testing.T) {
	_, clock := fixture(t)
	disk := DiskReading{TotalBytes: 1 << 40, FreeBytes: 13 << 30}
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1, Clock: clock}, automatic: true, diskProbe: func(string) (DiskReading, error) { return disk, nil }}
	cache.refreshCapacity()
	before := cache.capacityReport()
	const need = int64(15 << 30)
	var refusal *stagingCapacityError
	if err := cache.admit(context.Background(), "", need); !errors.As(err, &refusal) || refusal.required != need || refusal.available <= 0 || refusal.available >= need {
		t.Fatalf("missing typed current-headroom refusal: %v", err)
	}
	clock.Advance(time.Minute)
	disk.FreeBytes = 113 << 30 // Reproduce disk reclamation without worker restart.
	collector := workerHygieneCollector{w: Worker{Clock: clock, sharedInputs: cache}}
	collector.collect(context.Background())
	after := cache.capacityReport()
	if after.CapacityBytes <= before.CapacityBytes || after.FreeBytes < uint64(need) {
		t.Fatalf("stale allowance: before=%+v after=%+v", before, after)
	}
	if err := cache.admit(context.Background(), "", need); err != nil {
		t.Fatal("reclaimed disk did not restore admission", err)
	}

	// Shrinking physical headroom retains existing data and custody, while a
	// fresh report and actual admission agree that new cold bytes do not fit.
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "input", "retained input")
	input := SharedInput{NAS: "input", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
	path, release, err := cache.Acquire(context.Background(), input, nas)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	disk.FreeBytes = diskFreeFloor(disk.TotalBytes)
	clock.Advance(time.Minute)
	collector.collect(context.Background())
	if got := cache.capacityReport(); got.FreeBytes != 0 || got.Protected[input.key()] == 0 {
		t.Fatalf("custody/headroom lost: %+v", got)
	}
	if err := cache.admit(context.Background(), "", 1); !errors.As(err, &refusal) || refusal.available != 0 {
		t.Fatalf("shrink admitted new bytes: %v", err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "retained input" {
		t.Fatal("protected input removed", err)
	}
}

func TestFileCacheCurrentHeadroomAndExplicitLimit(t *testing.T) {
	q, clock := fixture(t)
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "input", "small input")
	client := Client{URL: testServerForEnvironments(t, q), Token: "test", Clock: clock}
	if err := client.UploadFile(context.Background(), filepath.Join(nas, "input"), ref); err != nil {
		t.Fatal(err)
	}
	disk := DiskReading{TotalBytes: 1 << 30, FreeBytes: diskFreeFloor(1 << 30)}
	cache := &FileCache{Root: t.TempDir(), Clock: clock, Client: client, automatic: true, diskProbe: func(string) (DiskReading, error) { return disk, nil }}
	var refusal *stagingCapacityError
	dest := filepath.Join(t.TempDir(), "local")
	if err := cache.copyTo(context.Background(), ref, dest); !errors.As(err, &refusal) || refusal.available != 0 {
		t.Fatalf("missing typed file refusal: %v", err)
	}
	disk.FreeBytes += 1 << 20
	clock.Advance(time.Minute)
	if err := cache.copyTo(context.Background(), ref, dest); err != nil {
		t.Fatal("automatic cache stayed stuck", err)
	}
	cache.automatic, cache.Limit = false, 1
	if err := cache.copyTo(context.Background(), FileRef{SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}, filepath.Join(t.TempDir(), "warm")); err != nil {
		t.Fatal("warm retained copy refused", err)
	}
	limit, _, err := inputCacheBudget(cache.Root, ref.SizeBytes, 1, cache.diskProbe, 0)
	if err != nil || limit != 1 {
		t.Fatal("explicit ceiling changed", limit, err)
	}
}
