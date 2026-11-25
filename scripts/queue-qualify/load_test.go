package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseBytes(t *testing.T) {
	for in, want := range map[string]int64{"4096": 4096, "4KiB": 4096, "2 MiB": 2 << 20, "1gib": 1 << 30, "1TiB": 1 << 40} {
		if got, err := parseBytes(in); err != nil || got != want {
			t.Errorf("%s: %d %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "-1", "1GB", "lots"} {
		if _, err := parseBytes(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func smallLoad(dir string, free func(string) (uint64, uint64, error)) loadOptions {
	return loadOptions{dir: dir, maxBytes: 24 << 20, workingSet: 8 << 20, fileSize: 4 << 20, minFree: 1 << 30,
		minFreePct: 10, duration: 20 * time.Second, writers: 2, readBack: true, free: free}
}

func plentyFree(string) (uint64, uint64, error) { return 900 << 30, 1000 << 30, nil }

func TestLoadIsBoundedAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	res, err := runLoad(context.Background(), smallLoad(dir, plentyFree))
	if err != nil {
		t.Fatal(err)
	}
	// Writers stop at the byte cap, give or take one file each.
	if res.StoppedBy != "max-bytes" || res.BytesWritten < 24<<20 || res.BytesWritten > 32<<20 || res.BytesRead == 0 || !res.CleanedUp {
		t.Fatalf("%+v", res)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("left behind %v", entries)
	}
}

func TestLoadRefusesOrStopsAtTheFreeSpaceFloor(t *testing.T) {
	dir := t.TempDir()
	// 100 GiB free of 1 TiB: the 10% floor (102.4 GiB) is already crossed.
	tight := func(string) (uint64, uint64, error) { return 100 << 30, 1 << 40, nil }
	if _, err := runLoad(context.Background(), smallLoad(dir, tight)); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("not refused: %v", err)
	}
	// Free space shrinks as other work writes; the generator stops itself.
	var calls atomic.Int64
	shrinking := func(string) (uint64, uint64, error) {
		n := uint64(calls.Add(1))
		return (200 << 30) - n*(20<<30), 1000 << 30, nil
	}
	opt := smallLoad(dir, shrinking)
	opt.maxBytes = 1 << 30
	res, err := runLoad(context.Background(), opt)
	if err != nil || res.StoppedBy != "free-space-floor" || !res.CleanedUp {
		t.Fatalf("%+v %v", res, err)
	}
	opt.writers = 9
	if _, err := runLoad(context.Background(), opt); err == nil {
		t.Fatal("nine writers accepted")
	}
	opt.writers, opt.duration = 1, 2*time.Hour
	if _, err := runLoad(context.Background(), opt); err == nil {
		t.Fatal("two hours accepted")
	}
}

func TestLoadRateCapHolds(t *testing.T) {
	opt := smallLoad(t.TempDir(), plentyFree)
	opt.rateMiBps, opt.readBack, opt.writers = 16, false, 1
	start := time.Now()
	res, err := runLoad(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	// 24 MiB at 16 MiB/s, with one second of burst allowance, takes >= 0.5 s.
	if elapsed := time.Since(start); elapsed < 500*time.Millisecond || res.WriteMiBps > 40 {
		t.Fatalf("cap ignored: %s, %.1f MiB/s", elapsed, res.WriteMiBps)
	}
}

func TestCleanupRemovesOnlyMarkedLeftovers(t *testing.T) {
	parent := t.TempDir()
	marked := filepath.Join(parent, loadDirPrefix+"abc")
	unmarked := filepath.Join(parent, loadDirPrefix+"def")
	other := filepath.Join(parent, "jobs")
	for _, d := range []string{marked, unmarked, other} {
		os.Mkdir(d, 0o755)
	}
	os.WriteFile(filepath.Join(marked, loadMarker), nil, 0o644)
	os.WriteFile(filepath.Join(parent, snapshotPrefix+".bin"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(parent, "keep.bin"), []byte("x"), 0o644)
	removed, err := removeLoadLeftovers(parent)
	if err != nil || len(removed) != 2 {
		t.Fatalf("%v %v", removed, err)
	}
	for _, keep := range []string{unmarked, other, filepath.Join(parent, "keep.bin")} {
		if _, err := os.Stat(keep); err != nil {
			t.Fatalf("removed %s", keep)
		}
	}
}
