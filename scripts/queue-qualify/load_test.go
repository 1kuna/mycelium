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

// A run killed hard leaves its directory and manifest; cleanup removes the
// files the manifest lists, a snapshot this tool created, and nothing else.
func TestCleanupRemovesWhatAKilledRunLeft(t *testing.T) {
	parent := t.TempDir()
	run, err := newLoadRun(parent, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := run.add("w0-000000.bin"); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(run.dir, "w0-000000.bin"), []byte("x"), 0o600)
	run.manifest.Close() // killed: no deferred removal
	snap := filepath.Join(parent, snapshotPrefix+".bin")
	if created, err := ensureSnapshot(context.Background(), snap, 8<<20, 0); err != nil || !created {
		t.Fatalf("snapshot: %v %v", created, err)
	}
	other := filepath.Join(parent, "jobs")
	os.Mkdir(other, 0o755)
	os.WriteFile(filepath.Join(parent, "keep.bin"), []byte("x"), 0o644)
	removed, kept, err := removeLoadLeftovers(parent)
	if err != nil || len(removed) != 3 || len(kept) != 0 {
		t.Fatalf("removed %v kept %v: %v", removed, kept, err)
	}
	entries, _ := os.ReadDir(parent)
	if len(entries) != 2 {
		t.Fatalf("left %v; want only jobs and keep.bin", entries)
	}
	// A manifest run whose directory also holds someone else's file: our
	// files go, theirs and the directory stay, and cleanup says so.
	run, _ = newLoadRun(parent, time.Minute)
	run.add("w0-000000.bin")
	os.WriteFile(filepath.Join(run.dir, "w0-000000.bin"), []byte("x"), 0o600)
	os.WriteFile(filepath.Join(run.dir, "theirs.txt"), []byte("x"), 0o600)
	run.manifest.Close()
	if _, _, err := removeLoadLeftovers(parent); err == nil || !strings.Contains(err.Error(), "did not create") {
		t.Fatalf("foreign file in a run directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(run.dir, "theirs.txt")); err != nil {
		t.Fatal("removed a file the run did not create")
	}
	if _, err := os.Stat(filepath.Join(run.dir, "w0-000000.bin")); err == nil {
		t.Fatal("kept the run's own file")
	}
}

// binBytes sums the load files under dir and counts them, the way a
// reservation sees the disk: a created file holds its slot even while empty.
func binBytes(dir string) (files int, bytes int64) {
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && strings.HasSuffix(p, ".bin") {
			if info, err := d.Info(); err == nil {
				files++
				bytes += info.Size()
			}
		}
		return nil
	})
	return files, bytes
}

// peakOnDisk samples dir until stop closes and returns the most load files
// and bytes seen at once.
func peakOnDisk(dir string, stop <-chan struct{}) <-chan [2]int64 {
	out := make(chan [2]int64, 1)
	go func() {
		var peak [2]int64
		for {
			files, bytes := binBytes(dir)
			peak[0], peak[1] = max(peak[0], int64(files)), max(peak[1], bytes)
			select {
			case <-stop:
				out <- peak
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	return out
}

// Eight writers must share one reservation for the working set and for free
// space: no writer may start a file that the others' files leave no room for.
func TestLoadWritersShareOneReservation(t *testing.T) {
	const file = 4 << 20
	t.Run("working set", func(t *testing.T) {
		dir := t.TempDir()
		opt := loadOptions{dir: dir, maxBytes: 160 << 20, workingSet: 8 * file, fileSize: file, minFree: 1 << 30,
			minFreePct: 10, duration: 20 * time.Second, writers: 8, rateMiBps: 64, free: plentyFree}
		stop := make(chan struct{})
		peak := peakOnDisk(dir, stop)
		res, err := runLoad(context.Background(), opt)
		close(stop)
		if err != nil || res.StoppedBy != "max-bytes" || !res.CleanedUp {
			t.Fatalf("%+v %v", res, err)
		}
		if p := <-peak; p[0] > 8 {
			t.Fatalf("%d files on disk at once; the working set holds 8", p[0])
		}
	})
	t.Run("free space", func(t *testing.T) {
		dir := t.TempDir()
		const floor = 1 << 30 // max(--min-free 1 GiB, 10% of 10 GiB)
		var calls atomic.Int64
		arrived := make(chan struct{}, 8)
		release := make(chan struct{})
		go func() {
			defer close(release)
			for range 8 {
				select {
				case <-arrived:
				case <-time.After(200 * time.Millisecond):
					return
				}
			}
		}()
		// Room for the working set at start; then other work leaves room for
		// one and a half files above the floor, and our own files use space.
		// The first checks wait until all eight writers have made one, so the
		// writers race exactly as they do on a slow share.
		free := func(string) (uint64, uint64, error) {
			n := calls.Add(1)
			if n == 1 {
				return floor + 8*file, 10 << 30, nil
			}
			if n <= 9 {
				arrived <- struct{}{}
				<-release
			}
			_, used := binBytes(dir)
			return uint64(max(floor+file*3/2-used, 0)), 10 << 30, nil
		}
		opt := loadOptions{dir: dir, maxBytes: 160 << 20, workingSet: 8 * file, fileSize: file, minFree: 1 << 30,
			minFreePct: 10, duration: 20 * time.Second, writers: 8, rateMiBps: 32, free: free}
		stop := make(chan struct{})
		peak := peakOnDisk(dir, stop)
		res, err := runLoad(context.Background(), opt)
		close(stop)
		if err != nil || res.StoppedBy != "free-space-floor" || !res.CleanedUp {
			t.Fatalf("%+v %v", res, err)
		}
		if p := <-peak; p[0] > 1 || p[1] > file*3/2 {
			t.Fatalf("%d files (%d MiB) started with room for one and a half above the floor", p[0], p[1]>>20)
		}
	})
}

// Other work can fill the volume while a large file is being written; the
// generator must notice during the write, not after it.
func TestLoadStopsWhenTheFloorIsCrossedMidWrite(t *testing.T) {
	dir := t.TempDir()
	free := func(string) (uint64, uint64, error) {
		if _, used := binBytes(dir); used >= 8<<20 {
			return 50 << 30, 1000 << 30, nil // below the 100 GiB floor
		}
		return 900 << 30, 1000 << 30, nil
	}
	opt := loadOptions{dir: dir, maxBytes: 1 << 30, workingSet: 64 << 20, fileSize: 64 << 20, minFree: 1 << 30,
		minFreePct: 10, duration: 20 * time.Second, writers: 1, rateMiBps: 32, free: free}
	res, err := runLoad(context.Background(), opt)
	if err != nil || res.StoppedBy != "free-space-floor" || !res.CleanedUp {
		t.Fatalf("%+v %v", res, err)
	}
	if res.BytesWritten >= 64<<20 || res.FileWrite.Count != 0 {
		t.Fatalf("the 64 MiB file finished after the floor was crossed: wrote %d MiB, %d files", res.BytesWritten>>20, res.FileWrite.Count)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("left behind %v", entries)
	}
}

// Cleanup runs on a shared volume: a file or directory that merely matches a
// prefix, or holds a file with the marker's name, belongs to someone else.
func TestCleanupLeavesForeignFilesAlone(t *testing.T) {
	parent := t.TempDir()
	foreignSnap := filepath.Join(parent, snapshotPrefix+"-theirs.bin")
	foreignDir := filepath.Join(parent, loadDirPrefix+"theirs")
	os.WriteFile(foreignSnap, []byte("not ours"), 0o644)
	os.Mkdir(foreignDir, 0o755)
	os.WriteFile(filepath.Join(foreignDir, loadMarker), []byte("someone else's marker\n"), 0o644)
	os.WriteFile(filepath.Join(foreignDir, "results.bin"), []byte("not ours"), 0o644)
	var out strings.Builder
	if err := cmdCleanup(context.Background(), []string{"--skip-db", "--load-dir", parent}, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	for _, keep := range []string{foreignSnap, filepath.Join(foreignDir, loadMarker), filepath.Join(foreignDir, "results.bin")} {
		if _, err := os.Stat(keep); err != nil {
			t.Fatalf("removed %s\n%s", keep, out.String())
		}
	}
}
