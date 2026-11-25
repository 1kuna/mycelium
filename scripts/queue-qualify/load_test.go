package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	return loadOptions{dir: dir, maxBytes: 24 << 20, fileSize: 4 << 20, minFree: 1 << 30,
		minFreePct: 10, duration: 20 * time.Second, writers: 2, readBack: true, free: free}
}

func plentyFree(string) (uint64, uint64, error) { return 900 << 30, 1000 << 30, nil }

// runDirOf returns the one run directory a load created under parent.
func runDirOf(t *testing.T, parent string) string {
	t.Helper()
	matches, _ := filepath.Glob(filepath.Join(parent, runDirPrefix+"*"))
	if len(matches) != 1 {
		t.Fatalf("want one run directory, found %v", matches)
	}
	return matches[0]
}

// binBytes sums the load files under dir and counts them.
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

// A load writes only inside one new directory of its own, created with an
// exclusive mkdir, and never removes anything: everything it wrote is still
// there afterwards, and what was in the parent before is untouched.
func TestLoadWritesOnlyInsideItsOwnNewDirectory(t *testing.T) {
	parent := t.TempDir()
	os.WriteFile(filepath.Join(parent, "jobs.bin"), []byte("someone else's"), 0o644)
	os.Mkdir(filepath.Join(parent, runDirPrefix+"load-theirs"), 0o755)
	var mu sync.Mutex
	var created []string
	opt := smallLoad(parent, plentyFree)
	opt.create = func(p string) (loadFile, error) {
		mu.Lock()
		created = append(created, p)
		mu.Unlock()
		return createExclusive(p)
	}
	res, err := runLoad(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(parent)
	if len(entries) != 3 {
		t.Fatalf("parent holds %v; want the two old entries and one run directory", entries)
	}
	if b, _ := os.ReadFile(filepath.Join(parent, "jobs.bin")); string(b) != "someone else's" {
		t.Fatal("touched a file outside the run directory")
	}
	if e, _ := os.ReadDir(filepath.Join(parent, runDirPrefix+"load-theirs")); len(e) != 0 {
		t.Fatal("wrote into another run's directory")
	}
	dir := filepath.Join(parent, res.RunDir)
	if res.path != dir || !strings.Contains(res.leftover(), dir) {
		t.Fatalf("run directory %q, leftover %q", res.path, res.leftover())
	}
	for _, p := range created {
		if filepath.Dir(p) != dir {
			t.Fatalf("created %s outside %s", p, dir)
		}
	}
	marker, err := os.ReadFile(filepath.Join(dir, runMarker))
	if err != nil || !strings.Contains(string(marker), "storage owner removes") {
		t.Fatalf("marker %q %v", marker, err)
	}
	files, bytes := binBytes(dir)
	if files != len(created) || bytes != res.BytesWritten || res.FilesComplete != files || len(res.FilesPartial) != 0 {
		t.Fatalf("on disk %d files %d bytes; result %+v; created %d", files, bytes, res, len(created))
	}

	// The mkdir is exclusive: a run whose ID collides refuses and enters
	// nothing.
	old := newRunID
	defer func() { newRunID = old }()
	newRunID = func() string { return "fixed" }
	first, err := newRunDir(parent, "load", 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(first, "theirs.txt"), []byte("x"), 0o644)
	if _, err := newRunDir(parent, "load", 1, time.Now()); err == nil {
		t.Fatal("a second run reused an existing directory")
	}
	if e, _ := os.ReadDir(first); len(e) != 2 {
		t.Fatalf("the colliding run changed the existing directory: %v", e)
	}
}

// --max-bytes caps the bytes on disk exactly, across writers, and the last
// file is cut to fit. A one-byte cap writes one byte.
func TestLoadNeverExceedsMaxBytes(t *testing.T) {
	for _, c := range []struct {
		max     int64
		writers int
	}{{1, 1}, {4<<20 + 1, 2}, {10 << 20, 3}, {37<<20 + 5, 8}} {
		parent := t.TempDir()
		opt := smallLoad(parent, plentyFree)
		opt.maxBytes, opt.writers = c.max, c.writers
		res, err := runLoad(context.Background(), opt)
		if err != nil {
			t.Fatal(err)
		}
		_, bytes := binBytes(runDirOf(t, parent))
		if res.StoppedBy != "max-bytes" || bytes != c.max || res.BytesWritten != c.max {
			t.Fatalf("cap %d with %d writers: %d bytes on disk, result %+v", c.max, c.writers, bytes, res)
		}
	}
}

func TestLoadRefusesOrStopsAtTheFreeSpaceFloor(t *testing.T) {
	dir := t.TempDir()
	// 100 GiB free of 1 TiB: the 10% floor (102.4 GiB) is already crossed.
	tight := func(string) (uint64, uint64, error) { return 100 << 30, 1 << 40, nil }
	if _, err := runLoad(context.Background(), smallLoad(dir, tight)); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("not refused: %v", err)
	}
	// The cap must fit twice above the floor: the run counts its own bytes
	// whether or not the share reports them yet.
	justOver := func(string) (uint64, uint64, error) { return 100<<30 + 30<<20, 1000 << 30, nil }
	if _, err := runLoad(context.Background(), smallLoad(dir, justOver)); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("a cap that fits only once was not refused: %v", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a refused run created %v", entries)
	}
	// Free space shrinks as other work writes; the generator stops itself
	// and leaves what it wrote for the storage owner.
	var calls atomic.Int64
	shrinking := func(string) (uint64, uint64, error) {
		n := uint64(calls.Add(1))
		return (200 << 30) - n*(20<<30), 1000 << 30, nil
	}
	opt := smallLoad(dir, shrinking)
	opt.maxBytes = 1 << 30
	res, err := runLoad(context.Background(), opt)
	if err != nil || res.StoppedBy != "free-space-floor" {
		t.Fatalf("%+v %v", res, err)
	}
	if _, bytes := binBytes(runDirOf(t, dir)); bytes != res.BytesWritten {
		t.Fatalf("on disk %d, reported %d", bytes, res.BytesWritten)
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

// Eight writers read free space outside the reservation lock and race to
// reserve with the same stale reading. Because every reservation counts in
// full and is never released, only the files that fit above the floor
// start.
func TestLoadWritersShareOneFreeSpaceReservation(t *testing.T) {
	const file = 4 << 20
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
	// Room for twice the cap at start; then other work leaves room for one
	// and a half files above the floor. The first readings wait until all
	// eight writers have one, so they race exactly as on a slow share.
	free := func(string) (uint64, uint64, error) {
		n := calls.Add(1)
		if n == 1 {
			return floor + 8*file, 10 << 30, nil
		}
		if n <= 9 {
			arrived <- struct{}{}
			<-release
		}
		return floor + file*3/2, 10 << 30, nil
	}
	opt := loadOptions{dir: dir, maxBytes: 4 * file, fileSize: file, minFree: 1 << 30,
		minFreePct: 10, duration: 20 * time.Second, writers: 8, rateMiBps: 32, free: free}
	stop := make(chan struct{})
	peak := peakOnDisk(dir, stop)
	res, err := runLoad(context.Background(), opt)
	close(stop)
	if err != nil || res.StoppedBy != "free-space-floor" {
		t.Fatalf("%+v %v", res, err)
	}
	if p := <-peak; p[0] > 1 || p[1] > file {
		t.Fatalf("%d files (%d MiB) started with room for one and a half above the floor", p[0], p[1]>>20)
	}
}

// Other work can fill the volume while a large file is being written; the
// generator must notice during the write, not after it, and leave the
// partial file for the storage owner rather than delete it.
func TestLoadStopsWhenTheFloorIsCrossedMidWrite(t *testing.T) {
	dir := t.TempDir()
	free := func(string) (uint64, uint64, error) {
		if _, used := binBytes(dir); used >= 8<<20 {
			return 50 << 30, 1000 << 30, nil // below the 100 GiB floor
		}
		return 900 << 30, 1000 << 30, nil
	}
	opt := loadOptions{dir: dir, maxBytes: 1 << 30, fileSize: 64 << 20, minFree: 1 << 30,
		minFreePct: 10, duration: 20 * time.Second, writers: 1, rateMiBps: 32, free: free}
	res, err := runLoad(context.Background(), opt)
	if err != nil || res.StoppedBy != "free-space-floor" {
		t.Fatalf("%+v %v", res, err)
	}
	if res.BytesWritten >= 64<<20 || res.FileWrite.Count != 0 {
		t.Fatalf("the 64 MiB file finished after the floor was crossed: wrote %d MiB, %d files", res.BytesWritten>>20, res.FileWrite.Count)
	}
	files, bytes := binBytes(runDirOf(t, dir))
	if files != 1 || bytes != res.BytesWritten || len(res.FilesPartial) != 1 || !strings.Contains(res.leftover(), "1 partial") {
		t.Fatalf("partial file not left and reported: %d files %d bytes, %+v", files, bytes, res)
	}
}

// Ctrl-C, or the duration limit, while a writer is stuck in a filesystem
// call: the process exits within the grace and lists what it left.
func TestStopExitsWithinTheGraceWhileAFileCallIsBlocked(t *testing.T) {
	for _, c := range []struct {
		name     string
		duration time.Duration
		cancel   bool
	}{{"interrupt", 20 * time.Second, true}, {"duration", 300 * time.Millisecond, false}} {
		t.Run(c.name, func(t *testing.T) {
			const grace = 200 * time.Millisecond
			var out strings.Builder
			var outMu sync.Mutex
			exited := make(chan int, 2)
			old := guard
			guard = newStopGuard(grace, func(code int) { exited <- code }, writerFunc(func(p []byte) (int, error) {
				outMu.Lock()
				defer outMu.Unlock()
				return out.Write(p)
			}))
			defer func() { guard = old }()
			release := make(chan struct{})
			defer close(release)
			blocked := make(chan struct{}, 8)
			opt := smallLoad(t.TempDir(), plentyFree)
			opt.duration = c.duration
			// Writer 0 has a healthy disk; writer 1's create hangs the way an
			// open on a stalled SMB mount does, until the test ends.
			opt.create = func(p string) (loadFile, error) {
				blocked <- struct{}{}
				if strings.Contains(filepath.Base(p), "w1-") {
					<-release
					return nil, errors.New("released")
				}
				return createExclusive(p)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go runLoad(ctx, opt)
			<-blocked
			<-blocked
			time.Sleep(50 * time.Millisecond)
			stopped := time.Now()
			if c.cancel {
				cancel()
			} else {
				stopped = time.Now().Add(c.duration - 50*time.Millisecond)
			}
			select {
			case code := <-exited:
				if took := time.Since(stopped); took > grace+time.Second {
					t.Fatalf("exited %s after the stop; the grace is %s", took, grace)
				}
				if code == 0 {
					t.Fatal("a forced exit reported success")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("still running five seconds after the stop with a blocked file call")
			}
			outMu.Lock()
			msg := out.String()
			outMu.Unlock()
			if !strings.Contains(msg, "stopped by force") || !strings.Contains(msg, runDirPrefix+"load-") || !strings.Contains(msg, "partial") {
				t.Fatalf("forced exit did not list the leftovers: %q", msg)
			}
		})
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
