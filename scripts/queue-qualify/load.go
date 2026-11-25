package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// The load generator stands in for bulk job I/O on one volume. It never
// deletes: each run creates one new directory of its own with an exclusive
// mkdir and a random run ID, writes a marker file and then only files inside
// that directory, and prints the directory and its size at the end. The
// storage owner removes it NAS-local. With no eviction the bytes on disk equal
// the bytes written, so --max-bytes caps both, exactly.
const (
	runDirPrefix   = "myce-qual-"
	runMarker      = "MYCE-QUALIFY-RUN"
	loadBlock      = 4 << 20
	maxLoadRuntime = time.Hour
	maxLoadWriters = 8
	// freeWatchEvery bounds how long other work can fill the volume before a
	// running file notices: at 1 GiB/s that is 256 MiB, far below any floor.
	// One statfs per interval costs the share almost nothing.
	freeWatchEvery = 250 * time.Millisecond
)

// loadFile is the part of *os.File the writers use, so tests can inject a
// call that blocks the way a hung SMB mount does.
type loadFile interface {
	io.Writer
	Sync() error
	Close() error
}

func createExclusive(path string) (loadFile, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
}

type loadOptions struct {
	dir        string
	kind       string // directory label: "load" or "snapshot"
	oneFile    string // when set, one writer writes this single file of maxBytes
	maxBytes   int64
	fileSize   int64
	minFree    int64
	minFreePct float64
	duration   time.Duration
	writers    int
	rateMiBps  float64
	readBack   bool
	seed       uint64
	free       func(string) (avail, total uint64, err error)
	create     func(string) (loadFile, error)
}

type loadResults struct {
	RunDir        string       `json:"run_dir"`
	BytesWritten  int64        `json:"bytes_written"`
	BytesRead     int64        `json:"bytes_read"`
	FilesComplete int          `json:"files_complete"`
	FilesPartial  []string     `json:"files_partial,omitempty"`
	Seconds       float64      `json:"seconds"`
	WriteMiBps    float64      `json:"write_MiBps"`
	ReadMiBps     float64      `json:"read_MiBps"`
	FileWrite     latencyStats `json:"file_write_and_fsync"`
	StoppedBy     string       `json:"stopped_by"`
	FreeAtStart   uint64       `json:"free_bytes_at_start"`
	FreeMinimum   uint64       `json:"free_bytes_minimum"`
	FloorBytes    uint64       `json:"free_bytes_floor"`

	// path is the run directory's full path. It goes to the terminal for the
	// storage owner, not into the report, which may be shared after sanitizing.
	path string
}

// leftover is the line the storage owner acts on.
func (r *loadResults) leftover() string {
	return leftoverLine(r.path, r.BytesWritten, r.FilesComplete+len(r.FilesPartial), len(r.FilesPartial))
}

func leftoverLine(path string, bytes int64, files, partial int) string {
	return fmt.Sprintf("left for NAS-local removal by the storage owner: %s (%d bytes in %d files, %d partial, plus the %s marker)",
		path, bytes, files, partial, runMarker)
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"TIB", 1 << 40}, {"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10}, {"B", 1}} {
		if v, ok := strings.CutSuffix(s, u.suffix); ok {
			s, mult = v, u.mult
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("bad size %q (use bytes or KiB/MiB/GiB/TiB)", s)
	}
	return n * mult, nil
}

func statfsFree(path string) (avail, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, probeErr(&os.PathError{Op: "statfs", Path: path, Err: err})
	}
	return uint64(st.Bavail) * uint64(st.Bsize), uint64(st.Blocks) * uint64(st.Bsize), nil
}

// floor is the free space the generator must leave behind.
func (o loadOptions) floor(total uint64) uint64 {
	return max(uint64(o.minFree), uint64(o.minFreePct/100*float64(total)))
}

func (o loadOptions) validate() error {
	switch {
	case o.duration <= 0 || o.duration > maxLoadRuntime:
		return fmt.Errorf("--duration must be in (0, %s]", maxLoadRuntime)
	case o.writers < 1 || o.writers > maxLoadWriters:
		return fmt.Errorf("--writers must be 1 to %d", maxLoadWriters)
	case o.oneFile == "" && o.fileSize < loadBlock:
		return errors.New("--file-size must be at least 4 MiB")
	case o.maxBytes <= 0:
		return errors.New("--max-bytes must be positive")
	case o.minFreePct < 0 || o.minFreePct >= 100:
		return errors.New("--min-free-pct must be in [0, 100)")
	}
	return nil
}

func cmdLoad(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("load", out)
	o := addOutputFlags(fs)
	opt := loadOptions{kind: "load", free: statfsFree, create: createExclusive}
	fs.StringVar(&opt.dir, "dir", "", "existing directory on the target volume; the run creates a new directory inside it (required)")
	maxBytes := fs.String("max-bytes", "16GiB", "most bytes to write, which is also what stays on disk until the storage owner removes the run directory")
	fileSize := fs.String("file-size", "512MiB", "size of each file, written and fsynced as a unit; the last file is cut to the cap")
	minFree := fs.String("min-free", "100GiB", "never leave less free space than this")
	fs.Float64Var(&opt.minFreePct, "min-free-pct", 10, "never leave less free space than this percentage")
	fs.DurationVar(&opt.duration, "duration", 10*time.Minute, "stop after this long (max 1h)")
	fs.IntVar(&opt.writers, "writers", 2, "concurrent writers (max 8)")
	fs.Float64Var(&opt.rateMiBps, "rate-mib", 0, "total write rate cap in MiB/s (0: no cap beyond the device and network)")
	fs.BoolVar(&opt.readBack, "read-back", true, "read each finished file back once, as jobs read their inputs")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var err error
	for _, p := range []struct {
		s   string
		dst *int64
	}{{*maxBytes, &opt.maxBytes}, {*fileSize, &opt.fileSize}, {*minFree, &opt.minFree}} {
		if *p.dst, err = parseBytes(p.s); err != nil {
			return err
		}
	}
	if opt.dir == "" {
		return errors.New("--dir is required")
	}
	r := newReport("load", o)
	r.Params["max_bytes"] = *maxBytes
	r.Params["file_size"] = *fileSize
	r.Params["min_free"] = fmt.Sprintf("%s or %.0f%%", *minFree, opt.minFreePct)
	r.Params["duration"] = opt.duration.String()
	r.Params["writers"] = opt.writers
	r.Params["rate_cap_MiBps"] = opt.rateMiBps
	res, err := runLoad(ctx, opt)
	if res == nil {
		return err
	}
	fmt.Fprintln(out, res.leftover())
	r.Results = res
	r.Checks = []check{
		{Name: "byte cap kept", Status: map[bool]string{true: statusPass, false: statusFail}[res.BytesWritten <= opt.maxBytes],
			Measured: fmt.Sprintf("%d bytes written", res.BytesWritten), Limit: fmt.Sprintf("<= %d", opt.maxBytes),
			Prevents: "leaving more test data on a shared production volume than the storage owner agreed to"},
		{Name: "free space floor kept", Status: map[bool]string{true: statusPass, false: statusFail}[res.FreeMinimum >= res.FloorBytes],
			Measured: fmt.Sprintf("%.1f GiB minimum free", float64(res.FreeMinimum)/(1<<30)), Limit: fmt.Sprintf(">= %.1f GiB", float64(res.FloorBytes)/(1<<30)),
			Prevents: "filling the volume that holds job inputs, outputs and checkpoints"},
		{Name: "load achieved", Status: statusInfo, Measured: fmt.Sprintf("%.1f MiB/s write, %.1f MiB/s read", res.WriteMiBps, res.ReadMiBps),
			Limit: "record the volume's busy percentage from the NAS monitor alongside", Prevents: "calling a run 'under load' without knowing how much load"},
		{Name: "left for NAS-local removal", Status: statusInfo,
			Measured: fmt.Sprintf("%s: %d bytes in %d files, %d partial", res.RunDir, res.BytesWritten, res.FilesComplete+len(res.FilesPartial), len(res.FilesPartial)),
			Limit:    "the storage owner removes the run directory", Prevents: "an agent deleting on the NAS"},
	}
	r.md = func(b *strings.Builder) {
		fmt.Fprintf(b, "\n## Load\n\nWrote %.2f GiB and read %.2f GiB in %.0f s (%.1f / %.1f MiB/s); stopped by %s; minimum free %.1f GiB. Run directory %s holds %d complete and %d partial files.\n",
			float64(res.BytesWritten)/(1<<30), float64(res.BytesRead)/(1<<30), res.Seconds, res.WriteMiBps, res.ReadMiBps, res.StoppedBy,
			float64(res.FreeMinimum)/(1<<30), res.RunDir, res.FilesComplete, len(res.FilesPartial))
		latencyTable(b, [][2]any{{"file write + fsync", res.FileWrite}})
	}
	if ferr := r.finish(out); ferr != nil {
		return ferr
	}
	return err
}

// newRunID is random so two runs, on one machine or several, never share a
// directory; the mkdir is exclusive in any case. Tests replace it.
var newRunID = func() string {
	b := make([]byte, 8)
	crand.Read(b)
	return hex.EncodeToString(b)
}

// newRunDir creates a run's directory exclusively and its marker. It never
// reuses or enters an existing directory. If the marker fails, the empty
// directory stays and its path comes back with the error.
func newRunDir(parent, kind string, maxBytes int64, ends time.Time) (string, error) {
	now := time.Now().UTC()
	name := fmt.Sprintf("%s%s-%s-%s", runDirPrefix, kind, now.Format("20060102T150405Z"), newRunID())
	dir := filepath.Join(parent, name)
	if err := os.Mkdir(dir, 0o755); err != nil {
		return "", probeErr(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, runMarker), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return dir, probeErr(err)
	}
	_, err = fmt.Fprintf(f, "queue-qualify %s run %s\nfrom %s/%s, started %s, ends by %s, capped at %d bytes\n"+
		"queue-qualify never deletes. The storage owner removes this whole directory NAS-local once the run has stopped.\n",
		kind, name, runtime.GOOS, runtime.GOARCH, now.Format(time.RFC3339), ends.UTC().Format(time.RFC3339), maxBytes)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return dir, probeErr(err)
	}
	return dir, nil
}

// fileRecord tracks one file in memory, so a forced exit can report the
// leftovers without touching the filesystem.
type fileRecord struct {
	name        string
	want, wrote int64
	done        bool
}

func runLoad(ctx context.Context, opt loadOptions) (*loadResults, error) {
	if opt.kind == "" {
		opt.kind = "load"
	}
	if opt.create == nil {
		opt.create = createExclusive
	}
	if opt.oneFile != "" {
		opt.writers, opt.fileSize = 1, opt.maxBytes
	}
	if err := opt.validate(); err != nil {
		return nil, err
	}
	info, err := os.Stat(opt.dir)
	if err != nil || !info.IsDir() {
		return nil, errors.New("--dir must be an existing directory")
	}
	avail, total, err := opt.free(opt.dir)
	if err != nil {
		return nil, err
	}
	floor := opt.floor(total)
	// The run counts its own bytes as used whether or not the share's free
	// space shows them yet, so writing the whole cap needs twice the cap above
	// the floor. Refusing here beats a run that stops itself halfway.
	if avail < floor+2*uint64(opt.maxBytes) {
		return nil, fmt.Errorf("refusing: %.1f GiB free, need twice the %.1f GiB cap above a %.1f GiB floor",
			float64(avail)/(1<<30), float64(opt.maxBytes)/(1<<30), float64(floor)/(1<<30))
	}
	path, err := newRunDir(opt.dir, opt.kind, opt.maxBytes, time.Now().Add(opt.duration))
	if err != nil {
		if path != "" {
			return nil, fmt.Errorf("%w; %s", err, leftoverLine(path, 0, 0, 0))
		}
		return nil, err
	}
	res := &loadResults{RunDir: filepath.Base(path), path: path, FreeAtStart: avail, FreeMinimum: avail, FloorBytes: floor}

	var (
		written, read atomic.Int64
		filesMu       sync.Mutex
		files         []*fileRecord
	)
	tally := func() (complete int, partial []string) {
		filesMu.Lock()
		defer filesMu.Unlock()
		for _, f := range files {
			if f.done {
				complete++
			} else {
				partial = append(partial, f.name)
			}
		}
		return complete, partial
	}
	forget := guard.note(func() string {
		complete, partial := tally()
		return leftoverLine(path, written.Load(), complete+len(partial), len(partial))
	})
	defer forget()

	runCtx, stop := context.WithTimeout(ctx, opt.duration)
	defer stop()
	// The duration limit, a floor stop or Ctrl-C all end in runCtx; a writer
	// stuck in a hung mount must not hold the process past the grace.
	disarm := guard.arm(runCtx)
	defer disarm()
	var (
		mu         sync.Mutex
		fileLat    []float64
		stopReason string
		firstErr   error
	)
	halt := func(reason string, err error) {
		mu.Lock()
		if stopReason == "" {
			stopReason, firstErr = reason, err
		}
		mu.Unlock()
		stop()
	}
	budget := &loadBudget{maxBytes: opt.maxBytes, fileSize: opt.fileSize, floor: floor, freeMin: avail}
	limiter := newRateLimiter(opt.rateMiBps * (1 << 20))
	start := time.Now()

	// The watcher catches other work filling the volume while files are
	// being written; reservations alone check only between files. It reads
	// free space outside the reservation lock, so a hung statfs blocks only
	// itself.
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		tick := time.NewTicker(freeWatchEvery)
		defer tick.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-tick.C:
			}
			avail, _, err := opt.free(opt.dir)
			if err != nil {
				halt("statfs-error", err)
				return
			}
			if !budget.holds(avail) {
				halt("free-space-floor", nil)
				return
			}
		}
	}()

	seed := opt.seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	var capReached atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < opt.writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed+uint64(w), 5))
			buf := make([]byte, loadBlock)
			for n := 0; runCtx.Err() == nil; n++ {
				name := opt.oneFile
				if name == "" {
					name = fmt.Sprintf("w%d-%06d.bin", w, n)
				}
				avail, _, err := opt.free(opt.dir)
				if err != nil {
					halt("statfs-error", err)
					return
				}
				size, reason := budget.reserve(avail)
				if reason == "max-bytes" {
					// The cap is fully reserved; other writers finish their
					// files, and the run ends when they do.
					capReached.Store(true)
					return
				}
				if reason != "" {
					halt(reason, nil)
					return
				}
				rec := &fileRecord{name: name, want: size}
				filesMu.Lock()
				files = append(files, rec)
				filesMu.Unlock()
				p := filepath.Join(path, name)
				began := time.Now()
				err = writeLoadFile(runCtx, opt.create, p, size, buf, rng, limiter, func(n int64) {
					written.Add(n)
					filesMu.Lock()
					rec.wrote += n
					filesMu.Unlock()
				})
				if err != nil {
					if runCtx.Err() == nil {
						halt("write-error", err)
					}
					return
				}
				filesMu.Lock()
				rec.done = true
				filesMu.Unlock()
				mu.Lock()
				fileLat = append(fileLat, ms(time.Since(began)))
				mu.Unlock()
				if opt.readBack {
					if err := readLoadFile(runCtx, p, buf, &read); err != nil && runCtx.Err() == nil {
						halt("read-error", err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	stop()
	<-watchDone
	res.Seconds = time.Since(start).Seconds()
	res.FreeMinimum = min(res.FreeMinimum, budget.minimum())
	// Every file is still on disk here, so this is the run's low point.
	if avail, _, err := opt.free(opt.dir); err == nil {
		res.FreeMinimum = min(res.FreeMinimum, avail)
	}
	res.BytesWritten, res.BytesRead = written.Load(), read.Load()
	res.FilesComplete, res.FilesPartial = tally()
	if res.Seconds > 0 {
		res.WriteMiBps = float64(res.BytesWritten) / (1 << 20) / res.Seconds
		res.ReadMiBps = float64(res.BytesRead) / (1 << 20) / res.Seconds
	}
	res.FileWrite = summarize(fileLat, 0)
	switch {
	case stopReason != "":
		res.StoppedBy = stopReason
	case capReached.Load():
		res.StoppedBy = "max-bytes"
	case ctx.Err() != nil:
		res.StoppedBy = "interrupted"
	default:
		res.StoppedBy = "duration"
	}
	if firstErr != nil {
		return res, probeErr(firstErr)
	}
	return res, nil
}

// loadBudget is the one reservation every writer takes before creating a
// file. reserved counts every byte the run has written or may still write,
// and it is never released: with no eviction nothing the run writes leaves
// the disk until the storage owner removes it. The lock covers arithmetic
// only; free space is read before taking it.
type loadBudget struct {
	maxBytes, fileSize int64
	floor              uint64

	mu       sync.Mutex
	reserved int64
	freeMin  uint64
}

// reserve claims the next file's bytes, cut to what is left of the cap, or
// names why the run must stop. avail is free space read just before the
// call. The run's own bytes count against it in full, whether or not the
// share reports them yet: free space over SMB can trail the client's write
// cache, and a reading taken before another writer's reservation is still
// safe, because that reservation is counted here.
func (b *loadBudget) reserve(avail uint64) (int64, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.freeMin = min(b.freeMin, avail)
	size := min(b.fileSize, b.maxBytes-b.reserved)
	if size <= 0 {
		return 0, "max-bytes"
	}
	if avail < b.floor+uint64(b.reserved+size) {
		return 0, "free-space-floor"
	}
	b.reserved += size
	return size, ""
}

// holds is the watcher's test: free space must still cover everything the
// run has reserved above the floor. Only other work, or the share's report
// catching up with the run's own bytes faster than the cap allows for, can
// trip it.
func (b *loadBudget) holds(avail uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.freeMin = min(b.freeMin, avail)
	return avail >= b.floor+uint64(b.reserved)
}

func (b *loadBudget) minimum() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.freeMin
}

func writeLoadFile(ctx context.Context, create func(string) (loadFile, error), path string, size int64, buf []byte, rng *rand.Rand, limiter *rateLimiter, wrote func(int64)) error {
	f, err := create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for left := size; left > 0; {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n := min(left, int64(len(buf)))
		// Fresh random bytes defeat compression and deduplication on the NAS.
		for i := 0; i < int(n); i += 8 {
			v := rng.Uint64()
			for j := 0; j < 8 && i+j < int(n); j++ {
				buf[i+j] = byte(v >> (8 * j))
			}
		}
		if !limiter.wait(ctx, n) {
			return ctx.Err()
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return err
		}
		wrote(n)
		left -= n
	}
	// Sync forces the bytes to the server's disk rather than the client cache.
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

func readLoadFile(ctx context.Context, path string, buf []byte, read *atomic.Int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for ctx.Err() == nil {
		n, err := f.Read(buf)
		read.Add(int64(n))
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

// rateLimiter is a token bucket shared by all writers; zero means no cap.
type rateLimiter struct {
	mu     sync.Mutex
	rate   float64
	tokens float64
	last   time.Time
}

func newRateLimiter(bytesPerSec float64) *rateLimiter {
	return &rateLimiter{rate: bytesPerSec, last: time.Now()}
}

func (l *rateLimiter) wait(ctx context.Context, n int64) bool {
	if l.rate <= 0 {
		return ctx.Err() == nil
	}
	l.mu.Lock()
	now := time.Now()
	l.tokens = min(l.tokens+now.Sub(l.last).Seconds()*l.rate, l.rate)
	l.last = now
	l.tokens -= float64(n)
	debt := l.tokens
	l.mu.Unlock()
	if debt >= 0 {
		return ctx.Err() == nil
	}
	return sleepUntil(ctx, now.Add(time.Duration(-debt/l.rate*float64(time.Second))))
}
