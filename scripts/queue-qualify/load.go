package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// The load generator stands in for bulk job I/O on one volume. Every bound
// is explicit: total bytes, bytes on disk at once, duration, a free-space
// floor and a rate cap. Writers reserve a file's bytes before creating it, so
// the working set and the floor hold across all writers together, and a
// watcher stops the run when other work fills the volume mid-file. It writes
// only inside a new directory whose manifest lists every file it creates,
// and removes exactly those on exit, including on interrupt.
const (
	loadDirPrefix  = "myce-qual-load-"
	loadMarker     = "MYCE-QUALIFY-LOAD"
	loadBlock      = 4 << 20
	maxLoadRuntime = time.Hour
	maxLoadWriters = 8
	// freeWatchEvery bounds how long other work can fill the volume before a
	// running file notices: at 1 GiB/s that is 256 MiB, far below any floor.
	// One statfs per interval costs the share almost nothing.
	freeWatchEvery = 250 * time.Millisecond
)

type loadOptions struct {
	dir                  string
	maxBytes, workingSet int64
	fileSize             int64
	minFree              int64
	minFreePct           float64
	duration             time.Duration
	writers              int
	rateMiBps            float64
	readBack             bool
	free                 func(string) (avail, total uint64, err error)
}

type loadResults struct {
	BytesWritten int64        `json:"bytes_written"`
	BytesRead    int64        `json:"bytes_read"`
	Seconds      float64      `json:"seconds"`
	WriteMiBps   float64      `json:"write_MiBps"`
	ReadMiBps    float64      `json:"read_MiBps"`
	FileWrite    latencyStats `json:"file_write_and_fsync"`
	StoppedBy    string       `json:"stopped_by"`
	FreeAtStart  uint64       `json:"free_bytes_at_start"`
	FreeMinimum  uint64       `json:"free_bytes_minimum"`
	FloorBytes   uint64       `json:"free_bytes_floor"`
	CleanedUp    bool         `json:"cleaned_up"`
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
	case o.fileSize < loadBlock:
		return errors.New("--file-size must be at least 4 MiB")
	case o.workingSet < o.fileSize*int64(o.writers):
		return errors.New("--working-set must hold at least one file per writer")
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
	opt := loadOptions{free: statfsFree}
	fs.StringVar(&opt.dir, "dir", "", "existing directory on the target volume; a marked subdirectory is created inside it (required)")
	maxBytes := fs.String("max-bytes", "32GiB", "stop after writing this much in total")
	workingSet := fs.String("working-set", "4GiB", "most bytes on disk at once; oldest files are deleted first")
	fileSize := fs.String("file-size", "512MiB", "size of each file, written and fsynced as a unit")
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
	}{{*maxBytes, &opt.maxBytes}, {*workingSet, &opt.workingSet}, {*fileSize, &opt.fileSize}, {*minFree, &opt.minFree}} {
		if *p.dst, err = parseBytes(p.s); err != nil {
			return err
		}
	}
	if opt.dir == "" {
		return errors.New("--dir is required")
	}
	r := newReport("load", o)
	r.Params["max_bytes"] = *maxBytes
	r.Params["working_set"] = *workingSet
	r.Params["file_size"] = *fileSize
	r.Params["min_free"] = fmt.Sprintf("%s or %.0f%%", *minFree, opt.minFreePct)
	r.Params["duration"] = opt.duration.String()
	r.Params["writers"] = opt.writers
	r.Params["rate_cap_MiBps"] = opt.rateMiBps
	res, err := runLoad(ctx, opt)
	if res == nil {
		return err
	}
	r.Results = res
	r.Checks = []check{
		{Name: "load directory removed", Status: map[bool]string{true: statusPass, false: statusFail}[res.CleanedUp], Measured: fmt.Sprint(res.CleanedUp),
			Limit: "true", Prevents: "leaving gigabytes of test data on a shared production volume"},
		{Name: "free space floor kept", Status: map[bool]string{true: statusPass, false: statusFail}[res.FreeMinimum >= res.FloorBytes],
			Measured: fmt.Sprintf("%.1f GiB minimum free", float64(res.FreeMinimum)/(1<<30)), Limit: fmt.Sprintf(">= %.1f GiB", float64(res.FloorBytes)/(1<<30)),
			Prevents: "filling the volume that holds job inputs, outputs and checkpoints"},
		{Name: "load achieved", Status: statusInfo, Measured: fmt.Sprintf("%.1f MiB/s write, %.1f MiB/s read", res.WriteMiBps, res.ReadMiBps),
			Limit: "record the volume's busy percentage from the NAS monitor alongside", Prevents: "calling a run 'under load' without knowing how much load"},
	}
	r.md = func(b *strings.Builder) {
		fmt.Fprintf(b, "\n## Load\n\nWrote %.2f GiB and read %.2f GiB in %.0f s (%.1f / %.1f MiB/s); stopped by %s; minimum free %.1f GiB.\n",
			float64(res.BytesWritten)/(1<<30), float64(res.BytesRead)/(1<<30), res.Seconds, res.WriteMiBps, res.ReadMiBps, res.StoppedBy, float64(res.FreeMinimum)/(1<<30))
		latencyTable(b, [][2]any{{"file write + fsync", res.FileWrite}})
	}
	if ferr := r.finish(out); ferr != nil {
		return ferr
	}
	return err
}

func runLoad(ctx context.Context, opt loadOptions) (*loadResults, error) {
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
	if avail < floor+uint64(opt.workingSet) {
		return nil, fmt.Errorf("refusing: %.1f GiB free, need %.1f GiB working set above a %.1f GiB floor",
			float64(avail)/(1<<30), float64(opt.workingSet)/(1<<30), float64(floor)/(1<<30))
	}
	run, err := newLoadRun(opt.dir, opt.duration)
	if err != nil {
		return nil, probeErr(err)
	}
	res := &loadResults{FreeAtStart: avail, FreeMinimum: avail, FloorBytes: floor}
	defer func() {
		_, err := run.remove()
		res.CleanedUp = err == nil
	}()

	runCtx, stop := context.WithTimeout(ctx, opt.duration)
	defer stop()
	var (
		written, read atomic.Int64
		mu            sync.Mutex
		fileLat       []float64
		stopReason    string
		firstErr      error
	)
	halt := func(reason string, err error) {
		mu.Lock()
		if stopReason == "" {
			stopReason, firstErr = reason, err
		}
		mu.Unlock()
		stop()
	}
	budget := &loadBudget{opt: opt, run: run, freeMin: avail}
	limiter := newRateLimiter(opt.rateMiBps * (1 << 20))
	start := time.Now()

	// The watcher catches other work filling the volume while files are
	// being written; reservations alone check only between files.
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
			if reason, err := budget.check(); reason != "" {
				halt(reason, err)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < opt.writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w)+1, uint64(time.Now().UnixNano())))
			buf := make([]byte, loadBlock)
			for n := 0; runCtx.Err() == nil; n++ {
				if written.Load() >= opt.maxBytes {
					halt("max-bytes", nil)
					return
				}
				name := fmt.Sprintf("w%d-%06d.bin", w, n)
				if reason, err := budget.reserve(name); reason != "" {
					halt(reason, err)
					return
				}
				path := filepath.Join(run.dir, name)
				began := time.Now()
				var done int64
				err := writeLoadFile(runCtx, path, opt.fileSize, buf, rng, limiter, func(n int64) {
					written.Add(n)
					budget.unwritten.Add(-n)
					done += n
				})
				if runCtx.Err() != nil || err != nil {
					budget.abandon(name, opt.fileSize-done)
					if runCtx.Err() == nil {
						halt("write-error", err)
					}
					return
				}
				mu.Lock()
				fileLat = append(fileLat, ms(time.Since(began)))
				mu.Unlock()
				budget.complete(name)
				if opt.readBack {
					if err := readLoadFile(runCtx, path, buf, &read); err != nil && runCtx.Err() == nil && !errors.Is(err, os.ErrNotExist) {
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
	// The last files are still on disk here, so this is the run's low point.
	if avail, _, err := opt.free(opt.dir); err == nil {
		res.FreeMinimum = min(res.FreeMinimum, avail)
	}
	res.BytesWritten, res.BytesRead = written.Load(), read.Load()
	if res.Seconds > 0 {
		res.WriteMiBps = float64(res.BytesWritten) / (1 << 20) / res.Seconds
		res.ReadMiBps = float64(res.BytesRead) / (1 << 20) / res.Seconds
	}
	res.FileWrite = summarize(fileLat, 0)
	switch {
	case stopReason != "":
		res.StoppedBy = stopReason
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
// file. held counts files on disk and files being written at their full
// size, so it never exceeds the working set. unwritten counts the bytes
// in-flight files have still to write, which free space must also cover.
type loadBudget struct {
	opt       loadOptions
	run       *loadRun
	mu        sync.Mutex
	done      []string // finished files, oldest first
	held      int64
	unwritten atomic.Int64
	freeMin   uint64
}

// reserve makes room for one more file or names why the run must stop. It
// evicts the oldest finished files for the working set, then requires free
// space for this file and every unwritten byte of the others above the floor.
// It holds the lock across statfs so two writers never claim the same room.
func (b *loadBudget) reserve(name string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	size := b.opt.fileSize
	for b.held+size > b.opt.workingSet && len(b.done) > 0 {
		if err := os.Remove(filepath.Join(b.run.dir, b.done[0])); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "evict-error", err
		}
		b.done = b.done[1:]
		b.held -= size
	}
	if b.held+size > b.opt.workingSet {
		// validate keeps one file per writer inside the working set.
		return "working-set", errors.New("no room in the working set")
	}
	avail, total, err := b.opt.free(b.opt.dir)
	if err != nil {
		return "statfs-error", err
	}
	b.freeMin = min(b.freeMin, avail)
	if avail < b.opt.floor(total)+uint64(max(b.unwritten.Load(), 0)+size) {
		return "free-space-floor", nil
	}
	// The manifest names the file before it exists, so cleanup after a hard
	// kill can find it.
	if err := b.run.add(name); err != nil {
		return "manifest-error", err
	}
	b.held += size
	b.unwritten.Add(size)
	return "", nil
}

// check is the watcher's test between reservations: stop once free space
// cannot cover the bytes still to be written above the floor. Our own writes
// lower free space and unwritten together, so only other work trips it.
func (b *loadBudget) check() (string, error) {
	avail, total, err := b.opt.free(b.opt.dir)
	if err != nil {
		return "statfs-error", err
	}
	b.mu.Lock()
	b.freeMin = min(b.freeMin, avail)
	b.mu.Unlock()
	if avail < b.opt.floor(total)+uint64(max(b.unwritten.Load(), 0)) {
		return "free-space-floor", nil
	}
	return "", nil
}

func (b *loadBudget) complete(name string) {
	b.mu.Lock()
	b.done = append(b.done, name)
	b.mu.Unlock()
}

// abandon removes a file that will not finish and releases its reservation.
func (b *loadBudget) abandon(name string, unwritten int64) {
	os.Remove(filepath.Join(b.run.dir, name))
	b.unwritten.Add(-unwritten)
	b.mu.Lock()
	b.held -= b.opt.fileSize
	b.mu.Unlock()
}

func (b *loadBudget) minimum() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.freeMin
}

func writeLoadFile(ctx context.Context, path string, size int64, buf []byte, rng *rand.Rand, limiter *rateLimiter, wrote func(int64)) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
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
		for i := 0; i+8 <= int(n); i += 8 {
			v := rng.Uint64()
			buf[i], buf[i+1], buf[i+2], buf[i+3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
			buf[i+4], buf[i+5], buf[i+6], buf[i+7] = byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56)
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

// A load run's manifest is its marker file. Its first line names the run's
// own directory, and each later line names one file the run created, written
// and synced before the file exists. Cleanup trusts nothing else: it removes
// the listed files, the manifest and then the directory only if it is empty.
const manifestHeader = "queue-qualify load run "

type loadRun struct {
	dir      string
	manifest *os.File
	mu       sync.Mutex
	files    []string
}

func newLoadRun(parent string, duration time.Duration) (*loadRun, error) {
	dir, err := os.MkdirTemp(parent, loadDirPrefix)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, loadMarker), os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		os.Remove(dir)
		return nil, err
	}
	r := &loadRun{dir: dir, manifest: f}
	head := fmt.Sprintf("%s%s\nbulk load test data, safe to delete. Started %s, ends by %s.\n", manifestHeader, filepath.Base(dir),
		time.Now().UTC().Format(time.RFC3339), time.Now().Add(duration).UTC().Format(time.RFC3339))
	if err := r.append(head); err != nil {
		r.remove()
		return nil, err
	}
	return r, nil
}

func (r *loadRun) append(text string) error {
	if _, err := r.manifest.WriteString(text); err != nil {
		return err
	}
	return r.manifest.Sync()
}

func (r *loadRun) add(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.append("file " + name + "\n"); err != nil {
		return err
	}
	r.files = append(r.files, name)
	return nil
}

// remove deletes this run's files and directory, as cleanup would.
func (r *loadRun) remove() ([]string, error) {
	r.manifest.Close()
	return removeLoadRun(r.dir)
}

// readManifest returns the files a run directory's manifest lists, or false
// when the directory has no manifest this tool wrote for it.
func readManifest(dir string) ([]string, bool) {
	b, err := os.ReadFile(filepath.Join(dir, loadMarker))
	if err != nil {
		return nil, false
	}
	lines := strings.Split(string(b), "\n")
	if lines[0] != manifestHeader+filepath.Base(dir) {
		return nil, false
	}
	var files []string
	for _, line := range lines[1:] {
		name, ok := strings.CutPrefix(line, "file ")
		// Only names this tool generates; never a path out of the directory.
		if ok && strings.HasSuffix(name, ".bin") && name == filepath.Base(name) && !strings.HasPrefix(name, ".") {
			files = append(files, name)
		}
	}
	return files, true
}

func removeLoadRun(dir string) ([]string, error) {
	files, ok := readManifest(dir)
	if !ok {
		return nil, fmt.Errorf("%s has no manifest from this tool; left alone", filepath.Base(dir))
	}
	var removed []string
	for _, name := range files {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, err
		}
		removed = append(removed, name)
	}
	if entries, err := os.ReadDir(dir); err != nil {
		return removed, err
	} else if len(entries) > 1 {
		return removed, fmt.Errorf("%s holds %d files this tool did not create; left them and the directory", filepath.Base(dir), len(entries)-1)
	}
	if err := os.Remove(filepath.Join(dir, loadMarker)); err != nil {
		return removed, err
	}
	return removed, os.Remove(dir)
}

// removeLoadLeftovers deletes only what this tool created under parent: run
// directories through their manifests, and snapshot files whose ownership
// record still matches them. Anything else with a matching name is reported
// and left alone.
func removeLoadLeftovers(parent string) (removed, kept []string, err error) {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil, nil, probeErr(err)
	}
	var errs []error
	for _, e := range entries {
		path := filepath.Join(parent, e.Name())
		switch {
		case e.Type().IsRegular() && strings.HasPrefix(e.Name(), snapshotPrefix) && !strings.HasSuffix(e.Name(), snapshotOwnerSuffix):
			ok, err := removeSnapshot(path)
			switch {
			case err != nil:
				errs = append(errs, fmt.Errorf("%s: %w", e.Name(), pathless(err)))
			case ok:
				removed = append(removed, e.Name())
			default:
				kept = append(kept, e.Name()+" (no matching ownership record)")
			}
		case e.IsDir() && strings.HasPrefix(e.Name(), loadDirPrefix):
			if _, ok := readManifest(path); !ok {
				kept = append(kept, e.Name()+" (no manifest from this tool)")
				continue
			}
			files, err := removeLoadRun(path)
			for _, f := range files {
				removed = append(removed, filepath.Join(e.Name(), f))
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", e.Name(), pathless(err)))
				continue
			}
			removed = append(removed, e.Name())
		}
	}
	return removed, kept, errors.Join(errs...)
}

// pathless drops the path from a file error, which can name a private share,
// and keeps this tool's own messages, which name only run directories.
func pathless(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}
