package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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
// floor and a rate cap. It writes only inside a new, marked directory and
// removes it on exit, including on interrupt.
const (
	loadDirPrefix  = "myce-qual-load-"
	loadMarker     = "MYCE-QUALIFY-LOAD"
	loadBlock      = 4 << 20
	maxLoadRuntime = time.Hour
	maxLoadWriters = 8
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
	runDir, err := os.MkdirTemp(opt.dir, loadDirPrefix)
	if err != nil {
		return nil, probeErr(err)
	}
	res := &loadResults{FreeAtStart: avail, FreeMinimum: avail, FloorBytes: floor}
	defer func() {
		res.CleanedUp = os.RemoveAll(runDir) == nil
		if _, err := os.Stat(runDir); err == nil {
			res.CleanedUp = false
		}
	}()
	marker := fmt.Sprintf("queue-qualify bulk load test data, safe to delete. Started %s, ends by %s.\n",
		time.Now().UTC().Format(time.RFC3339), time.Now().Add(opt.duration).UTC().Format(time.RFC3339))
	if err := os.WriteFile(filepath.Join(runDir, loadMarker), []byte(marker), 0o644); err != nil {
		return res, probeErr(err)
	}

	runCtx, stop := context.WithTimeout(ctx, opt.duration)
	defer stop()
	var (
		written, read atomic.Int64
		mu            sync.Mutex
		files         []string
		onDisk        int64
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
	limiter := newRateLimiter(opt.rateMiBps * (1 << 20))
	start := time.Now()
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
				avail, total, err := opt.free(opt.dir)
				if err != nil {
					halt("statfs-error", err)
					return
				}
				mu.Lock()
				res.FreeMinimum = min(res.FreeMinimum, avail)
				mu.Unlock()
				if avail < opt.floor(total)+uint64(opt.fileSize) {
					halt("free-space-floor", nil)
					return
				}
				path := filepath.Join(runDir, fmt.Sprintf("w%d-%06d.bin", w, n))
				began := time.Now()
				err = writeLoadFile(runCtx, path, opt.fileSize, buf, rng, limiter, &written)
				if runCtx.Err() != nil {
					os.Remove(path)
					return
				}
				if err != nil {
					halt("write-error", err)
					return
				}
				mu.Lock()
				fileLat = append(fileLat, ms(time.Since(began)))
				files = append(files, path)
				onDisk += opt.fileSize
				var evict []string
				for onDisk > opt.workingSet && len(files) > 0 {
					evict = append(evict, files[0])
					files = files[1:]
					onDisk -= opt.fileSize
				}
				mu.Unlock()
				for _, p := range evict {
					os.Remove(p)
				}
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
	res.Seconds = time.Since(start).Seconds()
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

func writeLoadFile(ctx context.Context, path string, size int64, buf []byte, rng *rand.Rand, limiter *rateLimiter, written *atomic.Int64) error {
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
		written.Add(n)
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

// removeLoadLeftovers deletes only what this tool created: directories with
// the load prefix and a marker inside, and snapshot test files.
func removeLoadLeftovers(parent string) ([]string, error) {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil, probeErr(err)
	}
	var removed []string
	for _, e := range entries {
		if e.Type().IsRegular() && strings.HasPrefix(e.Name(), snapshotPrefix) {
			if err := os.Remove(filepath.Join(parent, e.Name())); err != nil {
				return removed, probeErr(err)
			}
			removed = append(removed, e.Name())
			continue
		}
		if !e.IsDir() || !strings.HasPrefix(e.Name(), loadDirPrefix) {
			continue
		}
		dir := filepath.Join(parent, e.Name())
		if _, err := os.Stat(filepath.Join(dir, loadMarker)); err != nil {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			return removed, probeErr(err)
		}
		removed = append(removed, e.Name())
	}
	return removed, nil
}
