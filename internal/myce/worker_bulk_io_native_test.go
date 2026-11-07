//go:build linux && !race

package myce

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Explicit native measurement only. Run under an owned idle-host Mycelium job,
// never during another consumer's reservation. It uses a disposable real queue,
// not the installed queue's database, and at most 256 MiB of payload. The normal
// gate skips it. Four 90s phases plus setup stay within the ten-minute brief.
func TestWorkerBulkIONativeMeasurement(t *testing.T) {
	if os.Getenv("MYCE_TEST_BULK_IO_NATIVE") != "1" {
		t.Skip("opt-in native same-device measurement; requires idle host authorization")
	}
	root, err := os.MkdirTemp(".", "bulk-io-measurement-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	var fs syscall.Statfs_t
	if err := syscall.Statfs(root, &fs); err != nil {
		t.Fatal(err)
	}
	if uint64(fs.Bavail)*uint64(fs.Bsize) < 60<<30 {
		t.Fatal("native measurement refused: less than 60 GiB free")
	}
	device, err := bulkIONativeDevice(root)
	if err != nil {
		t.Fatal(err)
	}
	var result []bulkIONativePhase
	// Counterbalance order to expose drift. Same writer count, buffer size,
	// sync frequency, scratch capacity and duration in every phase.
	for _, paced := range []bool{false, true, true, false} {
		r, err := bulkIONativeRun(root, device, paced)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, r)
		data, _ := json.Marshal(r)
		t.Log(string(data))
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("bulk-io-measurement.json", append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

type bulkIONativePhase struct {
	Paced                   bool      `json:"paced"`
	Seconds                 float64   `json:"seconds"`
	Bytes                   int64     `json:"bytes"`
	MiBPerSecond            float64   `json:"mib_per_second"`
	CheckinMS               []float64 `json:"checkin_ms"`
	CommitMS                []float64 `json:"probe_commit_ms"`
	Errors                  int       `json:"checkin_errors"`
	DeadlineMisses          int       `json:"checkin_over_10s"`
	PSISomePercent          float64   `json:"psi_io_some_percent"`
	PSIFullPercent          float64   `json:"psi_io_full_percent"`
	DeviceAwaitMS           float64   `json:"device_await_ms"`
	PeakSecondAwaitMS       float64   `json:"peak_second_device_await_ms"`
	DeviceWriteMiBPerSecond float64   `json:"device_write_mib_per_second"`
	FinalAllowance          float64   `json:"final_allowance_bytes"`
}

func bulkIONativeRun(root, device string, paced bool) (result bulkIONativePhase, err error) {
	phase, err := os.MkdirTemp(root, "phase-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(phase)
	if err = os.Mkdir(filepath.Join(phase, "checkpoints"), 0700); err != nil {
		return result, err
	}
	q, err := Open(filepath.Join(phase, "queue.db"), filepath.Join(phase, "checkpoints"), RealClock{})
	if err != nil {
		return result, err
	}
	defer q.Close()
	server := httptest.NewServer(API{Queue: q, Token: "isolated-measurement"}.Handler())
	defer server.Close()
	controller := newWorkerBulkIO(RealClock{})
	client := Client{URL: server.URL, Token: "isolated-measurement", bulkIO: controller}
	machine := machine("measurement-worker", "cpu")
	machine.CapabilityProfile = nil
	var controlMu sync.Mutex
	var rtts, commits []float64
	errorsCount, misses := 0, 0
	heartbeat := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		started := time.Now()
		var reply Reply
		callErr := client.Call(ctx, "POST", "/api/workers/checkin", Checkin{Machine: machine}, &reply)
		elapsed := time.Since(started)
		// A separate small queue mutation exposes actual SQLite commit timing
		// through the existing timing seam. It is not the check-in's commit span.
		timing := newQueueUpdateTiming()
		commitErr := q.updateTimed(func() error { q.s.LastWorkerContact = q.clock.Now(); return nil }, timing)
		controlMu.Lock()
		defer controlMu.Unlock()
		rtts = append(rtts, float64(elapsed)/float64(time.Millisecond))
		commits = append(commits, float64(timing.sqlCommit)/float64(time.Millisecond))
		if callErr != nil || commitErr != nil {
			errorsCount++
		}
		if elapsed >= 10*time.Second {
			misses++
		}
	}
	// Learn idle latency at the real worker heartbeat cadence before each phase.
	for range 8 {
		heartbeat()
		<-time.After(defaultWorkerInterval)
	}
	controlMu.Lock()
	rtts, commits = nil, nil
	errorsCount, misses = 0, 0
	controlMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	before, err := bulkIONativeRead(device)
	if err != nil {
		return result, err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(defaultWorkerInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				heartbeat()
			}
		}
	}()
	started := time.Now()
	var written atomic.Int64
	writerErrors := make(chan error, 4)
	for i := range 4 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wctx := ctx
			if paced {
				wctx = controller.context(ctx)
			}
			defer trackBulkIO(wctx)()
			f, e := os.OpenFile(filepath.Join(phase, fmt.Sprintf("payload-%d", i)), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if e != nil {
				writerErrors <- e
				return
			}
			defer f.Close()
			block := make([]byte, bulkIOQuantum)
			if _, e = rand.Read(block); e != nil {
				writerErrors <- e
				return
			}
			out := bulkWriter(wctx, f)
			for {
				if e = ctx.Err(); e != nil {
					return
				}
				if _, e = f.Seek(0, io.SeekStart); e != nil {
					writerErrors <- e
					return
				}
				for range (64 << 20) / bulkIOQuantum {
					if e = ctx.Err(); e != nil {
						return
					}
					n, e := out.Write(block)
					written.Add(int64(n))
					if e != nil {
						if ctx.Err() == nil {
							writerErrors <- e
						}
						return
					}
				}
				// Preserve the worker's per-file sync policy, once per 64 MiB.
				if e = f.Sync(); e != nil {
					writerErrors <- e
					return
				}
			}
		}(i)
	}
	previous := before
	peakAwait := 0.0
	tick := time.NewTicker(time.Second)
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-tick.C:
			next, e := bulkIONativeRead(device)
			if e != nil {
				cancel()
				err = e
				continue
			}
			peakAwait = max(peakAwait, next.await(previous))
			previous = next
		}
	}
	tick.Stop()
	wg.Wait() // Join every owned writer/control call before payload cleanup.
	close(writerErrors)
	for e := range writerErrors {
		err = errors.Join(err, e)
	}
	after, readErr := bulkIONativeRead(device)
	if err = errors.Join(err, readErr); err != nil {
		return result, err
	}
	seconds := time.Since(started).Seconds()
	controlMu.Lock()
	defer controlMu.Unlock()
	controller.mu.Lock()
	allowance := controller.budget
	controller.mu.Unlock()
	result = bulkIONativePhase{Paced: paced, Seconds: seconds, Bytes: written.Load(), MiBPerSecond: float64(written.Load()) / (1 << 20) / seconds, CheckinMS: rtts, CommitMS: commits, Errors: errorsCount, DeadlineMisses: misses, PSISomePercent: float64(after.some-before.some) / 1e6 / seconds * 100, PSIFullPercent: float64(after.full-before.full) / 1e6 / seconds * 100, DeviceAwaitMS: after.await(before), PeakSecondAwaitMS: peakAwait, DeviceWriteMiBPerSecond: float64(after.sectors-before.sectors) * 512 / (1 << 20) / seconds, FinalAllowance: allowance}
	slices.Sort(result.CheckinMS)
	slices.Sort(result.CommitMS)
	return result, nil
}

type bulkIONativeCounters struct{ some, full, ios, ticks, sectors uint64 }

func (n bulkIONativeCounters) await(old bulkIONativeCounters) float64 {
	if n.ios <= old.ios {
		return 0
	}
	return float64(n.ticks-old.ticks) / float64(n.ios-old.ios)
}
func bulkIONativeDevice(root string) (string, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(root, &st); err != nil {
		return "", err
	}
	// Linux device number encoding; resolve only this known filesystem device.
	dev := uint64(st.Dev)
	major := (dev>>8)&0xfff | (dev>>32)&0xfffff000
	minor := dev&0xff | (dev>>12)&0xffffff00
	path, err := filepath.EvalSymlinks(fmt.Sprintf("/sys/dev/block/%d:%d", major, minor))
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(path, "partition")); err == nil {
		path = filepath.Dir(path)
	}
	return filepath.Join(path, "stat"), nil
}
func bulkIONativeRead(device string) (bulkIONativeCounters, error) {
	var n bulkIONativeCounters
	data, err := os.ReadFile(device)
	if err != nil {
		return n, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 11 {
		return n, errors.New("short device statistics")
	}
	values := make([]uint64, len(fields))
	for i, field := range fields {
		values[i], err = strconv.ParseUint(field, 10, 64)
		if err != nil {
			return n, err
		}
	}
	n.ios, n.ticks, n.sectors = values[0]+values[4], values[3]+values[7], values[6]
	data, err = os.ReadFile("/proc/pressure/io")
	if err != nil {
		return n, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		for _, field := range fields[1:] {
			if value, ok := strings.CutPrefix(field, "total="); ok {
				count, e := strconv.ParseUint(value, 10, 64)
				if e != nil {
					return n, e
				}
				if fields[0] == "some" {
					n.some = count
				}
				if fields[0] == "full" {
					n.full = count
				}
			}
		}
	}
	return n, nil
}
