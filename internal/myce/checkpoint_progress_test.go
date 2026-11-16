package myce

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// Count actual directory enumerations through fs.WalkDir's ReadDirFS seam.
// All fixtures live on local temporary storage; no NAS access is needed.
type checkpointCountingFS struct {
	fs.FS
	reads atomic.Int64
}

func (f *checkpointCountingFS) ReadDir(path string) ([]fs.DirEntry, error) {
	f.reads.Add(1)
	return fs.ReadDir(f.FS, path)
}

func checkpointCountingScan(root string) (*checkpointCountingFS, func(string) (int64, error)) {
	f := &checkpointCountingFS{FS: os.DirFS(root)}
	return f, func(path string) (int64, error) {
		return scanCheckpointProgressWalk(path, false, func(_ string, visit fs.WalkDirFunc) error {
			return fs.WalkDir(f, ".", func(name string, entry fs.DirEntry, err error) error {
				return visit(filepath.Join(path, name), entry, err)
			})
		})
	}
}

// Fail before a progress walk can enumerate output or stat its directory.
// The shared scanner serves worker resume/finalization and queue event checks.
type checkpointOutputGuardFS struct {
	fs.FS
	forbidden atomic.Int64
}

func (f *checkpointOutputGuardFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == "output" || strings.HasPrefix(name, "output/") {
		f.forbidden.Add(1)
		return nil, errors.New("progress enumerated output")
	}
	entries, err := fs.ReadDir(f.FS, name)
	if name == "." {
		for i, entry := range entries {
			if entry.Name() == "output" {
				entries[i] = checkpointOutputGuardEntry{DirEntry: entry, forbidden: &f.forbidden}
			}
		}
	}
	return entries, err
}

type checkpointOutputGuardEntry struct {
	fs.DirEntry
	forbidden *atomic.Int64
}

func (e checkpointOutputGuardEntry) Info() (fs.FileInfo, error) {
	e.forbidden.Add(1)
	return nil, errors.New("progress inspected output metadata")
}

func TestCheckpointProgressNeverEnumeratesOutput(t *testing.T) {
	root := t.TempDir()
	writeSave(t, root, "7")
	// This is output, not a checkpoint. A generation here must never win or
	// make progress invalid, even at a resume/finalization boundary.
	output := filepath.Join(root, "output", "stage", "nested", "boundaries")
	writeSave(t, output, "999")
	guard := &checkpointOutputGuardFS{FS: os.DirFS(root)}
	value, err := scanCheckpointProgressWalk(root, false, func(_ string, visit fs.WalkDirFunc) error {
		return fs.WalkDir(guard, ".", func(name string, entry fs.DirEntry, err error) error {
			return visit(filepath.Join(root, name), entry, err)
		})
	})
	if err != nil || value != 7 || guard.forbidden.Load() != 0 {
		t.Fatalf("progress touched output: generation=%d forbidden=%d err=%v", value, guard.forbidden.Load(), err)
	}
	// Exercise the production walker as well as the enumeration guard.
	for _, jobRoot := range []bool{false, true} {
		if value, err := scanCheckpointProgress(root, jobRoot); err != nil || value != 7 {
			t.Fatalf("production discovery used output: jobRoot=%t generation=%d err=%v", jobRoot, value, err)
		}
	}
}

func TestCheckpointProgressEventDriven(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := recoveryWorker(t)
		clock := &fakeClock{now: time.Unix(100, 0)}
		w.Clock, w.Interval, w.control = clock, 5*time.Second, &processControl{}
		root, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer root.close()
		j := recoveryJob(t, []string{"true"})
		j.State = "running"
		s, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		path := attemptCheckpoint(j)
		// Three directories, with a save below the top directory. In-place
		// nested writes prove why root-mtime-only suppression is incorrect.
		save := filepath.Join(path, "nested", "state")
		write := func(generation string) {
			t.Helper()
			if err := os.MkdirAll(save, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(save, "save.json"), []byte(`{"myce_checkpoint":{"generation":`+generation+`}}`), 0600); err != nil {
				t.Fatal(err)
			}
		}
		write("1")
		beforeFS, beforeScan := checkpointCountingScan(path)
		for tick := 0; tick < 12; tick++ {
			if _, err := beforeScan(path); err != nil {
				t.Fatal(err)
			}
		}
		afterFS, scan := checkpointCountingScan(path)
		s.nasProgress.scan = scan
		var markers atomic.Int64
		s.nasProgress.readSignal = func(path string, value any) error {
			markers.Add(1)
			return readCheckpointJSON(path, value)
		}
		signal := checkpointSignalFile(s.dir, j)
		if err := os.MkdirAll(filepath.Dir(signal), 0700); err != nil {
			t.Fatal(err)
		}
		// Inject an already-running process. No payload, NAS mount, or native
		// resource probes are needed to exercise the actual 100ms actor loop.
		p := &process{job: j, done: make(chan workerFinish, 1)}
		ctx, cancel := context.WithCancel(context.Background())
		a := &attemptSupervisor{store: s, job: j, worker: w, recovering: p, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go a.run(ctx)
		defer func() {
			p.done <- workerFinish{result: Result{Execution: Execution{ID: j.ID, Attempt: j.Attempt}}}
			cancel()
			<-a.done
		}()
		synctest.Wait()
		if afterFS.reads.Load() != 3 {
			t.Fatal("recovery did not read fresh progress", afterFS.reads.Load())
		}
		afterFS.reads.Store(0) // Recovery is an event, outside steady state.
		for tick := 0; tick < 600; tick++ {
			if tick == 251 {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				write("2")
				if err := os.WriteFile(signal, []byte(`{"name":"save.json","generation":2}`), 0600); err != nil {
					t.Fatal(err)
				}
				updated, err := os.Stat(path)
				if err != nil || !info.ModTime().Equal(updated.ModTime()) {
					t.Fatal("fixture changed root mtime", err)
				}
			}
			synctest.Wait()
			report, _, _, err := a.snapshot()
			if err != nil || !report.Started {
				t.Fatal("running progress unavailable", report, err)
			}
			if tick > 0 && tick < 251 && report.Progress != -1 {
				t.Fatal("arbitrary live progress must be unknown", report)
			}
			if tick >= 252 && report.Progress != 2 {
				t.Fatal("local save absent from worker report", report)
			}
			// Check every prefix, not just an average that could hide bursts.
			if got := afterFS.reads.Load(); got != 0 {
				t.Fatalf("tick %d: live status read %d NAS directories", tick, got)
			}
			if tick < 599 {
				clock.Advance(100 * time.Millisecond)
				time.Sleep(100 * time.Millisecond)
			}
		}
		if beforeFS.reads.Load() != 36 || afterFS.reads.Load() != 0 || markers.Load() != 1 {
			t.Fatal("unexpected directory-read measurement", beforeFS.reads.Load(), afterFS.reads.Load())
		}
		t.Logf("local counting filesystem, 60s, 100ms actor, 5s reports, three directories: round1=%d NAS reads/min (12 walks), after=%d NAS reads/min (zero walks), published save=%d local marker read", beforeFS.reads.Load(), afterFS.reads.Load(), markers.Load())
		// An arbitrary final save (no notification) is read at finalization.
		write("3")
		p.done <- workerFinish{result: Result{Execution: Execution{ID: j.ID, Attempt: j.Attempt}}}
		clock.Advance(100 * time.Millisecond)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		if r, _, _, _ := a.snapshot(); r.Progress != 3 || afterFS.reads.Load() != 3 {
			t.Fatal("finalization missed arbitrary save", r, afterFS.reads.Load())
		}
	})
}

func TestCheckpointProgressPendingRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &fakeClock{now: time.Unix(100, 0)}
		var n checkpointProgressReader
		n.scan = func(string) (int64, error) { return 1, nil }
		if value, err := n.read(clock, 5*time.Second, "attempt"); value != 1 || err != nil {
			t.Fatal(value, err)
		}
		clock.Advance(5 * time.Second)
		release := make(chan struct{})
		var scans atomic.Int64
		n.scan = func(string) (int64, error) { scans.Add(1); <-release; return 2, nil }
		value, err := n.read(clock, 5*time.Second, "attempt")
		if value != 1 || !errors.Is(err, errCheckpointIOPending) {
			t.Fatal("pending refresh lost prior progress", value, err)
		}
		started := time.Now()
		if value, err := n.read(clock, 5*time.Second, "attempt"); value != 1 || !errors.Is(err, errCheckpointIOPending) || time.Since(started) != 0 {
			t.Fatal("pending refresh blocked a duplicate report", value, err)
		}
		close(release)
		synctest.Wait()
		if value, err := n.read(clock, 5*time.Second, "attempt"); value != 2 || err != nil {
			t.Fatal("late generation was not consumed promptly", value, err)
		}
		for tick := 0; tick < 49; tick++ {
			clock.Advance(100 * time.Millisecond)
			if _, err := n.read(clock, 5*time.Second, "attempt"); err != nil {
				t.Fatal(err)
			}
		}
		if scans.Load() != 1 {
			t.Fatal("completed refresh restarted before report", scans.Load())
		}
	})
}

func TestCheckpointProgressLocalMarkerPrompt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := recoveryWorker(t)
		clock := &fakeClock{now: time.Unix(100, 0)}
		w.Clock, w.Interval, w.control = clock, 5*time.Second, &processControl{}
		root, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer root.close()
		j := recoveryJob(t, []string{"true"})
		j.Spec.CheckpointPolicy = "local_first"
		if err := os.MkdirAll(attemptCheckpoint(j), 0700); err != nil {
			t.Fatal(err)
		}
		localGeneration(t, j, 1)
		s, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		s.nasProgress.scan = func(string) (int64, error) {
			t.Error("local-first must use its local marker, never a NAS walk")
			return 0, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		a := &attemptSupervisor{store: s, job: j, worker: w, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go a.run(ctx)
		defer func() { cancel(); <-a.done }()
		synctest.Wait()
		if r, _, _, _ := a.snapshot(); r.LocalCheckpointGeneration != 1 {
			t.Fatal("missing initial local generation", r)
		}
		localGeneration(t, j, 2)
		clock.Advance(100 * time.Millisecond)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		if r, _, _, _ := a.snapshot(); r.LocalCheckpointGeneration != 2 {
			t.Fatal("new marker waited for the NAS report interval", r)
		}
	})
}

func TestQueueCheckpointProgressFromWorker(t *testing.T) {
	q, clock, m, _, j := yieldFixture(t, false)
	var scans atomic.Int64
	q.nasProgress.scan = func(string) (int64, error) { scans.Add(1); return -1, errors.New("status must not read NAS") }
	for tick := 0; tick < 600; tick++ {
		// Deliberately do not write the reported generation to the share: live
		// status comes from this worker; only finalization verifies durable bytes.
		_, err := q.Checkin(Checkin{Machine: m, period: 5 * time.Second, Attempts: []AttemptReport{{Execution: Execution{ID: j.ID, Attempt: j.Attempt, Progress: 2}, Session: j.Session, Started: true}}})
		if err != nil {
			t.Fatal(err)
		}
		if scans.Load() != 0 {
			t.Fatal("queue read NAS for status", scans.Load())
		}
		clock.Advance(100 * time.Millisecond)
	}
	got := job(t, q, j.ID)
	if got.Progress != 2 || got.YieldGeneration != 2 || got.CheckpointTarget == nil || got.CheckpointTarget.Generation != 2 {
		t.Fatal("worker progress not retained", got)
	}
}

func TestCheckpointProgressEventPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &fakeClock{now: time.Unix(100, 0)}
		var n checkpointProgressReader
		release := make(chan struct{})
		var reads atomic.Int64
		scan := func() (int64, error) { reads.Add(1); <-release; return 7, nil }
		if _, err := n.event(clock, "recovery", "attempt", scan); !errors.Is(err, errCheckpointIOPending) {
			t.Fatal(err)
		}
		for tick := 0; tick < 600; tick++ {
			clock.Advance(100 * time.Millisecond)
			if _, err := n.event(clock, "recovery", "attempt", scan); !errors.Is(err, errCheckpointIOPending) {
				t.Fatal(err)
			}
		}
		if reads.Load() != 1 {
			t.Fatal("pending event duplicated", reads.Load())
		}
		close(release)
		synctest.Wait()
		if v, err := n.event(clock, "recovery", "attempt", scan); v != 7 || err != nil {
			t.Fatal(v, err)
		}
		clock.Advance(time.Hour)
		if v, err := n.event(clock, "recovery", "attempt", scan); v != 7 || err != nil || reads.Load() != 1 {
			t.Fatal("idle event reread", v, err, reads.Load())
		}
	})
}
