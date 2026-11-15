package myce

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
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

func TestCheckpointProgressReportCadence(t *testing.T) {
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
		for tick := 0; tick < 600; tick++ {
			if _, err := beforeScan(path); err != nil {
				t.Fatal(err)
			}
		}
		afterFS, scan := checkpointCountingScan(path)
		s.nasProgress.scan = scan
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
		for tick := 0; tick < 600; tick++ {
			if tick == 251 {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				write("2")
				updated, err := os.Stat(path)
				if err != nil || !info.ModTime().Equal(updated.ModTime()) {
					t.Fatal("fixture changed root mtime", err)
				}
			}
			synctest.Wait()
			report, _, _, err := a.snapshot()
			if err != nil || !report.Started || report.Progress < 1 {
				t.Fatal("running progress unavailable", report, err)
			}
			if tick >= 301 && report.Progress != 2 {
				t.Fatal("new generation absent after next report interval", report)
			}
			// Check every prefix, not just an average that could hide bursts.
			if got, limit := afterFS.reads.Load(), int64(3*(1+tick/50)); got > limit {
				t.Fatalf("tick %d: directory reads %d exceed %d", tick, got, limit)
			}
			if tick < 599 {
				clock.Advance(100 * time.Millisecond)
				time.Sleep(100 * time.Millisecond)
			}
		}
		if beforeFS.reads.Load() != 1800 || afterFS.reads.Load() != 36 {
			t.Fatal("unexpected directory-read measurement", beforeFS.reads.Load(), afterFS.reads.Load())
		}
		t.Logf("local counting filesystem, 60s, 100ms actor, 5s reports, three directories: before=%d reads/min (600 walks), after=%d reads/min (12 walks)", beforeFS.reads.Load(), afterFS.reads.Load())
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

func TestQueueCheckpointProgressReportCadence(t *testing.T) {
	q, clock, m, _, j := yieldFixture(t, false)
	clock.Advance(5 * time.Second) // Expire the fixture's first observation.
	var scans atomic.Int64
	q.nasProgress.scan = func(path string) (int64, error) {
		// Scans must stay outside the queue mutex.
		q.mu.Lock()
		q.mu.Unlock()
		scans.Add(1)
		return checkpointProgress(path)
	}
	for tick := 0; tick < 50; tick++ {
		if tick == 1 {
			saveYieldGeneration(t, j, 2)
		}
		_, err := q.Checkin(Checkin{Machine: m, period: 5 * time.Second, Attempts: []AttemptReport{{Execution: Execution{ID: j.ID, Attempt: j.Attempt, Progress: 2}, Session: j.Session, Started: true}}})
		if err != nil {
			t.Fatal(err)
		}
		clock.Advance(100 * time.Millisecond)
	}
	if scans.Load() != 1 {
		t.Fatal("queue re-walked before consumer interval", scans.Load())
	}
	_, err := q.Checkin(Checkin{Machine: m, period: 5 * time.Second, Attempts: []AttemptReport{{Execution: Execution{ID: j.ID, Attempt: j.Attempt, Progress: 2}, Session: j.Session, Started: true}}})
	if err != nil || scans.Load() != 2 {
		t.Fatal("queue did not refresh next report", scans.Load(), err)
	}
	if got := job(t, q, j.ID).CheckpointTarget; got == nil || got.Generation != 2 {
		t.Fatal("queue failed to retain new generation", got)
	}
}
