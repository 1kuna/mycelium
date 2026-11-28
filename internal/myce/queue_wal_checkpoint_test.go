package myce

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlite3 "modernc.org/sqlite/lib"
)

func TestQueueWALCheckpointDoesNotHoldWriterTurn(t *testing.T) {
	if mode := os.Getenv("MYCE_TEST_WAL_CHECKPOINT_MODE"); mode != "" {
		testQueueCheckpointWriterWait(t, mode == "automatic")
		return
	}
	// The VFS hook is local to the child process, as in the root crash tests.
	for _, mode := range []string{"automatic", "passive"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestQueueWALCheckpointDoesNotHoldWriterTurn$", "-test.v", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "MYCE_TEST_WAL_CHECKPOINT_MODE="+mode, "GORACE=atexit_sleep_ms=0")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("checkpoint child: %v\n%s", err, out)
			}
			t.Logf("%s", out)
		})
	}
}

func testQueueCheckpointWriterWait(t *testing.T, automatic bool) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "queue.db")
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	var armed atomic.Bool
	installQueueFileSyncHook(t, sqlite3.SQLITE_OPEN_MAIN_DB, path, func(sync func() int32) int32 {
		if armed.CompareAndSwap(true, false) {
			close(entered)
			<-resume
		}
		return sync()
	})
	clock := &fakeClock{now: time.Date(2026, 11, 28, 16, 44, 0, 0, time.UTC)}
	q, err := Open(path, root, clock, WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	defer unblock()
	const workers = 5
	var machines [workers]Machine
	for i := range machines {
		machines[i] = machine(fmt.Sprintf("worker-%d", i), "fake")
		check(t, q, machines[i], nil)
	}
	drainAdmissionWait(t, q)
	var autoCheckpoint, synchronous int
	if err := q.db.QueryRow("PRAGMA wal_autocheckpoint").Scan(&autoCheckpoint); err != nil || autoCheckpoint != 0 {
		t.Fatalf("writer auto-checkpoint setting=%d: %v", autoCheckpoint, err)
	}
	if err := q.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil || synchronous != 2 {
		t.Fatalf("writer durability setting=%d: %v", synchronous, err)
	}
	waitClock := &fakeClock{now: clock.Now()}
	q.holdClock = waitClock
	var maxWait time.Duration
	q.observeUpdate = func(wait, _ time.Duration) { maxWait = max(maxWait, wait) }
	clock.Advance(time.Second)
	foreground := make(chan error, 1)
	if automatic {
		// Cross SQLite's checkpoint threshold in a foreground queue commit.
		if _, err := q.db.Exec("PRAGMA wal_autocheckpoint=1"); err != nil {
			t.Fatal(err)
		}
		armed.Store(true)
		go func() { foreground <- q.update(func() error { q.s.Front++; return nil }) }()
	} else {
		// Fill the WAL beyond 1,000 frames to exercise the real checkpoint
		// lane. The test owns this auxiliary table and never queues a job.
		armed.Store(true)
		if _, err := q.db.Exec("CREATE TABLE pressure (data BLOB); INSERT INTO pressure VALUES (zeroblob(4194304))"); err != nil {
			t.Fatal(err)
		}
		q.walCheckpoint.pages.Add(1)
	}
	awaitQueueWrite(t, entered)
	const checkpointStall = 6342 * time.Millisecond
	if !automatic {
		waitClock.Advance(checkpointStall)
	}
	finished := make(chan error, workers)
	for _, m := range machines {
		go func() {
			_, err := q.Checkin(Checkin{Machine: m, period: time.Second})
			finished <- err
		}()
	}
	if automatic {
		await(t, func() bool { return q.mu.progress().Waiting >= workers })
		waitClock.Advance(checkpointStall)
		unblock()
		if err := awaitQueueWrite(t, foreground); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for range workers {
		select {
		case err := <-finished:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("five check-ins did not finish within two seconds")
		}
	}
	// In passive mode all workers committed while the checkpoint xSync was
	// still blocked. Verify the accepted contact record in the real database.
	var saved state
	data, err := readQueueState(q.db, normalizedQueueMarker)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	for _, m := range machines {
		if contact := saved.ContactEvidence[m.ID]; contact.Session != m.Session || !contact.Seen.Equal(clock.Now()) {
			t.Fatalf("check-in contact missing from SQLite for %s", m.ID)
		}
	}
	unblock()
	drainAdmissionWait(t, q)
	if automatic && maxWait < 6*time.Second || !automatic && maxWait >= 2*time.Second {
		t.Fatalf("automatic=%t max modeled check-in writer wait=%s", automatic, maxWait)
	}
	t.Logf("automatic=%t workers=%d max modeled check-in writer wait=%s", automatic, workers, maxWait)
}
