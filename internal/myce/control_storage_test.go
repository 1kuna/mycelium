package myce

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestControlStorageDefersBeforeBufferExhaustion(t *testing.T) {
	q, _ := fixture(t)
	c := q.control
	// A pinned WAL may need this much new database space on checkpoint. Leave
	// positive free space but insufficient room for checkpoint plus the next turn.
	c.storageProbe = func() (controlDiskBudget, error) { return controlDiskBudget{Available: 8192, WALBytes: 8192}, nil }
	c.append(ControlEvent{Kind: "checkin", Operation: "retained_incident"})
	if err := c.flush(q.db); err != nil {
		t.Fatal(err)
	}
	before := c.snapshot()
	if !before.StorageDeferred || before.StorageReason != "checkpoint_headroom" || before.DiagnosticTransactions != 0 {
		t.Fatal(before)
	}
	c.append(ControlEvent{Kind: "request", Operation: "ordinary"})
	after := c.snapshot()
	if after.StorageDropped != 1 || after.Dropped != 1 || after.Pending >= controlEventCapacity {
		t.Fatal("did not shed before buffer exhaustion", after)
	}
	c.storageProbe = func() (controlDiskBudget, error) { return controlDiskBudget{Available: 1 << 30}, nil }
	if err := c.flush(q.db); err != nil {
		t.Fatal(err)
	}
	after = c.snapshot()
	if after.StorageDeferred || after.DiagnosticTransactions == 0 || after.SerializedBytes == 0 {
		t.Fatal("did not recover", after)
	}
	found := false
	for _, event := range after.Recent {
		found = found || event.Operation == "retained_incident"
	}
	if !found {
		t.Fatal("incident lost", after.Recent)
	}
}

func TestControlStorageObservedPressure(t *testing.T) {
	q, clock, limit := adaptiveObservationFixture(t, 1, 1)
	release := observationPressure(limit, clock, time.Millisecond)
	if pressured, _ := limit.pressure.pressure(); !pressured {
		t.Fatal("pressure fixture missing")
	}
	q.control.append(ControlEvent{Kind: "checkin", Operation: "pressure_incident"})
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	before := q.control.snapshot()
	q.control.append(ControlEvent{Kind: "request", Operation: "ordinary"})
	after := q.control.snapshot()
	release()
	if !before.StorageDeferred || before.StorageReason != "control_pressure" || after.StorageDropped != 1 || after.Pending >= controlEventCapacity {
		t.Fatal("pressure did not shed early", after)
	}
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	if q.control.snapshot().StorageDeferred {
		t.Fatal("resolved pressure still defers")
	}
}

func TestControlStorageMeasuredCommitPressure(t *testing.T) {
	q, _ := fixture(t)
	c := q.control
	c.observeCommit(time.Millisecond, false)
	c.observeCommit(time.Millisecond, true)
	c.observeCommit(time.Second, true)
	c.append(ControlEvent{Kind: "checkin", Operation: "incident"})
	if err := c.flush(q.db); err != nil {
		t.Fatal(err)
	}
	if got := c.snapshot(); !got.StorageDeferred || got.StorageReason != "commit_cost" || got.DiagnosticTransactions != 0 {
		t.Fatal(got)
	}
	c.append(ControlEvent{Kind: "lock", Operation: "ordinary"})
	if c.snapshot().StorageDropped != 1 {
		t.Fatal("no proactive shedding")
	}
	c.observeCommit(time.Millisecond, false)
	if err := c.flush(q.db); err != nil {
		t.Fatal(err)
	}
	if c.snapshot().StorageDeferred {
		t.Fatal("healthy control commit did not restore persistence")
	}
}

func TestControlStorageCommitClasses(t *testing.T) {
	c := newControlRecorder(&fakeClock{now: time.Unix(1000, 0)})
	c.observeCommit(time.Nanosecond, false, "initialization")
	c.observeCommit(time.Millisecond, false, "checkin")
	if c.commitPressure {
		t.Fatal("cold operations contaminated each other")
	}
	c.observeCommit(time.Second, false, "checkin")
	c.observeCommit(time.Nanosecond, false, "initialization")
	if !c.commitPressure {
		t.Fatal("unrelated empty/cheap operation cleared pressure")
	}
	c.observeCommit(time.Millisecond, false, "checkin")
	if c.commitPressure {
		t.Fatal("same operation did not demonstrate recovery")
	}
}

// Exercise real table/index/sequence, overflow and retention writes with a fresh
// WAL. A JSON-sized allowance accepts the 8KiB fixture, despite the transaction
// writing several SQLite pages. Physical admission must refuse before any write.
func TestControlStoragePhysicalTransactions(t *testing.T) {
	for _, scenario := range []string{"cold", "overflow", "retention"} {
		t.Run(scenario, func(t *testing.T) {
			q, _ := fixture(t)
			c := q.control
			var path string
			if err := q.db.QueryRow(`SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
				t.Fatal(err)
			}
			if scenario == "retention" {
				_, err := q.db.Exec(`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?) INSERT INTO control_events(data) SELECT '{}' FROM n`, controlEventCapacity)
				if err != nil {
					t.Fatal(err)
				}
				c.retainedRows = controlEventCapacity
			}
			if _, err := q.db.Exec(`PRAGMA wal_autocheckpoint=0; PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
				t.Fatal(err)
			}
			operation := "physical"
			if scenario == "overflow" {
				operation += strings.Repeat("x", 20000)
			}
			c.append(ControlEvent{Kind: "checkin", Operation: operation})
			c.storageProbe = func() (controlDiskBudget, error) { return controlDiskBudget{Available: 8192}, nil }
			if err := c.flush(q.db); err != nil {
				t.Fatal(err)
			}
			if !c.snapshot().StorageDeferred || c.snapshot().DiagnosticTransactions != 0 {
				t.Fatal("JSON-size admission wrote diagnostic data")
			}
			st, err := os.Stat(path + "-wal")
			if err != nil {
				t.Fatal(err)
			}
			if st.Size() != 0 {
				t.Fatal("refusal wrote WAL frames", st.Size())
			}
			// Capture the allowance at actual preparation, without assuming the number
			// of dirty SQLite pages from serialized payload alone.
			c.storageProbe = func() (controlDiskBudget, error) { return controlDiskBudget{Available: 1 << 30}, nil }
			if err := c.flush(q.db); err != nil {
				t.Fatal(err)
			}
			st, err = os.Stat(path + "-wal")
			if err != nil {
				t.Fatal(err)
			}
			snap := c.snapshot()
			mutations := uint64(5)
			if scenario == "retention" {
				mutations++
			}
			growth := c.transactionGrowth(snap.SerializedBytes+1024, mutations)
			if st.Size() <= 8192 || uint64(st.Size()) > growth {
				t.Fatal("physical allowance did not cover SQLite evidence", st.Size(), growth)
			}
			t.Logf("%s: payload=%d WAL=%d allowance=%d", scenario, snap.SerializedBytes, st.Size(), growth)
		})
	}
}

func TestControlStorageCheckpointRebasesHeadroom(t *testing.T) {
	c := newControlRecorder(&fakeClock{now: time.Unix(1000, 0)})
	budget := controlDiskBudget{Available: 1 << 30, WALBytes: 1 << 28}
	c.storageProbe = func() (controlDiskBudget, error) { return budget, nil }
	if !c.storagePermit(4096) {
		t.Fatal("initial capacity")
	}
	budget = controlDiskBudget{Available: 1 << 20, WALBytes: 4096}
	if !c.storagePermit(4096) {
		t.Fatal("checkpoint left historical growth latched")
	}
}

func TestControlStorageCrossOperationRecovery(t *testing.T) {
	c := newControlRecorder(&fakeClock{now: time.Unix(1000, 0)})
	c.observeCommit(10*time.Millisecond, false, "administration")
	c.observeCommit(time.Millisecond, false, "checkin")
	c.observeCommit(time.Second, false, "administration")
	if !c.commitPressure {
		t.Fatal("missing initial pressure")
	}
	for i := 0; i < 10; i++ {
		c.observeCommit(time.Millisecond, false, "checkin")
	}
	if c.commitPressure {
		t.Fatal("healthy checkins could not recover an infrequent administrative stall")
	}
	// Persistent change must adapt rather than exclude every slower observation.
	for i := 0; i < 100; i++ {
		c.observeCommit(100*time.Millisecond, false, "checkin")
	}
	if c.commitPressure {
		t.Fatal("persistent latency change never adapted")
	}
}

// Log pruning writes after dropping q.mu and bypasses writeGate. Reproduce its
// deletion while it owns the sole SQL connection: final admission must occur
// after that write, not while diagnostics are still waiting for the connection.
func TestControlStorageInterveningPruningWrite(t *testing.T) {
	q, _ := fixture(t)
	c := q.control
	var path string
	if err := q.db.QueryRow(`SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`INSERT INTO logs(job,attempt,text) VALUES('prune',1,?)`, strings.Repeat("x", 20000)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	conn, err := q.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var probes atomic.Int64
	c.storageProbe = func() (controlDiskBudget, error) {
		probes.Add(1)
		st, err := os.Stat(path + "-wal")
		if err != nil {
			return controlDiskBudget{}, err
		}
		wal := uint64(st.Size())
		available := uint64(1 << 30)
		if wal > 0 {
			available = wal
		}
		return controlDiskBudget{Available: available, WALBytes: wal}, nil
	}
	c.append(ControlEvent{Kind: "checkin", Operation: "pruning_race"})
	before := q.db.Stats().WaitCount
	done := make(chan error, 1)
	go func() { done <- c.flush(q.db) }()
	await(t, func() bool { return q.db.Stats().WaitCount > before })
	// Save evidence before releasing the connection; let the goroutine finish
	// even when testing an implementation that incorrectly probed too early.
	beforePruneProbes := probes.Load()
	_, deleteErr := conn.ExecContext(context.Background(), `DELETE FROM logs WHERE rowid IN (SELECT rowid FROM logs WHERE job=? LIMIT 512)`, "prune")
	closeErr := conn.Close()
	flushErr := <-done
	if deleteErr != nil || closeErr != nil || flushErr != nil {
		t.Fatal(deleteErr, closeErr, flushErr)
	}
	if beforePruneProbes != 1 {
		t.Fatal("final capacity probe preceded pruning connection release", beforePruneProbes)
	}
	snap := c.snapshot()
	if !snap.StorageDeferred || snap.StorageReason != "checkpoint_headroom" || snap.DiagnosticTransactions != 0 {
		t.Fatal("intervening pruning write did not cause fresh admission refusal", snap)
	}
}
