package myce

import (
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
