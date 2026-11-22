package myce

import (
	"context"
	"testing"
	"time"
)

func TestIdleStorageEvictsByMeasuredBytes(t *testing.T) {
	q, clock := fixture(t)
	host := cpuMachine("worker", "host", "cpu", 8, 32)
	check(t, q, host, nil)
	h := &idleHistory{db: q.idleHistory.db}
	at := clock.Now()
	class := idleMachineClass([]*Machine{&host})
	h.observe(1, at, map[string]idleObservation{host.HostID: {HostIdleEvidence: HostIdleEvidence{Class: class, State: idleWorking}}}, true)
	h.observe(2, at.Add(time.Second), map[string]idleObservation{host.HostID: {HostIdleEvidence: HostIdleEvidence{Class: class, State: idleNoDemand}}}, true)
	first := h.open[host.HostID]
	h.observe(3, at.Add(2*time.Second), h.previous, true)
	second := h.open[host.HostID]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Holding the control database writer must leave interval retention and
	// reads available on the separate evidence store.
	control, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Rollback()
	if err := h.persist(ctx, []idleStored{first, second}, first.size()+second.size()); err != nil {
		t.Fatal(err)
	}
	if err := h.persist(ctx, nil, second.size()); err != nil {
		t.Fatal(err)
	}
	report, err := q.idleReport(ctx, at, at.Add(2*time.Second), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Intervals) != 2 || report.Intervals[0].State != idleUnknown || report.Intervals[1].State != idleNoDemand ||
		report.MachineHours != 0 || h.retained != second.size() {
		t.Fatalf("byte retention did not preserve the newest interval and mark the gap: %+v (%d bytes retained)", report.Intervals, h.retained)
	}
}
