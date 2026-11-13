package myce

import (
	"errors"
	"testing"
	"time"
)

// Calibration is shared by ordinary and permanent-capacity readers, but new
// observations, rollback and future evidence must change the very next result.
func TestMemoryTransferDerivationFences(t *testing.T) {
	q, clock := fixture(t)
	q.memoryPlanning = newMemoryPlanningCache()
	var from, to memoryTransferTarget
	for i, work := range []string{"one", "two"} {
		for _, host := range []string{"source", "target"} {
			peak := 1.0
			if host == "target" {
				peak = 2
			}
			key := ObservationKey{Work: work, Host: host, Component: "cpu", Fingerprint: "identity", Target: ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}}
			q.putWorkObservation(host+work, WorkObservation{Key: key, MemoryConfiguration: "config", MemoryLayout: "separate", MemoryRecordedAt: clock.Now(), MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}})
			if i == 0 {
				target := transferTarget(key, "separate")
				if host == "source" {
					from = target
				} else {
					to = target
				}
			}
		}
	}
	check := func(known bool) float64 {
		t.Helper()
		got, ok := q.transferMemoryFactor(from, to, false)
		want, wantOK := q.transferMemoryFactorUncached(from, to, false)
		if ok != known || ok != wantOK || got != want {
			t.Fatalf("cached factor %v/%v; uncached %v/%v; known want %v", got, ok, want, wantOK, known)
		}
		view := Queue{clock: q.clock, s: q.s, memoryOrigin: q, memoryPlanning: newMemoryPlanningCache()}
		other, otherOK := view.transferMemoryFactor(from, to, false)
		if other != want || otherOK != wantOK {
			t.Fatal("capacity view changed transfer calibration")
		}
		return got
	}
	original := check(true)
	rollback := errors.New("rollback")
	err := q.update(func() error {
		point := q.s.WorkObservations["targetone"]
		peak := 12.0
		point.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
		q.putWorkObservation("targetone", point)
		if check(true) == original {
			t.Fatal("new observation reused old calibration")
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if check(true) != original {
		t.Fatal("rolled-back calibration survived")
	}
	for _, work := range []string{"one", "two"} {
		point := q.s.WorkObservations["target"+work]
		point.MemoryRecordedAt = clock.Now().Add(time.Second)
		q.putWorkObservation("target"+work, point)
	}
	check(false)
	clock.now = clock.now.Add(time.Second)
	check(true)
	clock.now = clock.now.Add(-time.Second)
	check(false)
}
