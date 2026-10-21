package myce

import (
	"strings"
	"testing"
	"time"
)

func TestStatusConcurrentOutageWarnings(t *testing.T) {
	q, clock := fixture(t)
	first := submit(t, q, spec("experiment"))
	running := check(t, q, machine("worker", "fake"), nil).Job
	if running == nil {
		t.Fatal("missing assignment")
	}
	q.checkpointProbe = func() bool { return false }
	clock.Advance(10 * time.Minute)
	snapshot, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(snapshot.Warning, "network") || !strings.Contains(snapshot.Warning, "NAS") {
		t.Fatalf("concurrent outage warning overwritten: %q", snapshot.Warning)
	}
	requireState(t, q, first.ID, "running")
	queued := submit(t, q, spec("experiment"))
	if check(t, q, machine("new-worker", "fake"), nil).Job != nil {
		t.Fatal("warning aggregation weakened NAS admission")
	}
	requireState(t, q, queued.ID, "queued")
}

func TestLateCheckinClearsNetworkWarning(t *testing.T) {
	q, clock := fixture(t)
	workers := []Machine{machine("a", "fake"), machine("b", "fake"), machine("c", "fake")}
	for _, m := range workers {
		check(t, q, m, nil)
	}
	clock.Advance(10 * time.Minute)
	s, err := q.Snapshot()
	if err != nil || !strings.Contains(s.Warning, "network") {
		t.Fatalf("whole-network warning missing: %q, %v", s.Warning, err)
	}
	check(t, q, workers[0], nil)
	s, err = q.Snapshot()
	if err != nil || strings.Contains(s.Warning, "network") || q.s.NetworkRecoveryAt.IsZero() {
		t.Fatalf("late contact did not clear warning and record recovery: %q, %v", s.Warning, err)
	}
	for _, m := range s.Machines {
		if m.Gone {
			t.Fatalf("peer %s marked gone during network recovery", m.ID)
		}
	}
}
