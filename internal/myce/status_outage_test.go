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
