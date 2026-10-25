package myce

import (
	"encoding/json"
	"strings"
	"testing"
)

// Fake worker inventory must withhold work temporarily, never reject submission.
func TestDiskHeadroomHoldsAndRecovers(t *testing.T) {
	q, c := fixture(t)
	m := machine("disk-worker", "fake", "disk-host")
	setFree := func(free uint64) {
		b, _ := json.Marshal(map[string]any{"disk_volumes": []any{map[string]any{"roots": []string{"data"}, "total_bytes": uint64(100 << 30), "free_bytes": free, "sampled_at": c.Now()}}})
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
	}
	setFree(2 << 30)
	check(t, q, m, nil)
	s := spec("experiment")
	// A hint is optional; this one makes the job need 2 GiB beyond the floor.
	if err := json.Unmarshal([]byte(`{"disk":{"output_bytes":2147483648}}`), &s); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, s)
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatalf("low-space host admitted job: %s", got.ID)
	}
	var snap Snapshot
	if err := json.Unmarshal([]byte(statusViewResponse(t, q, "/api/status")), &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Machines) != 1 || len(snap.Machines[0].DiskVolumes) != 1 || snap.Machines[0].DiskVolumes[0].FreeBytes != 2<<30 {
		t.Fatal("API omitted worker disk inventory", snap.Machines)
	}
	held := cpuIsolationJob(t, snap, j.ID)
	if held.State != "queued" || held.DiagnosticCode != "disk_headroom" {
		t.Fatalf("not a temporary typed hold: %+v", held)
	}
	for _, part := range []string{"disk-host", "free=2147483648", "floor=1073741824", "need=2147483648"} {
		if !strings.Contains(held.Reason, part) {
			t.Fatalf("missing %q: %s", part, held.Reason)
		}
	}
	setFree(10 << 30)
	admitted := check(t, q, m, nil).Job
	if admitted == nil || admitted.ID != j.ID {
		t.Fatal("recovered space did not admit queued work", admitted)
	}
	// Launch uses the same predicate if disk pressure returns after dispatch.
	setFree(2 << 30)
	check(t, q, m, admitted)
	if err := q.Started(result(admitted, true)); err != ErrStale {
		t.Fatalf("launch ignored disk drift: %v", err)
	}
	refused := result(admitted, false)
	refused.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "launch_refused"}
	if err := q.Complete(refused); err != nil {
		t.Fatal(err)
	}
	held = job(t, q, j.ID)
	if held.State != "queued" || held.Losses != 0 || held.InfrastructureRetries != 0 {
		t.Fatalf("prelaunch disk hold charged a failure: %+v", held)
	}
	setFree(10 << 30)
	admitted = check(t, q, m, nil).Job
	if admitted == nil {
		t.Fatal("disk drift did not recover")
	}
	if err := q.Started(result(admitted, true)); err != nil {
		t.Fatalf("launch did not recover: %v", err)
	}
}
