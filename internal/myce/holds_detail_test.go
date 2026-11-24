package myce

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestHoldsDetailKindsAndLegacyCompatibility(t *testing.T) {
	q, c := fixture(t)
	m := machine("worker", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	setMaintenance(t, q, "host", "disable")
	deadline := c.Now()
	q.s.Jobs["startup"] = &Job{ID: "startup", State: "failed", Spec: Spec{StartBy: &deadline}}
	r := HostRelease{Host: "host", Job: "startup", Machine: m.ID, Session: m.Session, Attempt: 1}
	q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
	s, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, d := range s.Holds {
		kinds[d.Kind] = true
		// Only a deliberate disable waits on the operator; a live startup
		// release clears itself.
		if d.operatorAction() != (d.Kind == "host_disabled") {
			t.Fatalf("operator fence: %+v", d)
		}
		if strings.Contains(d.Detail, "private reason") {
			t.Fatal("private reason exposed")
		}
	}
	if !kinds["host_disabled"] || !kinds["startup_release"] || len(hostHoldTexts(s.Holds)) != 0 {
		t.Fatalf("fences: %+v", s)
	}
	data, _ := json.Marshal(s)
	if strings.Contains(string(data), `"host_holds"`) {
		t.Fatal("empty legacy list must be omitted")
	}
	if !strings.Contains(string(data), `"holds":[`) {
		t.Fatal("missing details array")
	}
}

// The production fence evidence status used to list as host holds.
func hostHoldTexts(holds []Hold) []string {
	var out []string
	for _, h := range holds {
		if strings.HasPrefix(h.Kind, "production") || h.Kind == "unclassified" {
			out = append(out, h.Detail)
		}
	}
	return out
}

func TestJobHostMaintenanceHold(t *testing.T) {
	q, clock := fixture(t)
	first := machine("worker-a", "fake", "host-a")
	second := machine("worker-b", "fake", "host-b")
	check(t, q, first, nil)
	check(t, q, second, nil)
	expected := clock.Now().Add(30 * time.Minute)
	const reason = "replace a fan"
	if _, err := q.ChangeHostMaintenance(first.HostID, HostMaintenanceRequest{
		Action: "drain", Reason: reason, ExpectedReturn: &expected,
	}); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, spec("experiment"))
	read := func() *Job {
		t.Helper()
		got, err := q.jobSnapshot(j.ID)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	// Losing observations on an undrained compatible host is an independent
	// route failure. A drain on another host must not claim it.
	clock.Advance(LossAfter + time.Minute)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := read(); got.Hold == nil || got.Hold.Kind != "unknown_observation" {
		t.Fatalf("stale route lost its own diagnosis: %+v", got.Hold)
	}
	if err := setMaintenance(t, q, second.HostID, "drain"); err != nil {
		t.Fatal(err)
	}
	got := read()
	h := got.Hold
	if got.State != "queued" || h == nil || h.Kind != hostMaintenanceJobHold ||
		h.Owner != ownerOperator || !strings.Contains(h.Next, "myce host resume host-a") ||
		!strings.Contains(h.Reason, first.HostID) || !strings.Contains(h.Reason, second.HostID) ||
		!strings.Contains(h.Reason, reason) || strings.Count(h.Reason, "expected return") != 1 ||
		!strings.Contains(h.Reason, expected.Format(time.RFC3339)) || h.Expires != nil {
		t.Fatalf("job maintenance hold: %+v", h)
	}
	fleet, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(fleet.Holds) != 2 || fleet.Holds[0].Scope != first.HostID ||
		fleet.Holds[0].Owner != ownerOperator || !strings.Contains(fleet.Holds[0].Line(), expected.Format(time.RFC3339)) ||
		strings.Contains(fleet.Holds[1].Line(), "expected return") {
		t.Fatalf("fleet maintenance holds: %+v", fleet.Holds)
	}

	// A recorded forecast never resumes admission, even after it passes.
	clock.Advance(time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := read(); got.Hold == nil || got.Hold.Kind != hostMaintenanceJobHold {
		t.Fatalf("forecast cleared the drain: %+v", got.Hold)
	}
	resumed, err := q.ChangeHostMaintenance(first.HostID, HostMaintenanceRequest{Action: "resume"})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Policy.Reason != "" || !resumed.Policy.ExpectedReturn.IsZero() {
		t.Fatalf("resume retained old maintenance metadata: %+v", resumed.Policy)
	}
	if got := read(); got.Hold != nil && got.Hold.Kind == hostMaintenanceJobHold {
		t.Fatalf("resume left the maintenance hold: %+v", got.Hold)
	}
	if offered := check(t, q, first, nil).Job; offered == nil || offered.ID != j.ID {
		t.Fatalf("resumed worker did not take the same job: %+v", offered)
	}
}
