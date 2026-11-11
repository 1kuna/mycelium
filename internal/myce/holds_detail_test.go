package myce

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestHoldsDetailDemandReturnAndDockerEscalation(t *testing.T) {
	q, c, a, _, _ := holdFleet(t, true)
	p := spec("production")
	p.AllowedMachines = []string{a.ID}
	submit(t, q, p)
	running := check(t, q, a, nil).Job
	if running == nil {
		t.Fatal("no production assignment")
	}
	finish(t, q, running, true)
	idle := c.Now()
	// A worker-reported Docker blocker survives settled production demand.
	if err := q.update(func() error {
		q.touchMachine(a.ID).DockerCleanup = "cleanup unconfirmed: inspect"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b70: Docker cleanup unconfirmed; cleanup unconfirmed: inspect"}
	if !reflect.DeepEqual(hostHoldTexts(snap.Holds), want) {
		t.Fatalf("legacy strings changed: %v", hostHoldTexts(snap.Holds))
	}
	if len(snap.Holds) != 1 {
		t.Fatalf("retired idle hold remains: %+v", snap.Holds)
	}
	cleanup := snap.Holds[0]
	if cleanup.Kind != "docker_cleanup_waiting" || cleanup.Scope != "b70" || cleanup.operatorAction() || cleanup.Expires != nil || cleanup.Detail != want[0] {
		t.Fatalf("Docker blocker: %+v", cleanup)
	}
	// An attempt-specific custody record drives the existing escalation timer.
	r := HostRelease{Host: "b70", Job: "old-job", Machine: a.ID, Session: "old-session", Attempt: 1, Docker: true, CleanupSince: &idle}
	if err := q.update(func() error {
		if q.s.HostReleases == nil {
			q.s.HostReleases = map[string]HostRelease{}
		}
		q.s.HostReleases[releaseKey(r)] = r
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c.Advance(2 * time.Minute)
	// Durable escalation belongs to maintenance, not status observation.
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	snap, err = q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range snap.Holds {
		if d.Scope == "b70" && d.Kind == "docker_cleanup_operator_action" {
			found = true
			if !d.operatorAction() || d.Expires != nil || d.Detail != "b70: Docker cleanup unconfirmed; waiting for attempt old-job/1; operator action required (myce resolve-docker)" {
				t.Fatalf("escalation: %+v", d)
			}
		}
	}
	if !found {
		t.Fatalf("not escalated: %+v", snap.Holds)
	}
}

func TestHoldsDetailKindsAndLegacyCompatibility(t *testing.T) {
	q, c := fixture(t)
	m := machine("worker", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	for _, status := range []string{"creating", "create pending", "start pending", "stopping", "cleanup unconfirmed: inspect"} {
		q.s.Machines[m.ID].DockerCleanup = status
		s, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		want := "host: Docker cleanup unconfirmed; " + status
		kind := "docker_transition"
		if strings.HasPrefix(status, "cleanup unconfirmed") {
			kind = "docker_cleanup_waiting"
		}
		if len(s.Holds) != 1 || s.Holds[0].Kind != kind || s.Holds[0].Detail != want || s.Holds[0].operatorAction() || !reflect.DeepEqual(hostHoldTexts(s.Holds), []string{want}) {
			t.Fatalf("%s: %+v %v", status, s.Holds, hostHoldTexts(s.Holds))
		}
	}
	q.s.Machines[m.ID].DockerCleanup = ""
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

func TestHoldsDetailProductionKinds(t *testing.T) {
	q, c, a, _, _ := holdFleet(t, true)
	// Direct state fixtures isolate formatting from dispatch; snapshots above
	// exercise the public status path after explicit maintenance.
	h := q.s.Hosts["b70"]
	h.Active = true
	d := q.productionHold("b70")
	if d.Kind != "production_hold" || d.operatorAction() || d.Detail != "b70: production hold" {
		t.Fatalf("active: %+v", d)
	}
	r := HostRelease{Host: "b70", Job: "old", Machine: a.ID, Session: a.Session, Attempt: 1, Until: c.Now().Add(10 * time.Second)}
	q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
	d = q.productionHold("b70")
	if d.Kind != "production_stopping" || d.operatorAction() || d.Detail != "b70: production hold; stopping 1 attempts; grace remaining 10s" {
		t.Fatalf("grace: %+v", d)
	}
	c.Advance(10 * time.Second)
	d = q.productionHold("b70")
	if d.Kind != "production_release_unknown" || d.operatorAction() || d.Detail != "b70: production hold; release unknown: worker b70-0" {
		t.Fatalf("unknown: %+v", d)
	}
}

func TestHoldsDetailEmptyIsArray(t *testing.T) {
	q, _ := fixture(t)
	s, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"holds":[]`) || strings.Contains(string(data), `"host_holds"`) {
		t.Fatalf("empty holds fields: %s", data)
	}
}

// The production and Docker fence evidence status used to list as host holds.
func hostHoldTexts(holds []Hold) []string {
	var out []string
	for _, h := range holds {
		if strings.HasPrefix(h.Kind, "production") || strings.HasPrefix(h.Kind, "docker") || h.Kind == "unclassified" {
			out = append(out, h.Detail)
		}
	}
	return out
}
