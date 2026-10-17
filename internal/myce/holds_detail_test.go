package myce

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestHoldsDetailCooldownAndDockerEscalation(t *testing.T) {
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
	// A worker-reported cleanup obligation can coexist with the host's cooldown.
	q.s.Machines[a.ID].DockerCleanup = "cleanup unconfirmed: inspect"
	snap, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b70: Docker cleanup unconfirmed; cleanup unconfirmed: inspect", "b70: production idle; experiments resume in 180s"}
	if !reflect.DeepEqual(snap.HostHolds, want) {
		t.Fatalf("legacy strings changed: %v", snap.HostHolds)
	}
	var cooldown, cleanup *HoldDetail
	for i := range snap.HoldsDetail {
		d := &snap.HoldsDetail[i]
		if d.Kind == "production_cooldown" {
			cooldown = d
		}
		if d.Kind == "docker_cleanup_waiting" {
			cleanup = d
		}
	}
	if cooldown == nil || cleanup == nil || cooldown.OperatorActionRequired || cleanup.OperatorActionRequired {
		t.Fatalf("details: %+v", snap.HoldsDetail)
	}
	if cooldown.Host != "b70" || cleanup.Host != "b70" || cooldown.ClearsAt == nil || !cooldown.ClearsAt.Equal(idle.Add(180*time.Second)) || cleanup.ClearsAt != nil {
		t.Fatalf("holds: %+v %+v", cooldown, cleanup)
	}
	if cooldown.Text != want[1] || cleanup.Text != want[0] {
		t.Fatalf("text: %+v %+v", cooldown, cleanup)
	}
	// An attempt-specific custody record drives the existing escalation timer.
	r := HostRelease{Host: "b70", Job: "old-job", Machine: a.ID, Session: "old-session", Attempt: 1, Docker: true, CleanupSince: &idle}
	q.s.HostReleases[releaseKey(r)] = r
	c.Advance(2 * time.Minute)
	snap, err = q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range snap.HoldsDetail {
		if d.Host == "b70" && d.Kind == "docker_cleanup_operator_action" {
			found = true
			if !d.OperatorActionRequired || d.ClearsAt != nil || d.Text != "b70: Docker cleanup unconfirmed; waiting for attempt old-job/1; operator action required (myce resolve-docker)" {
				t.Fatalf("escalation: %+v", d)
			}
		}
	}
	if !found {
		t.Fatalf("not escalated: %+v", snap.HoldsDetail)
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
		if len(s.HoldsDetail) != 1 || s.HoldsDetail[0].Kind != kind || s.HoldsDetail[0].Text != want || s.HoldsDetail[0].OperatorActionRequired || !reflect.DeepEqual(s.HostHolds, []string{want}) {
			t.Fatalf("%s: %+v %v", status, s.HoldsDetail, s.HostHolds)
		}
	}
	q.s.Machines[m.ID].DockerCleanup = ""
	closeQueueHost(q, "host", "private reason", nil)
	deadline := c.Now()
	q.s.Jobs["startup"] = &Job{ID: "startup", State: "failed", Spec: Spec{StartBy: &deadline}}
	r := HostRelease{Host: "host", Job: "startup", Machine: m.ID, Session: m.Session, Attempt: 1}
	q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
	s, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, d := range s.HoldsDetail {
		kinds[d.Kind] = true
		if d.OperatorActionRequired {
			t.Fatalf("operator fence: %+v", d)
		}
		if strings.Contains(d.Text, "private reason") {
			t.Fatal("private reason exposed")
		}
	}
	if !kinds["host_disabled"] || !kinds["startup_release"] || len(s.HostHolds) != 0 {
		t.Fatalf("fences: %+v", s)
	}
	data, _ := json.Marshal(s)
	if strings.Contains(string(data), `"host_holds"`) {
		t.Fatal("empty legacy list must be omitted")
	}
	if !strings.Contains(string(data), `"holds_detail":[`) {
		t.Fatal("missing details array")
	}
}

func TestHoldsDetailProductionKinds(t *testing.T) {
	q, c, a, _, _ := holdFleet(t, true)
	// Direct state fixtures isolate formatting from dispatch; snapshots above
	// exercise the maintained, public status path.
	h := q.s.Hosts["b70"]
	h.Active = true
	d := q.productionHoldDetail("b70")
	if d.Kind != "production_hold" || d.OperatorActionRequired || d.Text != "b70: production hold" {
		t.Fatalf("active: %+v", d)
	}
	r := HostRelease{Host: "b70", Job: "old", Machine: a.ID, Session: a.Session, Attempt: 1, Until: c.Now().Add(10 * time.Second)}
	q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
	d = q.productionHoldDetail("b70")
	if d.Kind != "production_stopping" || d.OperatorActionRequired || d.Text != "b70: production hold; stopping 1 attempts; grace remaining 10s" {
		t.Fatalf("grace: %+v", d)
	}
	c.Advance(10 * time.Second)
	d = q.productionHoldDetail("b70")
	if d.Kind != "production_release_unknown" || d.OperatorActionRequired || d.Text != "b70: production hold; release unknown: worker b70-0" {
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
	if !strings.Contains(string(data), `"holds_detail":[]`) || strings.Contains(string(data), `"host_holds"`) {
		t.Fatalf("empty holds fields: %s", data)
	}
}
