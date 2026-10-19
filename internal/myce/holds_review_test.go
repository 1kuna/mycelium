package myce

import (
	"testing"
	"time"
)

func TestHoldsReviewHostDisableScope(t *testing.T) {
	q, _, _, _, _ := holdFleet(t, false)
	assert := func(want bool) {
		t.Helper()
		s, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, d := range s.HoldsDetail {
			if d.Host == "b70" && d.Kind == "host_disabled" {
				found = true
				if d.OperatorActionRequired || d.ClearsAt != nil {
					t.Fatalf("disable is not a fault: %+v", d)
				}
			}
		}
		if found != want {
			t.Fatalf("host disable=%v want=%v: %+v", found, want, s.HoldsDetail)
		}
	}
	assert(false)
	if err := setMaintenance(t, q, "b70", "disable"); err != nil {
		t.Fatal(err)
	}
	assert(true)
	if err := setMaintenance(t, q, "b70", "resume"); err != nil {
		t.Fatal(err)
	}
	assert(false)
}

func TestHoldsReviewProductionReleaseAttention(t *testing.T) {
	for _, mode := range []string{"just-past-grace", "well-past-grace", "restart", "gone", "failed"} {
		t.Run(mode, func(t *testing.T) {
			q, c, a, _, _ := holdFleet(t, true)
			q.s.Hosts["b70"].Active = true
			r := HostRelease{Host: "b70", Job: "old", Machine: a.ID, Session: a.Session, Attempt: 1, Until: c.Now().Add(-time.Second)}
			want := false
			switch mode {
			case "well-past-grace":
				r.Until = c.Now().Add(-2 * time.Minute)
				want = true
			case "restart":
				r.Until = c.Now().Add(-10 * time.Minute)
				q.unreported[a.ID] = true
				q.restartUntil = c.Now().Add(time.Minute)
			case "gone":
				q.s.Machines[a.ID].Gone = true
				want = true
			case "failed":
				r.CleanupFailed = true
				want = true
			}
			q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
			d := q.productionHoldDetail("b70")
			if d.Kind != "production_release_unknown" || d.OperatorActionRequired != want || d.ClearsAt != nil {
				t.Fatalf("%s: %+v", mode, d)
			}
		})
	}
}

func TestHoldsReviewModelReleaseAttention(t *testing.T) {
	for _, mode := range []string{"draining-calls", "recovering", "kill-pending", "well-past-grace", "restart", "gone", "failed-unload"} {
		t.Run(mode, func(t *testing.T) {
			q, c, a, _, _ := holdFleet(t, true)
			q.s.Hosts["b70"].Active = true
			deadline := c.Now().Add(-time.Second)
			slot := &ModelSlot{Machine: a.ID, Model: "model", Session: a.Session, State: "draining", StopAt: &deadline}
			want := false
			switch mode {
			case "draining-calls":
				slot.ReleasingCalls = true
				slot.StopAt = nil
			case "recovering":
				slot.Recovering = true
				slot.StopAt = nil
			case "well-past-grace":
				deadline = c.Now().Add(-2 * time.Minute)
				want = true
			case "restart":
				deadline = c.Now().Add(-10 * time.Minute)
				q.unreported[a.ID] = true
				q.restartUntil = c.Now().Add(time.Minute)
			case "gone":
				q.s.Machines[a.ID].Gone = true
				want = true
			case "failed-unload":
				slot.State = "unload_failed"
				want = true
			}
			q.s.ModelSlots[slotKey(a.ID, slot.Model)] = slot
			d := q.productionHoldDetail("b70")
			if d.Kind != "production_release_unknown" || d.OperatorActionRequired != want || d.ClearsAt != nil {
				t.Fatalf("%s: %+v", mode, d)
			}
		})
	}
}

func TestHoldsReviewDockerSeverityDeterministic(t *testing.T) {
	q, c, a, b, _ := holdFleet(t, false)
	q.s.Machines[a.ID].DockerCleanup = "creating"
	q.s.Machines[b.ID].DockerCleanup = "cleanup unconfirmed: inspect"
	r := HostRelease{Host: "b70", Job: "old", Machine: a.ID, Session: "old", Attempt: 1, Docker: true}
	q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
	assert := func(kind, text string) {
		t.Helper()
		for i := 0; i < 100; i++ {
			d := q.dockerHoldDetail("b70")
			if d.Kind != kind || d.Text != text {
				t.Fatalf("iteration %d: %+v", i, d)
			}
		}
	}
	assert("docker_cleanup_waiting", "b70: Docker cleanup unconfirmed; cleanup unconfirmed: inspect")
	q.s.Machines[b.ID].DockerCleanup = "start pending"
	assert("docker_cleanup_waiting", "b70: Docker cleanup unconfirmed; waiting for attempt old/1")
	now := c.Now()
	r.EscalatedAt = &now
	q.s.HostReleases[releaseKey(r)] = r
	q.s.Machines[b.ID].DockerCleanup = "cleanup unconfirmed: inspect"
	assert("docker_cleanup_operator_action", "b70: Docker cleanup unconfirmed; waiting for attempt old/1; operator action required (myce resolve-docker)")
	delete(q.s.HostReleases, releaseKey(r))
	q.s.Machines[b.ID].DockerCleanup = "creating"
	q.s.Machines[a.ID].DockerCleanup = "start pending"
	assert("docker_transition", "b70: Docker cleanup unconfirmed; creating") // lexical tie-break.
}

func TestHoldsReviewStartupGone(t *testing.T) {
	q, c := fixture(t)
	m := machine("worker", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	now := c.Now()
	q.s.Jobs["startup"] = &Job{ID: "startup", State: "failed", Spec: Spec{StartBy: &now}}
	r := HostRelease{Host: "host", Job: "startup", Machine: m.ID, Session: m.Session, Attempt: 1}
	q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
	for _, gone := range []bool{false, true} {
		q.s.Machines[m.ID].Gone = gone
		s, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, d := range s.HoldsDetail {
			if d.Kind == "startup_release" {
				found = true
				if d.OperatorActionRequired != gone || d.ClearsAt != nil {
					t.Fatalf("gone=%v: %+v", gone, d)
				}
			}
		}
		if !found {
			t.Fatal("startup fence missing")
		}
	}
}

func TestHoldsReviewUnclassifiedFailsClosed(t *testing.T) {
	for _, kind := range []string{"", "new-kind"} {
		d := productionBlockerDetail("host", "future blocker", hostBlockerClass{Kind: kind})
		if d.Kind != "unclassified" || !d.OperatorActionRequired || d.ClearsAt != nil || d.Text != "host: production hold; future blocker" {
			t.Fatalf("unclassified blocker: %+v", d)
		}
	}
}

func TestHoldsReviewCallAttention(t *testing.T) {
	q, c, a, _, _ := holdFleet(t, true)
	q.s.Hosts["b70"].Active = true
	deadline := c.Now().Add(-time.Second)
	q.calls["call"] = &ModelCall{ID: "call", Machine: a.ID, Priority: "experiment", StopAt: &deadline}
	d := q.productionHoldDetail("b70")
	if d.Kind != "production_release_unknown" || d.OperatorActionRequired {
		t.Fatalf("kill result pending: %+v", d)
	}
	c.Advance(releaseAttentionDelay - time.Second)
	d = q.productionHoldDetail("b70")
	if !d.OperatorActionRequired {
		t.Fatalf("late release: %+v", d)
	}
}

func TestHoldsReviewUnknownDockerStatusFailsClosed(t *testing.T) {
	q, _, a, b, _ := holdFleet(t, false)
	q.s.Machines[a.ID].DockerCleanup = "pull pending"
	q.s.Machines[b.ID].DockerCleanup = "cleanup unconfirmed: inspect"
	d := q.dockerHoldDetail("b70")
	if d.Kind != "unclassified" || !d.OperatorActionRequired || d.ClearsAt != nil || d.Text != "b70: Docker cleanup unconfirmed; pull pending" {
		t.Fatalf("unknown Docker status must fail closed: %+v", d)
	}
	now := q.clock.Now()
	r := HostRelease{Host: "b70", Job: "old", Machine: a.ID, Session: "old", Attempt: 1, Docker: true, EscalatedAt: &now}
	q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
	d = q.dockerHoldDetail("b70")
	if d.Kind != "docker_cleanup_operator_action" || !d.OperatorActionRequired {
		t.Fatalf("unknown status hid escalation: %+v", d)
	}
}
