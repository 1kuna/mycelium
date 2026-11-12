package myce

import (
	"strings"
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
		for _, d := range s.Holds {
			if d.Scope == "b70" && d.Kind == "host_disabled" {
				found = true
				if !d.operatorAction() || d.Expires != nil || !strings.Contains(d.Next, "myce host resume b70") {
					t.Fatalf("disable is the operator's to resume, with no expiry: %+v", d)
				}
			}
		}
		if found != want {
			t.Fatalf("host disable=%v want=%v: %+v", found, want, s.Holds)
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
			d := q.productionHold("b70")
			if d.Kind != "production_release_unknown" || d.operatorAction() != want || d.Expires != nil {
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
			d := q.productionHold("b70")
			if d.Kind != "production_release_unknown" || d.operatorAction() != want || d.Expires != nil {
				t.Fatalf("%s: %+v", mode, d)
			}
		})
	}
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
		for _, d := range s.Holds {
			if d.Kind == "startup_release" {
				found = true
				if d.operatorAction() != gone || d.Expires != nil {
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
		d := productionBlockerHold("host", "future blocker", hostBlockerClass{Kind: kind})
		if d.Kind != "unclassified" || !d.operatorAction() || d.Expires != nil || d.Detail != "host: production hold; future blocker" {
			t.Fatalf("unclassified blocker: %+v", d)
		}
	}
}

func TestHoldsReviewCallAttention(t *testing.T) {
	q, c, a, _, _ := holdFleet(t, true)
	q.s.Hosts["b70"].Active = true
	deadline := c.Now().Add(-time.Second)
	q.calls["call"] = &ModelCall{ID: "call", Machine: a.ID, Priority: "experiment", StopAt: &deadline}
	d := q.productionHold("b70")
	if d.Kind != "production_release_unknown" || d.operatorAction() {
		t.Fatalf("kill result pending: %+v", d)
	}
	c.Advance(releaseAttentionDelay - time.Second)
	d = q.productionHold("b70")
	if !d.operatorAction() {
		t.Fatalf("late release: %+v", d)
	}
}
