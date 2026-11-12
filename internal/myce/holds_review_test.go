package myce

import (
	"strings"
	"testing"
)

func TestHoldsReviewHostDisableScope(t *testing.T) {
	q, _ := fixture(t)
	check(t, q, machine("b70-0", "fake", "b70"), nil)
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
