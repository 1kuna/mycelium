package myce

import (
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
		q.s.Machines[a.ID].DockerCleanup = "cleanup unconfirmed: inspect"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b70: Docker cleanup unconfirmed; cleanup unconfirmed: inspect"}
	if !reflect.DeepEqual(snap.HostHolds, want) {
		t.Fatalf("legacy strings changed: %v", snap.HostHolds)
	}
	if len(snap.HoldsDetail) != 1 {
		t.Fatalf("retired idle hold remains: %+v", snap.HoldsDetail)
	}
	cleanup := snap.HoldsDetail[0]
	if cleanup.Kind != "docker_cleanup_waiting" || cleanup.Host != "b70" || cleanup.OperatorActionRequired || cleanup.ClearsAt != nil || cleanup.Text != want[0] {
		t.Fatalf("Docker blocker: %+v", cleanup)
	}
	// An attempt-specific custody record drives the existing escalation timer.
	r := HostRelease{Host: "b70", Job: "old-job", Machine: a.ID, Session: "old-session", Attempt: 1, Docker: true, CleanupSince: &idle}
	q.s.HostReleases[releaseKey(r)] = r
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

	t.Run("HoldsDetailKindsAndLegacyCompatibility", func(t *testing.T) {
		q, c := fixture(t)
		m := machine("worker", "fake")
		m.HostID = "host"
		check(t, q, m, nil)
		setMaintenance(t, q, "host", "disable")
		if err := q.update(func() error {
			deadline := c.Now()
			q.s.Jobs["startup"] = &Job{ID: "startup", State: "failed", Spec: Spec{StartBy: &deadline}}
			r := HostRelease{Host: "host", Job: "startup", Machine: m.ID, Session: m.Session, Attempt: 1}
			q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
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
	})

	t.Run("HoldsReviewHostDisableScope", func(t *testing.T) {
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

	})

	t.Run("HoldsReviewStartupGone", func(t *testing.T) {
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

	})
}
