package myce

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"
)

func dockerEscalationFixture(t *testing.T) (*Queue, *fakeClock, *Job, *Machine) {
	t.Helper()
	q, c := fixture(t)
	m := machine("worker", "fake")
	m.HostID = "host"
	q.s.Machines[m.ID] = &m
	now := c.Now()
	j := &Job{ID: "job", Attempt: 1, Machine: m.ID, Session: m.Session, State: "running", Queued: now, StartedAt: &now}
	j.Spec.Priority = "production"
	j.Spec.Run.Docker = true
	q.s.Jobs[j.ID] = j
	q.retainRelease(j)
	return q, c, j, &m
}

func TestDockerEscalationHealthyCreateThenCancel(t *testing.T) {
	q, c, j, m := dockerEscalationFixture(t)
	key := releaseKey(HostRelease{Job: j.ID, Machine: m.ID, Session: m.Session, Attempt: 1})
	m.DockerCleanup = "creating"
	q.escalateDockerCleanup()
	r := q.s.HostReleases[key]
	if r.CleanupSince != nil || r.EscalatedAt != nil {
		t.Fatal("healthy create started cleanup timer")
	}
	// Old persisted false timers must be reset when the exact execution is live.
	old := c.Now().Add(-5 * time.Minute)
	r.CleanupSince = &old
	r.EscalatedAt = &old
	q.s.HostReleases[key] = r
	m.DockerCleanup = ""
	q.escalateDockerCleanup()
	r = q.s.HostReleases[key]
	if r.CleanupSince != nil || r.EscalatedAt != nil {
		t.Fatal("healthy execution retained false timer")
	}
	c.Advance(10 * time.Minute)
	q.retainRelease(j)
	// A persisted, un-escalated startup timer must not survive a new stop even
	// without an intervening maintenance pass after upgrading the queue.
	r = q.s.HostReleases[key]
	r.CleanupSince = &old
	q.s.HostReleases[key] = r
	q.stop(j, "cancel requested")
	m.DockerCleanup = "stopping"
	q.retainRelease(j)
	q.escalateDockerCleanup()
	r = q.s.HostReleases[key]
	if r.CleanupSince == nil || !r.CleanupSince.Equal(*j.StopAt) || r.EscalatedAt != nil {
		t.Fatalf("cancel timer: %+v", r)
	}
	c.Advance(j.StopAt.Sub(c.Now()) + 2*time.Minute - time.Nanosecond)
	q.escalateDockerCleanup()
	if q.s.HostReleases[key].EscalatedAt != nil {
		t.Fatal("normal cancel escalated before bound")
	}
	c.Advance(time.Nanosecond)
	q.escalateDockerCleanup()
	if q.s.HostReleases[key].EscalatedAt == nil {
		t.Fatal("unconfirmed stop did not escalate")
	}
}

func TestDockerEscalationSlowCreate(t *testing.T) {
	for _, status := range []string{"creating", "create pending", "network-create pending", "start pending"} {
		t.Run(status, func(t *testing.T) {
			q, c, _, m := dockerEscalationFixture(t)
			m.DockerCleanup = status
			q.escalateDockerCleanup()
			c.Advance(130 * time.Second)
			q.escalateDockerCleanup()
			for _, r := range q.s.HostReleases {
				if r.CleanupSince != nil || r.EscalatedAt != nil {
					t.Fatalf("slow create escalated: %+v", r)
				}
			}
			if q.dockerHoldDetail("host").OperatorActionRequired {
				t.Fatal("slow create operator hold")
			}
		})
	}
}

func TestDockerEscalationLostOrFailedCleanup(t *testing.T) {
	for _, mode := range []string{"gone", "failed", "wrong-session", "wrong-attempt"} {
		t.Run(mode, func(t *testing.T) {
			q, c, j, m := dockerEscalationFixture(t)
			m.DockerCleanup = "creating"
			switch mode {
			case "gone":
				m.Gone = true
			case "failed":
				m.DockerCleanup = "cleanup unconfirmed: create"
			case "wrong-session":
				m.Session = "replacement"
			case "wrong-attempt":
				j.Attempt++
			}
			q.escalateDockerCleanup()
			c.Advance(2 * time.Minute)
			q.escalateDockerCleanup()
			if !q.dockerHoldDetail("host").OperatorActionRequired || len(q.s.HostReleases) != 1 {
				t.Fatal("unconfirmed custody did not escalate or was released")
			}
		})
	}
}

func TestDockerEscalationSurvivesStopRequest(t *testing.T) {
	q, c, j, m := dockerEscalationFixture(t)
	m.DockerCleanup = "cleanup unconfirmed: create"
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)
	q.escalateDockerCleanup()
	c.Advance(2 * time.Minute)
	q.escalateDockerCleanup()
	key := releaseKey(HostRelease{Job: j.ID, Machine: m.ID, Session: m.Session, Attempt: j.Attempt})
	escalated := q.s.HostReleases[key].EscalatedAt
	if escalated == nil {
		t.Fatal("cleanup did not escalate")
	}
	assert := func() {
		t.Helper()
		r := q.s.HostReleases[key]
		d := q.dockerHoldDetail("host")
		if len(q.s.HostReleases) != 1 || r.EscalatedAt == nil || !r.EscalatedAt.Equal(*escalated) || d.Kind != "docker_cleanup_operator_action" || !d.OperatorActionRequired || d.ClearsAt != nil {
			t.Fatalf("escalation withdrawn: release=%+v detail=%+v", r, d)
		}
		if count := strings.Count(logs.String(), "event=docker_cleanup_escalated "); count != 1 {
			t.Fatalf("escalation events=%d, want 1", count)
		}
	}
	assert()
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	if j.StopAt == nil {
		t.Fatal("cancel did not request a stop")
	}
	q.retainRelease(j)
	assert() // A first stop must not withdraw the existing operator hold.
	q.escalateDockerCleanup()
	assert()
	c.Advance(StopGrace + 2*time.Minute)
	q.retainRelease(j)
	q.escalateDockerCleanup()
	assert() // Crossing the later stop bound must not log a second escalation.
}
