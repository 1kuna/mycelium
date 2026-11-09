package myce

import (
	"testing"
)

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
