package myce

import (
	"strings"
	"testing"
	"time"
)

func qcMachineGone(t *testing.T, q *Queue, id string) bool {
	t.Helper()
	s, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Machines {
		if m.ID == id {
			return m.Gone
		}
	}
	t.Fatal("missing machine", id)
	return false
}

func TestQCPreferredPlatformDeferral(t *testing.T) {
	q, _ := fixture(t)
	metal := machine("mac", "metal", "mac-host")
	check(t, q, metal, nil)
	m := m3Machine()
	machineCheck(t, q, m)
	s := spec("experiment")
	s.Needs.Platform = Platform{Preferred: []string{"metal"}}
	j := submit(t, q, s)
	if got := assignedJob(machineCheck(t, q, m)); got != nil {
		t.Fatalf("machine worker took %s although preferred idle metal exists", got.ID)
	}
	if got := check(t, q, metal, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("preferred machine did not get job")
	}
}

func TestQCLeaveWaitsForExactMachineRelease(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	submit(t, q, spec("experiment"))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil {
		t.Fatal("setup")
	}
	if err := q.Leave(m.ID, true); err != nil {
		t.Fatal(err)
	}
	clock.Advance(StopGrace + time.Second)
	machineCheck(t, q, m) // omission is not proof of release
	if qcMachineGone(t, q, m.ID) {
		t.Fatal("leave discarded unacknowledged custody")
	}
	finishInterrupted(t, q, a)
	machineCheck(t, q, m)
	if !qcMachineGone(t, q, m.ID) {
		t.Fatal("leave did not finish after release")
	}
}

func TestQCCheckpointWarningOncePerMachine(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.CheckpointUnavailable = true
	machineCheck(t, q, m)
	s, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(s.Warning, "unreachable on worker "+m.ID); got != 1 {
		t.Fatalf("warning count=%d: %s", got, s.Warning)
	}
}
