package myce

import (
	"strings"

	"testing"
)

func TestUnitControlCounterRecoveryAndHostMove(t *testing.T) {
	q, _ := fixture(t)
	m := machine("idle", "fake")
	m.HostID = "old-host"
	check(t, q, m, nil)
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, nil, 10); err != nil {
		t.Fatal(err)
	}
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, nil, 1); err == nil || !strings.Contains(err.Error(), "at least 10") {
		t.Fatalf("counter recovery guidance: %v", err)
	}
	// Restoring the durable counter to the floor makes the next disable usable.
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, nil, 11); err != nil {
		t.Fatal(err)
	}
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", true, false, nil, 11); err == nil || !strings.Contains(err.Error(), "already records") || strings.Contains(err.Error(), "restore") {
		t.Fatalf("opposite operation guidance: %v", err)
	}
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", true, false, nil, 12); err != nil {
		t.Fatal(err)
	}
	m.HostID = "new-host"
	m.Session = "new-session"
	check(t, q, m, nil)
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := q.ControlMachine(m.HostID, m.ID, m.Session, "", true, false, nil, 2); err != nil {
		t.Fatal(err)
	}
	if q.s.Machines[m.ID].Disabled != "" {
		t.Fatal("moved machine stayed disabled")
	}
	if err := q.ControlMachine("old-host", m.ID, "session-idle", "", false, true, nil, 99); err == nil {
		t.Fatal("former host accepted")
	}
}

func TestUnitDisableRetainsModelsWithoutMatchingCompleteEvidence(t *testing.T) {
	for _, mode := range []string{"outstanding", "other-session"} {
		t.Run(mode, func(t *testing.T) {
			f := newUnitDisableFixture(t, false)
			m := f.machines[0]
			if mode == "other-session" {
				m = f.machines[1]
			}
			f.q.s.ModelSlots = map[string]*ModelSlot{slotKey(m.ID, "model"): {Model: "model", Machine: m.ID, Session: m.Session, State: "ready"}}
			session := m.Session
			if mode == "other-session" {
				session = "previous-session"
			}
			err := f.q.ControlMachine("host", m.ID, session, "", false, true, nil, 1)
			if mode == "other-session" {
				if err == nil {
					t.Fatal("stale session accepted")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if f.q.s.ModelSlots[slotKey(m.ID, "model")] == nil {
				t.Fatal("uncovered model cleared")
			}
		})
	}
}

func TestUnitControlStaleIdleSessionCannotShadowCleanup(t *testing.T) {
	f := newUnitDisableFixture(t, false)
	m := f.machines[0]
	// A stale idle registry entry can notify first at this same unit generation.
	if err := f.q.ControlMachine(m.HostID, m.ID, "old-session", "", false, true, nil, 1); err == nil {
		t.Fatal("stale session accepted")
	}
	r := result(f.jobs[0], true)
	if err := f.q.ControlMachine(m.HostID, m.ID, m.Session, "", false, true, []Result{r}, 1); err != nil {
		t.Fatal(err)
	}
	assertNoUnitDockerHold(t, f.q)
	if !f.q.s.Machines[m.ID].Gone {
		t.Fatal("live cleanup shadowed by stale idle session")
	}
}

// A failed cleanup replay must preserve its evidence until a later retry settles it.
