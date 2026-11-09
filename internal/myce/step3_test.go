package myce

import (
	"strings"
	"testing"
	"time"
)

func TestPriorityLossFakeClock(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, tc := range []struct {
			p    string
			wait time.Duration
		}{{"production", 5 * time.Second}, {"deadline", 20 * time.Second}, {"experiment", 60 * time.Second}} {
			t.Run(tc.p, func(t *testing.T) {
				q, c := fixture(t)
				j := submit(t, q, spec(tc.p))
				m := machine("lost", "fake")
				check(t, q, m, nil)
				peer := observer()
				check(t, q, peer, nil)
				c.Advance(tc.wait - time.Nanosecond)
				check(t, q, peer, nil)
				requireState(t, q, j.ID, "running")
				c.Advance(time.Nanosecond)
				check(t, q, peer, nil)
				requireState(t, q, j.ID, "queued")
			})
		}
		t.Run("custom", func(t *testing.T) {
			q, c := fixture(t)
			if err := q.SetLossTimeouts(9*time.Second, 2*time.Second, time.Second); err != nil {
				t.Fatal(err)
			}
			j := submit(t, q, spec("production"))
			check(t, q, machine("lost", "fake"), nil)
			peer := observer()
			check(t, q, peer, nil)
			c.Advance(8 * time.Second)
			check(t, q, peer, nil)
			requireState(t, q, j.ID, "running")
			c.Advance(time.Second)
			check(t, q, peer, nil)
			requireState(t, q, j.ID, "queued")
		})

	})

	t.Run("ProductionModelLoadingLossFakeClock", func(t *testing.T) {
		q, c, m := modelFixture(t)
		addModelCall(t, q, "production")
		peer := observer()
		check(t, q, peer, nil)
		c.Advance(ProductionLossAfter - time.Nanosecond)
		check(t, q, peer, nil)
		if q.s.Machines[m.ID].Gone {
			t.Fatal("loading production worker lost early")
		}
		c.Advance(time.Nanosecond)
		check(t, q, peer, nil)
		if !q.s.Machines[m.ID].Gone {
			t.Fatal("loading production model ignored production loss threshold")
		}

	})
}
func TestMajoritySilenceProtectsProductionFakeClock(t *testing.T) {
	q, c := fixture(t)
	for _, id := range []string{"a", "b", "c"} {
		s := spec("production")
		s.AllowedMachines = []string{id}
		submit(t, q, s)
		check(t, q, machine(id, "fake"), nil)
	}
	c.Advance(5 * time.Second)
	m := machine("c", "fake")
	q.mu.Lock()
	q.s.Machines[m.ID].LastSeen = c.Now()
	q.mu.Unlock()
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	snap, _ := q.Snapshot()
	if !strings.Contains(snap.Warning, "network") {
		t.Fatal("majority loss not held")
	}
	for _, j := range snap.Jobs {
		if j.State != "running" {
			t.Fatal("fleet job moved during majority outage")
		}
	}
}
