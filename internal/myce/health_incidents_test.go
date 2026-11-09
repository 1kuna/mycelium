package myce

import (
	"testing"
)

func requireIncident(t *testing.T, q *Queue, kind, key string, count int, active bool) {
	t.Helper()
	if got := alertsOf(q, kind); len(got) != count || q.s.Health.Conditions[key].Active != active {
		t.Fatalf("%s: events=%+v condition=%+v; want count=%d active=%v", kind, got, q.s.Health.Conditions[key], count, active)
	}
}

func TestHealthIncidentStaleFlapReplay(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "cuda")
	key, kind := "stale-flaps/worker", "worker_stale_flapping"
	check(t, q, m, nil)
	freshness := q.machineLossTimeout(m.ID)
	// An ordinary healthy history, then one continuously quiet worker, is quiet.
	for range 3 {
		clock.Advance(freshness / 2)
		check(t, q, m, nil)
	}
	clock.Advance(q.machineLossTimeout(m.ID))
	for range 3 {
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	requireIncident(t, q, kind, key, 0, false)
	for i := range 2 {
		check(t, q, m, nil) // Same session, just like #661.
		clock.Advance(q.machineLossTimeout(m.ID))
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		requireIncident(t, q, kind, key, i, i == 1)
	}
	check(t, q, m, nil)
	clock.Advance(staleFlapPeriod)
	check(t, q, m, nil)
	requireIncident(t, q, kind, key, 1, false)
	// Planned leave doesn't seed the next burst of flaps.
	m.Leaving = true
	check(t, q, m, nil)
	clock.Advance(q.machineLossTimeout(m.ID))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	requireIncident(t, q, kind, key, 1, false)
}
