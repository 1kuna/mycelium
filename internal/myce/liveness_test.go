package myce

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestFleetReconnectDoesNotRefreshOtherWorkers(t *testing.T) {
	for _, tc := range []struct {
		priority string
		timeout  time.Duration
	}{{"production", ProductionLossAfter}, {"deadline", DeadlineLossAfter}, {"experiment", LossAfter}} {
		t.Run(tc.priority, func(t *testing.T) {
			q, c := fixture(t)
			dead := machine("dead", "fake")
			s := spec(tc.priority)
			s.AllowedMachines = []string{dead.ID}
			j := submit(t, q, s)
			assigned := check(t, q, dead, nil).Job
			if err := q.Started(result(assigned, true)); err != nil {
				t.Fatal(err)
			}
			live := machine("live", "fake")
			check(t, q, live, nil)
			last := q.s.Machines[dead.ID].LastSeen
			c.Advance(10 * time.Minute)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if !q.networkQuiet {
				t.Fatal("fleet outage was not detected")
			}
			check(t, q, live, nil)
			if !q.s.Machines[dead.ID].LastSeen.Equal(last) {
				t.Fatal("peer reconnect rewrote actual heartbeat")
			}
			c.Advance(tc.timeout - time.Nanosecond)
			check(t, q, live, nil)
			requireState(t, q, j.ID, "running")
			c.Advance(time.Nanosecond)
			check(t, q, live, nil)
			requireState(t, q, j.ID, "queued")
			if !q.s.Machines[dead.ID].Gone {
				t.Fatal("dead worker stayed joined")
			}
			if q.s.Jobs[j.ID].ReleaseConfirmed {
				t.Fatal("loss inferred release")
			}
			if !errors.Is(q.Started(result(assigned, true)), ErrStale) {
				t.Fatal("late stale execution was not stopped")
			}
		})
	}
}

func TestFleetPartialRecoveryHasOneDeadline(t *testing.T) {
	q, c := fixture(t)
	for _, id := range []string{"a", "b", "c", "d"} {
		check(t, q, machine(id, "fake"), nil)
	}
	last := q.s.Machines["d"].LastSeen
	c.Advance(10 * time.Minute)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	check(t, q, machine("a", "fake"), nil)
	c.Advance(30 * time.Second)
	check(t, q, machine("b", "fake"), nil)
	// A rejected old session must not revive or extend peers either.
	stale := machine("a", "fake")
	stale.Session = "stale"
	if _, err := q.Checkin(Checkin{Machine: stale}); err == nil {
		t.Fatal("accepted stale session")
	}
	c.Advance(30 * time.Second)
	check(t, q, machine("b", "fake"), nil)
	if !q.s.Machines["d"].Gone || !q.s.Machines["d"].LastSeen.Equal(last) {
		t.Fatal("partial recovery renewed dead worker grace")
	}
}

func TestFleetContinuousSurvivorDoesNotMaskLoss(t *testing.T) {
	for _, tc := range []struct {
		priority string
		timeout  time.Duration
	}{{"production", ProductionLossAfter}, {"deadline", DeadlineLossAfter}, {"experiment", LossAfter}} {
		t.Run(tc.priority, func(t *testing.T) {
			q, c := fixture(t)
			for _, id := range []string{"dead-a", "dead-b"} {
				s := spec(tc.priority)
				s.AllowedMachines = []string{id}
				submit(t, q, s)
				assigned := check(t, q, machine(id, "fake"), nil).Job
				if err := q.Started(result(assigned, true)); err != nil {
					t.Fatal(err)
				}
			}
			live := machine("live", "fake")
			check(t, q, live, nil)
			for elapsed := time.Second; elapsed <= tc.timeout; elapsed += time.Second {
				c.Advance(time.Second)
				check(t, q, live, nil)
			}
			for _, id := range []string{"dead-a", "dead-b"} {
				if !q.s.Machines[id].Gone {
					t.Fatalf("%s stayed joined beyond %s", id, tc.timeout)
				}
			}
		})
	}
}

func TestFleetRecoveryEpochSurvivesQueueRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	root := t.TempDir()
	c := &fakeClock{now: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"live", "dead"} {
		check(t, q, machine(id, "fake"), nil)
	}
	last := q.s.Machines["dead"].LastSeen
	c.Advance(10 * time.Minute)
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	check(t, q, machine("live", "fake"), nil)
	epoch := q.s.NetworkRecoveryAt
	if epoch.IsZero() {
		t.Fatal("missing recovery epoch")
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	c.Advance(30 * time.Second)
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.SetRestartGrace(10 * time.Second)
	if !q.s.NetworkRecoveryAt.Equal(epoch) || !q.s.Machines["dead"].LastSeen.Equal(last) {
		t.Fatal("restart rewrote outage/heartbeat history")
	}
	check(t, q, machine("live", "fake"), nil)
	c.Advance(10 * time.Second)
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	if !q.s.Machines["dead"].Gone {
		t.Fatal("outage epoch extended queue restart grace")
	}
}
