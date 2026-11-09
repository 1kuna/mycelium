package myce

import (
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

// Legacy control: an idle leaving worker becomes Gone on its next check-in.

func TestQCLeaveMarksGoneMachine(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		m := m3Machine()
		machineCheck(t, q, m)
		if err := q.Leave(m.ID, false); err != nil {
			t.Fatal(err)
		}
		r := machineCheck(t, q, m)
		if !r.Leave {
			t.Fatal("no leave directive")
		}
		if !qcMachineGone(t, q, m.ID) {
			// Show the consequence: a fresh rejoin under a new session is refused.
			clock.Advance(time.Second)
			m2 := m
			m2.Session = "rejoin"
			_, err := q.Checkin(Checkin{Machine: m2, AttemptProtocol: keyedAttemptProtocol})
			t.Fatalf("machine worker leave never marked Gone; rejoin err=%v", err)
		}
		clock.Advance(time.Second)
		m.Session = "rejoin"
		if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}); err != nil {
			t.Fatal("fresh rejoin refused", err)
		}

	})

	t.Run("QCLeaveMarksGoneLegacy", func(t *testing.T) {
		q, _ := fixture(t)
		m := machine("legacy", "fake", "legacy-host")
		check(t, q, m, nil)
		if err := q.Leave(m.ID, false); err != nil {
			t.Fatal(err)
		}
		r := check(t, q, m, nil)
		if !r.Leave || !qcMachineGone(t, q, m.ID) {
			t.Fatal("legacy leave did not finish", r.Leave)
		}

	})

	t.Run("QCLeaveWaitsForExactMachineRelease", func(t *testing.T) {
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

	})
}

func TestQCKillPhaseDoesNotBlockNeighborDevice(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m3Machine()))
	if a == nil {
		t.Fatal("setup")
	}
	if err := q.Cancel(a.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(StopGrace + time.Second)
	n := spec("experiment")
	n.Needs.GPU = &GPUSelection{Devices: []string{"1"}}
	neighbor := submit(t, q, n)
	for i := 0; i < 3; i++ {
		r := machineCheck(t, q, m3Machine(), a)
		kill := false
		for _, d := range r.Attempts {
			kill = kill || d.ID == a.ID && d.Kill
		}
		if got := assignedJob(r); got != nil {
			t.Logf("neighbor admitted on check-in %d (kill=%v)", i, kill)
			if got.ID != neighbor.ID {
				t.Fatalf("wrong neighbor: %s", got.ID)
			}
			return
		}
		t.Logf("check-in %d: kill=%v no admission", i, kill)
		clock.Advance(time.Minute)
	}
	t.Fatalf("wedged kill on device 0 fenced device 1 admission; neighbor=%s", job(t, q, neighbor.ID).State)
}
