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

// Legacy control: an idle leaving worker becomes Gone on its next check-in.
func TestQCLeaveMarksGoneLegacy(t *testing.T) {
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
}

func TestQCLeaveMarksGoneMachine(t *testing.T) {
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

func TestQCKillPhaseRetainsDeviceCPUAndRAM(t *testing.T) {
	for _, resource := range []string{"device", "cpu", "ram"} {
		t.Run(resource, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			s := m3Spec("experiment", "0", 1, 1)
			s.Needs.CPUCores = 1
			submit(t, q, s)
			a := assignedJob(machineCheck(t, q, m))
			if a == nil {
				t.Fatal("setup")
			}
			if err := q.Cancel(a.ID); err != nil {
				t.Fatal(err)
			}
			clock.Advance(StopGrace + time.Second)
			var demand Spec
			switch resource {
			case "device":
				demand = m3Spec("experiment", "0", 1, 1)
			case "cpu":
				demand = cpuSpec(q.s.Machines[m.ID].EffectiveCPUCores, 1)
			case "ram":
				demand = cpuSpec(1, m.HostAvailableMemoryGB)
			}
			waiting := submit(t, q, demand)
			if got := assignedJob(machineCheck(t, q, m, a)); got != nil {
				t.Fatal("unreleased resources reused", got.ID)
			}
			if err := q.Complete(Result{Machine: a.Machine, Session: a.Session, Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Interrupted: true}); err != nil {
				t.Fatal(err)
			}
			got := assignedJob(machineCheck(t, q, m))
			if got == nil || got.ID != waiting.ID {
				t.Fatal("released resources unavailable", got)
			}
		})
	}
}

func TestQCKillPhaseAllowsCPUBackfill(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, m3Spec("experiment", "0", 1, 1))
	a := assignedJob(machineCheck(t, q, m3Machine()))
	if a == nil {
		t.Fatal("setup")
	}
	if err := q.Cancel(a.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(StopGrace + time.Second)
	cpu := submit(t, q, cpuSpec(1, 1))
	got := assignedJob(machineCheck(t, q, m3Machine(), a))
	if got == nil || got.ID != cpu.ID {
		t.Fatal("kill fenced free CPU resources", got)
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

func TestQCRetiredCPUHint(t *testing.T) {
	q, _ := fixture(t)
	q.s.Machines["old"] = &Machine{ID: "old", Replacement: &MachineReplacement{Machine: "new"}}
	s := cpuSpec(1, 1)
	s.AllowedMachines = []string{"old"}
	_, err := q.Submit(s)
	if err == nil || !strings.Contains(err.Error(), "allowed_machines: [new]") || strings.Contains(err.Error(), "needs.gpu") {
		t.Fatalf("invalid CPU guidance: %v", err)
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
func TestQCMachineCheckPreservesInventory(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	submit(t, q, spec("experiment"))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil {
		t.Fatal("setup")
	}
	before := m.Devices[0].FreeMemoryGB
	machineCheck(t, q, m, a)
	if m.Devices[0].FreeMemoryGB != before {
		t.Fatal("helper mutated caller inventory")
	}
}
