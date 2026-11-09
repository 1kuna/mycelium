package myce

import (
	"testing"
	"time"
)

func TestHostMemoryChargeCheckinPersistsAcrossRestart(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		m := cpuMachine("worker", "host", "cpu", 8, 16)
		m.ExtensionFingerprint = "memory-v1"
		m.HostMemoryBudgetGB = 12
		m.CapabilityProfile.SystemMemoryBytes = 16 << 30
		m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
		check(t, q, m, nil)
		spec := cpuSpec(1, 0)
		runMemory(t, q, clock, m, spec, 2, nil)
		pending := submit(t, q, spec)
		assigned := check(t, q, m, nil).Job
		if assigned == nil || assigned.ID != pending.ID || adaptiveMemoryCeilingGB(assigned, &m) == 0 || assigned.MemoryEnvelope.Basis != "learned" || assigned.MemoryEnvelope.ProbeFloorGB > 0 {
			t.Fatal("learned guard not assigned")
		}
		footprint := 6.0
		report := AttemptReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Session: assigned.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{FootprintGB: &footprint, NonreclaimableGB: 5, GrowthObserved: true}}
		if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
			t.Fatal(err)
		}
		grown := q.hostJobCharge(job(t, q, assigned.ID))
		if grown < 6 {
			t.Fatalf("peak not charged: %g", grown)
		}
		clock.Advance(20 * time.Second)
		report.CurrentHostMemory = nil
		if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
			t.Fatal(err)
		}
		q = reopenQueue(t, q, clock)
		if got := q.hostJobCharge(job(t, q, assigned.ID)); got < grown {
			t.Fatalf("restart released charge: %g < %g", got, grown)
		}

	})

	t.Run("HostMemoryGrownReleaseCustody", func(t *testing.T) {
		q, c := fixture(t)
		m := cpuMachine("worker", "host", "cpu", 8, 64)
		m.Devices = []MachineDevice{}
		m.HostMemoryBudgetGB = 48
		m.CapabilityProfile.SystemMemoryBytes = 64 << 30
		j := incidentGuardJob()
		j.ID, j.Machine, j.Session, j.Attempt, j.State = "holder", m.ID, m.Session, 1, "running"
		j.Spec.Needs.HostMemoryGB = 3
		q.s.Machines[m.ID], q.s.Jobs[j.ID] = &m, &j
		footprint := 9.0
		r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{FootprintGB: &footprint, NonreclaimableGB: 8, GrowthObserved: true}}
		q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: currentHostMemorySamples([]AttemptReport{r}, c.Now())}
		q.retainHostMemoryCharges(m.ID, c.Now())
		grown := q.hostJobCharge(q.s.Jobs[j.ID])
		q.retainRelease(q.s.Jobs[j.ID])
		q.requeue(q.s.Jobs[j.ID], "lost", false)
		q.refreshHostMemory()
		if got := q.s.Machines[m.ID].HostMemoryInUseGB; got < grown {
			t.Fatalf("unreleased grown charge fell from %g to %g on requeue, new 40GiB neighbor fits=%v, room=%g", grown, got, hostFits(q.s.Machines[m.ID], 0, 40), q.s.Machines[m.ID].FreeHostMemoryGB)
		}

	})
}
