package myce

import (
	"testing"
	"time"
)

func TestNeverStartedLearnedAllocationKeepsBoundedCharge(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 64)
	m.ExtensionFingerprint = "memory-v1"
	m.HostMemoryBudgetGB = 48
	m.CapabilityProfile.SystemMemoryBytes = 64 << 30
	m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
	check(t, q, m, nil)
	spec := cpuSpec(1, 0)
	runMemory(t, q, clock, m, spec, 2, nil)
	pending := submit(t, q, spec)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != pending.ID || adaptiveMemoryCeilingGB(assigned, &m) == 0 || assigned.MemoryEnvelope.Basis != "learned" || assigned.MemoryEnvelope.ProbeFloorGB > 0 {
		t.Fatal("learned guard not assigned")
	}
	// Replay the incident's concurrent declared custody. A whole-host charge for
	// the auto allocation plus this sibling exceeds the allowance. AdmissionBlocked
	// would hold startup, preventing the first sample the old charge required.
	sibling := submit(t, q, cpuSpec(1, 6.125))
	sibling = q.s.Jobs[sibling.ID]
	sibling.Machine, sibling.Session, sibling.Attempt, sibling.State = m.ID, m.Session, 1, "running"
	started := clock.Now()
	sibling.StartedAt = &started
	siblingReport := AttemptReport{Execution: Execution{ID: sibling.ID, Attempt: sibling.Attempt}, Session: sibling.Session, Started: true}
	report := AttemptReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Session: assigned.Session}
	for _, elapsed := range []time.Duration{0, 30 * time.Minute} {
		// No process or sample ever exists for the learned allocation.
		clock.Advance(elapsed)
		reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{siblingReport, report}})
		if err != nil {
			t.Fatal(err)
		}
		if reply.AdmissionBlocked {
			t.Fatalf("whole-host charge trapped never-started allocation: elapsed=%s reserved=%g budget=%g", elapsed, q.s.Machines[m.ID].HostMemoryInUseGB, m.HostMemoryBudgetGB)
		}
		holder := job(t, q, assigned.ID)
		if holder.StartedAt != nil || holder.State != "running" {
			t.Fatalf("unexpected allocation state: %+v", holder)
		}
		charge := q.hostJobCharge(holder)
		offered := hostNeed(&m, holder.Spec.Needs.MemoryGB, holder.Spec.Needs.HostMemoryGB)
		if charge != offered || charge >= m.HostMemoryBudgetGB {
			t.Fatalf("never-started charge=%g offered=%g", charge, offered)
		}
		if !hostFits(q.s.Machines[m.ID], 0, 32) {
			t.Fatal("never-started allocation blocked fitting neighbor")
		}
	}
}

// #1049: negotiated sample identity must recover from the expired bootstrap anchor.
func TestHostMemoryNegotiatedIdentityRecoversAfterBootstrap(t *testing.T) {
	_, clock := fixture(t)
	key := attemptKey{"holder", 1, "session"}
	w := Worker{unifiedMemoryWire: &workerUnifiedMemoryWire{machine: "worker", session: "session", supported: true, probeSupported: true}}
	c := Checkin{Machine: Machine{ID: "worker", Session: "session"}, Attempts: []AttemptReport{{Execution: Execution{ID: key.job, Attempt: key.attempt}, Session: key.session, Started: true, CurrentHostMemory: &CurrentHostMemory{SampleID: 1, NonreclaimableGB: 2, GrowthObserved: true}}}}
	samples := currentHostMemorySamples(w.unifiedMemoryCheckin(c).Attempts, clock.Now())
	w.unifiedMemoryWire.sampleSupported = true
	// Negotiation can reveal the identity of an already expired cached sample.
	// Anchor that identity without pretending receipt proves new sampling.
	clock.Advance(20 * time.Second)
	c.Attempts[0].CurrentHostMemory.AgeSeconds = 10
	samples = updateHostMemorySamples(w.unifiedMemoryCheckin(c).Attempts, clock.Now(), samples)
	if s := samples[key]; s.id != 1 || s.period != 0 || remainingHostGrowth(3, s, clock.Now()) != 3 {
		t.Fatalf("identity transition renewed expired credit or lost anchor: %+v", s)
	}
	for _, id := range []int64{2, 3} {
		clock.Advance(6 * time.Second)
		c.Attempts[0].CurrentHostMemory.SampleID, c.Attempts[0].CurrentHostMemory.AgeSeconds = id, 0
		samples = updateHostMemorySamples(w.unifiedMemoryCheckin(c).Attempts, clock.Now(), samples)
		if s := samples[key]; s.id != id || s.period != 6*time.Second || remainingHostGrowth(3, s, clock.Now()) != 1 {
			t.Fatalf("fresh identified sample failed to recover cadence/credit: %+v", s)
		}
	}
}
