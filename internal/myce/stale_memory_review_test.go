package myce

import (
	"testing"
	"time"
)

func TestHostMemoryForecastRespectsContainment(t *testing.T) {
	q, c := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 64)
	m.HostMemoryBudgetGB = 48
	m.CapabilityProfile.SystemMemoryBytes = 64 << 30
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	j := incidentGuardJob()
	j.ID, j.Machine, j.Session, j.Attempt, j.State = "holder", m.ID, m.Session, 1, "running"
	j.Spec.Needs.HostMemoryGB = 3
	q.s.Machines[m.ID], q.s.Jobs[j.ID] = &m, &j
	footprint := 2.0
	r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{FootprintGB: &footprint, NonreclaimableGB: 2, GrowthObserved: true, GrowthGB: 2, GrowthRateGB: 20, AgeSeconds: 3}}
	q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: currentHostMemorySamples([]AttemptReport{r}, c.Now())}
	q.retainHostMemoryCharges(m.ID, c.Now())
	fresh := q.hostJobCharge(q.s.Jobs[j.ID])
	c.Advance(30 * time.Second)
	q.hostMemorySamples = nil
	stale := q.hostJobCharge(q.s.Jobs[j.ID])
	if fresh > 48 || stale > 48 {
		t.Fatalf("forecast-only charge bypassed containment: fresh=%g persisted-without-telemetry=%g cap=48 observed=2", fresh, stale)
	}
}
func TestHostMemoryGrownReleaseCustody(t *testing.T) {
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
}

func TestHostMemoryDelayedCachedSampleRetainsExpiry(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "cached", true: "missing then cached"}[missing], func(t *testing.T) {
			_, clock := fixture(t)
			r := AttemptReport{Execution: Execution{ID: "holder", Attempt: 1}, Session: "session", Started: true, CurrentHostMemory: &CurrentHostMemory{SampleID: 1, NonreclaimableGB: 2, GrowthObserved: true}}
			key := attemptKey{"holder", 1, "session"}
			samples := currentHostMemorySamples([]AttemptReport{r}, clock.Now())
			clock.Advance(20 * time.Second)
			if missing {
				lost := r
				lost.CurrentHostMemory = nil
				samples = updateHostMemorySamples([]AttemptReport{lost}, clock.Now(), samples)
			}
			r.CurrentHostMemory.AgeSeconds = 10
			samples = updateHostMemorySamples([]AttemptReport{r}, clock.Now(), samples)
			if got := reservedGrowth(nil, 0, 3, samples[key], clock.Now()); got != 3 {
				t.Fatalf("cached sample renewed credit: %g", got)
			}
			// A genuinely new observation establishes cadence without renewing the old one.
			r.CurrentHostMemory.SampleID, r.CurrentHostMemory.AgeSeconds = 2, 0
			samples = updateHostMemorySamples([]AttemptReport{r}, clock.Now(), samples)
			if got := reservedGrowth(nil, 0, 3, samples[key], clock.Now()); got != 1 {
				t.Fatalf("new observation lost credit: %g", got)
			}
		})
	}
}

func TestHostMemoryIdentityRecoversAfterUnknownSample(t *testing.T) {
	_, clock := fixture(t)
	key := attemptKey{"holder", 1, "session"}
	c := Checkin{Machine: Machine{ID: "worker", Session: "session"}, Attempts: []AttemptReport{{Execution: Execution{ID: key.job, Attempt: key.attempt}, Session: key.session, Started: true, CurrentHostMemory: &CurrentHostMemory{SampleID: 0, NonreclaimableGB: 2, GrowthObserved: true}}}}
	samples := currentHostMemorySamples(c.Attempts, clock.Now())
	// A recovered sensor can reveal the identity of an expired cached sample.
	// Anchor that identity without pretending receipt proves new sampling.
	clock.Advance(20 * time.Second)
	c.Attempts[0].CurrentHostMemory.SampleID = 1
	c.Attempts[0].CurrentHostMemory.AgeSeconds = 10
	samples = updateHostMemorySamples(c.Attempts, clock.Now(), samples)
	if s := samples[key]; s.id != 1 || s.period != 0 || reservedGrowth(nil, 0, 3, s, clock.Now()) != 3 {
		t.Fatalf("identity transition renewed expired credit or lost anchor: %+v", s)
	}
	for _, id := range []int64{2, 3} {
		clock.Advance(6 * time.Second)
		c.Attempts[0].CurrentHostMemory.SampleID, c.Attempts[0].CurrentHostMemory.AgeSeconds = id, 0
		samples = updateHostMemorySamples(c.Attempts, clock.Now(), samples)
		if s := samples[key]; s.id != id || s.period != 6*time.Second || reservedGrowth(nil, 0, 3, s, clock.Now()) != 1 {
			t.Fatalf("fresh identified sample failed to recover cadence/credit: %+v", s)
		}
	}
}

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
