package myce

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// A cold probe's residual admission envelope is not its adaptive execution
// ceiling. Whole-host promotion beside existing custody can hold startup before
// the command has any opportunity to report memory (#1116).
func TestColdProbePrestartKeepsOfferedCharge(t *testing.T) {
	q, clock, m, spec := coldProbeAdmissionFixture(t, true)
	m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	machineCheck(t, q, m)
	submit(t, q, cpuSpec(1, 6))
	sibling := assignedJob(machineCheck(t, q, m))
	if sibling == nil {
		t.Fatal("declared sibling not assigned")
	}
	if err := q.Started(result(sibling, false)); err != nil {
		t.Fatal(err)
	}
	pending := submit(t, q, spec)
	probe := assignedJob(machineCheck(t, q, m, sibling))
	if probe == nil || probe.ID != pending.ID || probe.MemoryEnvelope.ProbeFloorGB <= 0 {
		t.Fatal("cold probe not assigned")
	}
	offered := hostNeed(&m, 0, probe.Spec.Needs.HostMemoryGB)
	if cap := adaptiveMemoryCeilingGB(probe, &m); cap <= offered {
		t.Fatalf("fixture did not separate admission and containment: offered=%g cap=%g", offered, cap)
	}
	w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return m.HostAvailableMemoryGB, nil }}
	bytes, err := w.jobCeilingBytes(*probe)
	if err != nil || bytes != 24<<30 {
		t.Fatalf("adaptive execution containment changed: %d %v", bytes, err)
	}
	reports := []AttemptReport{
		{Execution: Execution{ID: sibling.ID, Attempt: sibling.Attempt}, Session: sibling.Session, Started: true},
		{Execution: Execution{ID: probe.ID, Attempt: probe.Attempt}, Session: probe.Session},
	}
	// The original offer consumes almost all residual live headroom. A later
	// host observation, not a smaller offer, supplies room for the neighbor.
	m.HostAvailableMemoryGB = 24
	w.Machine = m
	for _, elapsed := range []time.Duration{0, 30 * time.Minute} {
		clock.Advance(elapsed)
		reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports})
		if err != nil {
			t.Fatal(err)
		}
		holder := q.s.Jobs[probe.ID]
		charge, growth := jobHostMemoryCharge(holder, &m, offered, hostMemorySample{}, clock.Now())
		if charge != offered || growth != offered || reply.AdmissionBlocked || holder.StartedAt != nil {
			t.Fatalf("unstarted probe promoted to containment: charge=%g growth=%g offered=%g blocked=%t", charge, growth, offered, reply.AdmissionBlocked)
		}
		// Both worker admission paths must protect the full offered envelope,
		// admitting a fitting neighbor but refusing static overcommit.
		next := Job{ID: "neighbor", Spec: cpuSpec(1, 2)}
		next.Spec.Needs.MemoryGB, next.Spec.Needs.HostMemoryGB = 0, 2
		if !hostFits(q.s.Machines[m.ID], 0, 2) {
			t.Fatal("offered probe blocked fitting neighbor")
		}
		if err := localAttemptAdmission(w, []Job{*sibling, *holder}, next, nil); err != nil {
			t.Fatal("worker disagrees with offered reservation", err)
		}
		rt := modelRuntime{clock: clock, memoryMachine: m, batchJobs: []Job{*sibling, *holder, next}}
		if err := rt.checkHostAdmissionLocked(m.HostAvailableMemoryGB, true, nil); err != nil {
			t.Fatal("final launch disagrees with offered reservation", err)
		}
		next.Spec.Needs.HostMemoryGB = 24
		if hostFits(q.s.Machines[m.ID], 0, 24) || localAttemptAdmission(w, []Job{*sibling, *holder}, next, nil) == nil {
			t.Fatal("offered reservation was undercharged")
		}
		rt.batchJobs[2] = next
		if err := rt.checkHostAdmissionLocked(32, true, nil); err == nil {
			t.Fatal("final launch admitted static overcommit")
		}
	}
	// Loss cannot give a replacement the unacknowledged attempt's RAM.
	holder := q.s.Jobs[probe.ID]
	q.retainRelease(holder)
	q.requeue(holder, "lost", false)
	q.refreshHostMemory()
	if got := q.s.Machines[m.ID].HostMemoryInUseGB; got < offered+6 || got >= 24 {
		t.Fatalf("release custody lost or inflated the offered reservation: %g", got)
	}
}

// The execution guard can let a cold job grow beyond its residual offer after
// siblings release. Its measured floor must then survive missing/stale samples
// and restart, even though fresh complete evidence may release unused offer RAM.
func TestColdProbeObservedGrowthSurvivesMissingTelemetry(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 64)
	m.HostMemoryBudgetGB = 24
	m.CapabilityProfile.SystemMemoryBytes = 64 << 30
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	j := incidentGuardJob()
	j.ID, j.Machine, j.Session, j.Attempt, j.State = "holder", m.ID, m.Session, 1, "running"
	j.Spec.Needs.HostMemoryGB = 8
	j.MemoryEnvelope.Basis, j.MemoryEnvelope.ProbeFloorGB = "exploratory", 1
	q.s.Machines[m.ID], q.s.Jobs[j.ID] = &m, &j
	footprint := 16.0
	r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session, Started: true,
		CurrentHostMemory: &CurrentHostMemory{SampleID: 1, FootprintGB: &footprint, NonreclaimableGB: 16, GrowthObserved: true, GrowthGB: 2, GrowthRateGB: 2}}
	samples := currentHostMemorySamples([]AttemptReport{r}, clock.Now())
	q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: samples}
	q.retainHostMemoryCharges(m.ID, clock.Now())
	if charge := q.hostJobCharge(q.s.Jobs[j.ID]); charge < 18 {
		t.Fatal("fresh cold forecast was clipped", charge)
	}
	clock.Advance(time.Second)
	footprint = 2
	r.CurrentHostMemory.SampleID = 2
	r.CurrentHostMemory.NonreclaimableGB = 2
	r.CurrentHostMemory.GrowthGB, r.CurrentHostMemory.GrowthRateGB = 0, 0
	samples = updateHostMemorySamples([]AttemptReport{r}, clock.Now(), samples)
	q.hostMemorySamples[m.ID] = samples
	q.retainHostMemoryCharges(m.ID, clock.Now())
	if charge := q.hostJobCharge(q.s.Jobs[j.ID]); charge < 16 || charge >= 17 {
		t.Fatalf("correction lost observed peak or pinned forecast: charge=%g observed=16", charge)
	}
	clock.Advance(time.Minute)
	assertReserved := func(label string) {
		t.Helper()
		holder := q.s.Jobs[j.ID]
		s := q.hostMemorySamples[m.ID]
		charge, pending := jobHostMemoryCharge(holder, &m, 8, s[jobKey(j)], clock.Now())
		if charge < 16 || pending != charge {
			t.Fatalf("%s lost cold measured floor or granted stale credit: charge=%g pending=%g observed=16", label, charge, pending)
		}
		q.refreshHostMemory()
		next := Job{ID: "neighbor", Spec: cpuSpec(1, 0)}
		next.Spec.Needs.HostMemoryGB = 12
		w := Worker{Machine: m, Clock: clock, hostMemorySamples: s, HostMemorySource: func(context.Context) (float64, error) { return 40, nil }}
		rt := modelRuntime{clock: clock, memoryMachine: m, batchJobs: []Job{*holder, next}}
		if hostFits(q.s.Machines[m.ID], 0, 12) || localAttemptAdmission(w, []Job{*holder}, next, nil) == nil || rt.checkHostAdmissionLocked(40, true, s) == nil {
			t.Fatalf("%s admitted neighbor beyond measured allowance", label)
		}
	}
	assertReserved("expired")
	r.CurrentHostMemory = nil
	samples = updateHostMemorySamples([]AttemptReport{r}, clock.Now(), samples)
	q.hostMemorySamples[m.ID] = samples
	q.retainHostMemoryCharges(m.ID, clock.Now())
	assertReserved("missing")
	// A real durable round trip drops all in-memory sample credit.
	data, err := json.Marshal(q.s.Jobs[j.ID])
	if err != nil {
		t.Fatal(err)
	}
	var restored Job
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	q.s.Jobs[j.ID] = &restored
	q.hostMemorySamples = nil
	assertReserved("restart")
	q.retainRelease(q.s.Jobs[j.ID])
	q.requeue(q.s.Jobs[j.ID], "lost", false)
	q.refreshHostMemory()
	if q.s.Machines[m.ID].HostMemoryInUseGB < 16 {
		t.Fatal("lost measured peak in release custody")
	}
}
