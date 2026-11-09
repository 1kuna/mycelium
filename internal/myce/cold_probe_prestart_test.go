package myce

import (
	"context"
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
