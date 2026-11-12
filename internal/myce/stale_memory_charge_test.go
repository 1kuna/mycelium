package myce

import (
	"context"
	"testing"
	"time"
)

func TestStaleLearnedHostMemoryCharge(t *testing.T) {
	for _, mode := range []string{"fresh", "stale", "grown", "missing before start", "fresh before start"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 8, 64)
			m.HostMemoryBudgetGB = 48
			m.CapabilityProfile.SystemMemoryBytes = 64 << 30
			m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
			m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
			j := incidentGuardJob()
			j.ID, j.Machine, j.Session, j.Attempt, j.State = "holder", m.ID, m.Session, 1, "running"
			j.Spec.Needs.HostMemoryGB = 3
			q.s.Machines[m.ID], q.s.Jobs[j.ID] = &m, &j
			footprint := 2.0
			if mode == "grown" {
				footprint = 9
			}
			report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{FootprintGB: &footprint, NonreclaimableGB: footprint, GrowthObserved: true}}
			if mode == "fresh before start" {
				report.Started = false
			}
			if mode == "missing before start" {
				report.Started, report.CurrentHostMemory = false, nil
			}
			samples := currentHostMemorySamples([]AttemptReport{report}, clock.Now())
			q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: samples}
			q.retainHostMemoryCharges(m.ID, clock.Now())
			fresh, _ := memoryCharge(&j, &m, 3, samples[jobKey(j)], clock.Now())
			if mode != "fresh" {
				clock.Advance(30 * time.Second)
			}
			if mode == "grown" {
				// A new flat, smaller report cannot release an earlier grown reservation.
				footprint = 1
				samples = updateHostMemorySamples([]AttemptReport{report}, clock.Now(), samples)
				q.hostMemorySamples[m.ID] = samples
				q.retainHostMemoryCharges(m.ID, clock.Now())
			}
			charge, pending := memoryCharge(&j, &m, 3, samples[jobKey(j)], clock.Now())
			if charge < fresh || charge >= m.HostMemoryBudgetGB {
				t.Fatalf("charge=%g fresh=%g pending=%g", charge, fresh, pending)
			}
			if mode == "grown" && charge < 9 {
				t.Fatalf("released peak: %g", charge)
			}
			if mode == "fresh" && (charge != 3 || pending != 1) {
				t.Fatalf("fresh changed: %g/%g", charge, pending)
			}
			if mode == "stale" && pending != charge {
				t.Fatalf("stale resident credit: %g/%g", charge, pending)
			}
			q.refreshHostMemory()
			next := Job{Spec: cpuSpec(1, 32)}
			next.Spec.Needs.HostMemoryGB, next.Spec.Needs.MemoryGB = 32, 0
			if q.s.Machines[m.ID].FreeHostMemoryGB < 32 {
				t.Fatal("bounded charge blocked fitting neighbor")
			}
			w := Worker{Machine: m, Clock: clock, hostMemorySamples: samples, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
			if err := localAttemptAdmission(w, []Job{j}, next, nil); err != nil {
				t.Fatal(err)
			}
			rt := &modelRuntime{clock: clock, memoryMachine: m, batchJobs: []Job{j, next}}
			if err := rt.checkHostAdmissionLocked(64, true, samples); err != nil {
				t.Fatal("final launch disagrees with queue/worker:", err)
			}
			// The same charge must refuse an overcommitted neighbor.
			next.Spec.Needs.HostMemoryGB = 48
			if err := localAttemptAdmission(w, []Job{j}, next, nil); err == nil {
				t.Fatal("under-reserved neighbor")
			}
			rt.batchJobs = []Job{j, next}
			if err := rt.checkHostAdmissionLocked(64, true, samples); err == nil {
				t.Fatal("final launch under-reserved neighbor")
			}
			// Queue restart loses resident credit, but retains exact-attempt high water.
			restored := cloneJob(&j)
			restoredCharge, _ := memoryCharge(&restored, &m, 3, hostMemorySample{}, clock.Now())
			if restoredCharge < fresh {
				t.Fatalf("restart released growth: %g < %g", restoredCharge, fresh)
			}
			restored.Attempt++
			if retainedHostCharge(&restored) != 0 {
				t.Fatal("retry inherited old charge")
			}
		})
	}
}

func TestGrownHostMemoryChargeSurvivesMissingTelemetry(t *testing.T) {
	_, clock := fixture(t)
	m := Machine{HostMemoryBudgetGB: 48, CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 64 << 30}}
	j := incidentGuardJob()
	j.ID, j.Attempt, j.Session, j.State = "holder", 1, "session", "running"
	footprint := 9.0
	r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{FootprintGB: &footprint, NonreclaimableGB: 8, GrowthObserved: true, GrowthGB: .5, GrowthRateGB: .25}}
	key := jobKey(j)
	samples := currentHostMemorySamples([]AttemptReport{r}, clock.Now())
	clock.Advance(3 * time.Second)
	grown, _ := memoryCharge(&j, &m, 4, samples[key], clock.Now())
	r.CurrentHostMemory = nil
	samples = updateHostMemorySamples([]AttemptReport{r}, clock.Now(), samples)
	charge, pending := memoryCharge(&j, &m, 4, samples[key], clock.Now())
	if charge < grown || pending != charge {
		t.Fatalf("lost grown charge or granted resident credit: %g/%g, previous %g", charge, pending, grown)
	}
	j.HostMemoryCharge = &AttemptHostMemoryCharge{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session, GiB: charge}
	// Losing guard inventory must not release the accepted charge.
	m.HostMemoryBudgetGB, m.CapabilityProfile = 0, nil
	charge, _ = memoryCharge(&j, &m, 4, hostMemorySample{}, clock.Now())
	if charge < grown {
		t.Fatalf("inventory loss released charge: %g", charge)
	}
}

func TestHostMemoryChargeCheckinPersistsAcrossRestart(t *testing.T) {
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
}

// reservedGrowth is the growth memoryCharge leaves a declared attempt
// reserving gb, device GiB of it on a unified host: the resident credit a
// sample earns is the only difference.
func reservedGrowth(m *Machine, device, gb float64, s hostMemorySample, now time.Time) float64 {
	j := Job{State: "running", Spec: cpuSpec(1, gb)}
	if device > 0 {
		j.Spec.Needs.MemoryGB, j.Spec.Needs.Platform = device, Platform{Any: true}
	}
	if m == nil {
		m = &Machine{}
	}
	_, growth := memoryCharge(&j, m, gb, s, now)
	return growth
}
