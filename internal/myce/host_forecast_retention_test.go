package myce

import (
	"encoding/json"
	"testing"
	"time"
)

func TestHostForecastExpiredSampleCannotGrowCharge(t *testing.T) {
	for _, id := range []int64{0, 1} {
		t.Run(map[int64]string{0: "legacy", 1: "identified"}[id], func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 8, 128)
			m.HostMemoryBudgetGB = 96
			m.CapabilityProfile.SystemMemoryBytes = 128 << 30
			j := incidentGuardJob()
			j.ID, j.Machine, j.Session, j.Attempt, j.State = "holder", m.ID, m.Session, 1, "running"
			j.Spec.Needs.HostMemoryGB = 2
			q.s.Machines[m.ID], q.s.Jobs[j.ID] = &m, &j
			footprint := 1.5
			r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session, Started: true,
				CurrentHostMemory: &CurrentHostMemory{SampleID: id, FootprintGB: &footprint, NonreclaimableGB: 1, GrowthObserved: true, GrowthGB: .2, GrowthRateGB: .1}}
			samples := currentHostMemorySamples([]AttemptReport{r}, clock.Now())
			q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: samples}
			q.retainHostMemoryCharges(m.ID, clock.Now())
			before := q.hostJobCharge(q.s.Jobs[j.ID])
			clock.Advance(20 * time.Minute)
			// A legacy actor may send a newly sampled low footprint with no ID;
			// a modern actor can resend its cached observation with its true age.
			if id > 0 {
				r.CurrentHostMemory.AgeSeconds = (20 * time.Minute).Seconds()
			}
			samples = updateHostMemorySamples([]AttemptReport{r}, clock.Now(), samples)
			q.hostMemorySamples[m.ID] = samples
			q.retainHostMemoryCharges(m.ID, clock.Now())
			charge, pending := memoryCharge(q.s.Jobs[j.ID], &m, 2, samples[jobKey(j)], clock.Now())
			if charge != before || pending != charge || charge >= 3 {
				t.Fatalf("expired rate grew permanent charge: before=%g charge=%g pending=%g allowance=96 observed=1.5", before, charge, pending)
			}
		})
	}
}

func TestHostForecastCorrectionRetainsObservedPeak(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 64)
	m.HostMemoryBudgetGB = 48
	m.CapabilityProfile.SystemMemoryBytes = 64 << 30
	j := incidentGuardJob()
	j.ID, j.Machine, j.Session, j.Attempt, j.State = "holder", m.ID, m.Session, 1, "running"
	j.Spec.Needs.HostMemoryGB = 3
	q.s.Machines[m.ID], q.s.Jobs[j.ID] = &m, &j
	footprint := 6.0
	r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session, Started: true,
		CurrentHostMemory: &CurrentHostMemory{SampleID: 1, FootprintGB: &footprint, NonreclaimableGB: 5, GrowthObserved: true, GrowthGB: 2, GrowthRateGB: 20}}
	samples := currentHostMemorySamples([]AttemptReport{r}, clock.Now())
	clock.Advance(time.Second)
	q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: samples}
	q.retainHostMemoryCharges(m.ID, clock.Now())
	grown := q.hostJobCharge(q.s.Jobs[j.ID])
	if grown < 28 {
		t.Fatalf("fresh identified forecast under-reserved: %g", grown)
	}
	// Serialize the real durable Job: restart loses the sampler, not provenance.
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
	if got := q.hostJobCharge(&restored); got != grown {
		t.Fatalf("missing telemetry released accepted reservation: %g < %g", got, grown)
	}
	clock.Advance(20 * time.Second)
	footprint = 1.5
	r.CurrentHostMemory.NonreclaimableGB = 1
	r.CurrentHostMemory.GrowthGB, r.CurrentHostMemory.GrowthRateGB = 0, 0
	// The same ID delivered after restart must not be mistaken for correction.
	samples = currentHostMemorySamples([]AttemptReport{r}, clock.Now())
	q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: samples}
	q.retainHostMemoryCharges(m.ID, clock.Now())
	if got := q.hostJobCharge(q.s.Jobs[j.ID]); got != grown {
		t.Fatalf("cached identity corrected accepted forecast: %g != %g", got, grown)
	}
	r.CurrentHostMemory.SampleID = 2
	samples = currentHostMemorySamples([]AttemptReport{r}, clock.Now())
	q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: samples}
	q.retainHostMemoryCharges(m.ID, clock.Now())
	charge := q.hostJobCharge(q.s.Jobs[j.ID])
	if charge < 6 || charge >= 7 {
		t.Fatalf("correction did not replace forecast while protecting peak: grown=%g corrected=%g observed=6", grown, charge)
	}
	clock.Advance(time.Minute)
	q.hostMemorySamples = nil
	if got := q.hostJobCharge(q.s.Jobs[j.ID]); got != charge {
		t.Fatalf("lost corrected durable floor: %g != %g", got, charge)
	}
	q.refreshHostMemory()
	if !hostFits(q.s.Machines[m.ID], 0, 40) || hostFits(q.s.Machines[m.ID], 0, 43) {
		t.Fatalf("corrected charge failed neighbor admission bounds: charge=%g free=%g", charge, q.s.Machines[m.ID].FreeHostMemoryGB)
	}
	q.retainRelease(q.s.Jobs[j.ID])
	q.requeue(q.s.Jobs[j.ID], "lost", false)
	q.refreshHostMemory()
	if got := q.s.Machines[m.ID].HostMemoryInUseGB; got < charge {
		t.Fatalf("release custody lost corrected charge: %g < %g", got, charge)
	}
}

func TestHostForecastOpaqueChargeStaysConservative(t *testing.T) {
	_, clock := fixture(t)
	m := Machine{HostMemoryBudgetGB: 48, CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 64 << 30}}
	j := incidentGuardJob()
	j.ID, j.Session, j.Attempt, j.State = "holder", "session", 1, "running"
	j.HostMemoryCharge = &AttemptHostMemoryCharge{Execution: Execution{ID: j.ID, Attempt: 1}, Session: j.Session, GiB: 40}
	sample := hostMemorySample{id: 2, at: clock.Now(), gb: 1, footprintGB: 1.5, growthObserved: true}
	charge, _ := memoryCharge(&j, &m, 3, sample, clock.Now())
	if charge < 40 {
		t.Fatalf("opaque pre-upgrade charge was assumed to be forecast-only: %g", charge)
	}
}
