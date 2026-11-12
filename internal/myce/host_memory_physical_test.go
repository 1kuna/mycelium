package myce

import (
	"math"
	"testing"
	"time"
)

// The incident's envelope, inventory, resident reading and inflated charge are
// known. Status did not expose its historical slope: this synthetic slope
// reproduces the same magnitude, rather than claiming those missing inputs.
func TestHostMemoryAcceleratorForecastPhysicalBound(t *testing.T) {
	const envelope = 12.963153839111328
	const resident = 7.612392425537109
	const inflated = 126.51168989884452
	q, clock := fixture(t)
	m := m3Machine()
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 24, 10.153858184814453
	m.CapabilityProfile.SystemMemoryBytes = 33507360768
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	physical := float64(m.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
	j := Job{ID: "holder", State: "running", Attempt: 1, Machine: m.ID, Session: m.Session,
		AllocatedDevices: []string{"0"}, Spec: spec("production"),
		MemoryEnvelope: &JobMemoryEnvelope{Basis: "learned", HostGB: envelope, DeviceGB: 24.142858325225912, Enforcement: m.MemoryEnforcement}}
	j.Spec.Needs.Memory, j.Spec.Needs.HostMemoryGB, j.Spec.Needs.MemoryGB = "auto", envelope, 24.142858325225912
	q.s.Jobs[j.ID], q.s.Machines[m.ID] = &j, &m
	if adaptiveMemoryCeilingGB(&j, &m) != 0 {
		t.Fatal("accelerator fixture unexpectedly used the CPU adaptive bound")
	}
	sample := hostMemorySample{id: 1, gb: resident, footprintGB: resident, peakGB: resident,
		growthObserved: true, growthGB: 2, rateGB: (inflated - resident - 2) / 3, at: clock.Now()}
	clock.Advance(3 * time.Second)
	if math.Abs(sample.hostForecast(clock.Now())-inflated) > 1e-9 {
		t.Fatal("fixture no longer reproduces the reported forecast")
	}
	q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: {jobKey(j): sample}}
	// The attempt's cgroup memory.max is its envelope, and the forecast is read
	// from that cgroup, so the envelope bounds it (#1487), well below physical RAM.
	charge, pending := memoryCharge(&j, &m, envelope, sample, clock.Now())
	if math.Abs(charge-envelope) > 1e-9 || math.Abs(pending-(envelope-resident)) > 1e-9 || charge >= physical {
		t.Errorf("forecast passed its cgroup ceiling: charge=%g pending=%g envelope=%g resident=%g", charge, pending, envelope, resident)
	}
	q.retainRelease(&j)
	ledger := q.hostMemoryLedger(&m)
	if len(ledger.Owners) != 1 || ledger.ReservedGB != charge || ledger.GrowthGB != pending {
		t.Fatal("live attempt and release receipt were counted more than once", ledger)
	}
	r := q.s.HostReleases[releaseKey(HostRelease{Job: j.ID, Machine: m.ID, Session: m.Session, Attempt: 1})]
	if math.Abs(r.HostChargeGB-envelope) > 1e-9 {
		t.Errorf("release receipt retained impossible forecast: %g", r.HostChargeGB)
	}
	// A distinct flat sample corrects the live prediction. The old release
	// receipt still fences custody, but cannot double-charge this live attempt.
	sample.id, sample.at, sample.growthGB, sample.rateGB = 2, clock.Now(), 0, 0
	q.hostMemorySamples[m.ID][jobKey(j)] = sample
	if got := q.hostMemoryLedger(&m).ReservedGB; got != envelope {
		t.Errorf("corrected observation retained forecast-only live charge: %g", got)
	}
	// A fault/requeue does not prove physical cleanup. Only the exact old
	// attempt's release can remove custody; a different session cannot do so.
	j.State = "queued"
	if q.hostMemoryLedger(&m).ReservedGB != r.HostChargeGB {
		t.Fatal("requeue lost unreleased custody")
	}
	wrong := Result{Execution: Execution{ID: j.ID, Attempt: 1}, Machine: m.ID, Session: "other-session"}
	q.settleRelease(wrong, true)
	if len(q.s.HostReleases) != 1 {
		t.Fatal("different session released the holder")
	}
	wrong.Session = m.Session
	q.settleRelease(wrong, true)
	if ledger := q.hostMemoryLedger(&m); len(ledger.Owners) != 0 || ledger.ReservedGB != 0 || ledger.GrowthGB != 0 {
		t.Fatal("confirmed cleanup left a phantom charge", ledger)
	}
}

func TestHostMemoryPhysicalBoundPreservesEvidence(t *testing.T) {
	_, clock := fixture(t)
	m := m3Machine()
	m.CapabilityProfile.SystemMemoryBytes = 32 << 30
	j := Job{ID: "holder", Attempt: 1, Session: m.Session, State: "running", Spec: spec("production")}
	sample := hostMemorySample{gb: 7, footprintGB: 8, peakGB: 40, growthGB: 100, at: clock.Now()}
	charge, pending := memoryCharge(&j, &m, 13, sample, clock.Now())
	if charge != 40 || pending != 33 {
		t.Fatal("physical forecast cap discarded actual high water", charge, pending)
	}
	// Neither a declaration nor an opaque exact-attempt floor is a forecast.
	j.HostMemoryCharge = &AttemptHostMemoryCharge{Execution: Execution{ID: j.ID, Attempt: 1}, Session: m.Session, GiB: 48}
	charge, _ = memoryCharge(&j, &m, 50, sample, clock.Now())
	if charge != 50 {
		t.Fatal("physical bound weakened a known reservation", charge)
	}
	clock.Advance(sample.freshFor())
	charge, pending = memoryCharge(&j, &m, 13, sample, clock.Now())
	if charge != 48 || pending != charge {
		t.Fatal("stale evidence granted resident credit or lost a floor", charge, pending)
	}
	j.HostMemoryCharge = nil
	sample.peakGB, sample.at = 8, clock.Now()
	m.CapabilityProfile.SystemMemoryBytes = 0
	charge, pending = memoryCharge(&j, &m, 13, sample, clock.Now())
	if charge != 108 || pending != 101 {
		t.Fatal("unknown physical inventory invented a cap", charge, pending)
	}
}
