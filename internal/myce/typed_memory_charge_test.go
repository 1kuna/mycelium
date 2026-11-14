package myce

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// A Linux host whose cgroup ceilings contain each job at its typed figure,
// with a 32 GiB allowance and a worker that launches measured charges.
func typedChargeFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuMachine("worker", "worker-host", "cpu", 16, 32)
	// A real worker's known (empty) device inventory and CPU fingerprint.
	m.Devices, m.CPUExtensionFingerprint = []MachineDevice{}, "cpu-v1"
	m.HostMemoryBudgetGB, m.ExtensionFingerprint = 32, "cpu-v1"
	m.MemoryEnforcement, m.MemoryCeilingSlackGB = cgroupMemoryEnforcement, .125
	m.Capabilities = append(slices.Clone(m.Capabilities), measuredMemoryChargeCapability)
	check(t, q, m, nil)
	return q, clock, m
}

func typedSpec(name string, typed float64) Spec {
	s := cpuSpec(1, typed)
	s.Name = name
	return s
}

// A Linux cgroup worker's accepted peak: resident, non-reclaimable and its
// memory.events counters, with the sampler's measured growth.
func linuxPeak(growth float64) func(*Result) {
	return func(r *Result) {
		peak := *r.MemoryPeak.HostGB
		r.MemoryPeak.NonreclaimableGB, r.MemoryPeak.GrowthGB = &peak, &growth
		r.MemoryPeak.Events = &MemoryEventCounts{}
	}
}

func learnTyped(t *testing.T, q *Queue, clock *fakeClock, m Machine, s Spec, peaks ...float64) *Job {
	t.Helper()
	var last *Job
	for _, peak := range peaks {
		last = runMemory(t, q, clock, m, s, peak, linuxPeak(.25))
	}
	return last
}

// One check-in reporting each running attempt, with a current sample where
// use names one, and the host's live availability.
func typedCheckin(t *testing.T, q *Queue, clock *fakeClock, m Machine, available float64, jobs []*Job, use func(*Job) *CurrentHostMemory) Reply {
	t.Helper()
	clock.Advance(time.Second)
	c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
	c.Machine.HostAvailableMemoryGB = available
	for _, j := range jobs {
		a := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true}
		if use != nil {
			a.CurrentHostMemory = use(j)
		}
		c.Attempts = append(c.Attempts, a)
	}
	r, err := q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// assignNext checks in until the next queued job is assigned: a placement may
// first reserve its slot, then assign on a later check-in.
func assignNext(t *testing.T, q *Queue, clock *fakeClock, m Machine, available float64, jobs []*Job, use func(*Job) *CurrentHostMemory) *Job {
	t.Helper()
	for range 3 {
		if a := assignedJob(typedCheckin(t, q, clock, m, available, jobs, use)); a != nil {
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			return a
		}
	}
	return nil
}

func cgroupUse(clock *fakeClock, gb, rate float64) *CurrentHostMemory {
	fp := gb
	return &CurrentHostMemory{NonreclaimableGB: gb, FootprintGB: &fp, GrowthObserved: true, GrowthGB: rate, GrowthRateGB: rate, sampledAt: clock.Now()}
}

func ledgerCharges(q *Queue, m Machine) map[string]float64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := map[string]float64{}
	for _, o := range q.hostMemoryLedgers[m.ID].Owners {
		if o.Execution != nil {
			out[o.Execution.ID] = o.ChargeGB
		}
	}
	return out
}

// Two accepted runs of a typed 16 GiB suite reached 3 and 4 GiB. The next run
// is charged the 4 GiB peak plus the 1 GiB spread between them, not 16 GiB,
// and the typed figure stays the limit the worker enforces. Admission and the
// ledger both charge 5.125 GiB, so a host with 20 GiB live room runs two
// where the typed figure fitted one. With nothing learned the typed figure
// is the charge.
func TestTypedMemoryChargesLearnedPeakPlusSpread(t *testing.T) {
	q, clock, m := typedChargeFixture(t)
	s := typedSpec("suite", 16)
	first := learnTyped(t, q, clock, m, s, 3)
	if e := first.MemoryEnvelope; e.Basis != "declared" || e.HostGB != 16.125 || e.ChargeHostGB != 0 {
		t.Fatalf("a first run with nothing learned was not charged its typed figure: %+v", e)
	}
	learnTyped(t, q, clock, m, s, 4)

	submit(t, q, s)
	submit(t, q, s)
	var running []*Job
	for range 2 {
		j := assignNext(t, q, clock, m, 20-5.125*float64(len(running)), running, nil)
		if j == nil {
			t.Fatalf("assigned %d jobs on 20 GiB of room, want both", len(running))
		}
		running = append(running, j)
		e := j.MemoryEnvelope
		if e.Basis != "learned" || e.Evidence != 2 || e.ChargeHostGB != 5 || e.LimitHostGB != 16 || e.HostGB != 5.125 || !strings.Contains(e.Source, "typed 16 GiB is the limit") {
			t.Fatalf("not charged its peak plus spread: %+v", e)
		}
		if j.Spec.Needs.HostMemoryGB != 16 {
			t.Fatalf("typed limit lost: the worker would enforce %g GiB", j.Spec.Needs.HostMemoryGB)
		}
	}
	typedCheckin(t, q, clock, m, 20, running, nil)
	for id, charge := range ledgerCharges(q, m) {
		if charge != 5.125 {
			t.Fatalf("ledger charged %s %g GiB before any sample, want its learned 5.125", id, charge)
		}
	}
	// Measured use above the charge raises it, up to the typed ceiling.
	typedCheckin(t, q, clock, m, 12, running, func(*Job) *CurrentHostMemory { return cgroupUse(clock, 9, 0) })
	ceiling := hostNeed(&m, 0, 16)
	for id, charge := range ledgerCharges(q, m) {
		if charge < 9 || charge > ceiling {
			t.Fatalf("ledger charged %s %g GiB using 9 GiB, want use plus margin within %g", id, charge, ceiling)
		}
	}
}

// The typed figure stays the charge wherever the change could not be
// contained or has no evidence: each case is a protection, not a preference.
func TestTypedMemoryChargeKeepsTypedFigure(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Queue, *Machine, *Job)
	}{
		{"older worker", func(_ *Queue, m *Machine, _ *Job) {
			m.Capabilities = slices.DeleteFunc(slices.Clone(m.Capabilities), func(c string) bool { return c == measuredMemoryChargeCapability })
		}},
		{"no kernel or sampled guard", func(_ *Queue, m *Machine, _ *Job) { m.MemoryEnforcement = "admission-only" }},
		{"shared interactive host", func(_ *Queue, m *Machine, _ *Job) { m.SharedHost = &SharedHostProfile{} }},
		{"production", func(_ *Queue, _ *Machine, j *Job) { j.Spec.Priority = "production" }},
		{"allowance below the typed figure", func(_ *Queue, m *Machine, _ *Job) { m.EffectiveHostMemoryBudgetGB = 12 }},
		{"OOM retry", func(_ *Queue, _ *Machine, j *Job) { j.OOMRetries = 1 }},
		{"no accepted evidence", func(_ *Queue, _ *Machine, j *Job) { j.Spec.Name = "never-ran" }},
		{"learned memory off", func(q *Queue, _ *Machine, _ *Job) { setLearnedMemory(t, q, false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m := typedChargeFixture(t)
			s := typedSpec("suite", 16)
			s.Project = "prod" // A production tier in the default policy.
			learnTyped(t, q, clock, m, s, 3, 4)
			j := &Job{ID: "probe", State: "queued", Spec: s}
			if err := normalizeResources(&j.Spec); err != nil {
				t.Fatal(err)
			}
			q.mu.Lock()
			copy := *q.s.Machines[m.ID]
			q.mu.Unlock()
			if got := q.memoryCandidate(j, &copy); got.MemoryEnvelope.Basis != "learned" {
				t.Fatalf("baseline not charged its learned figure: %+v", got.MemoryEnvelope)
			}
			tc.edit(q, &copy, j)
			got := q.memoryCandidate(j, &copy)
			if e := got.MemoryEnvelope; e.Basis != "declared" || e.ChargeHostGB != 0 || got.Spec.Needs.HostMemoryGB != 16 {
				t.Fatalf("typed figure not the charge: %+v needs %+v", e, got.Spec.Needs)
			}
		})
	}
}

// Production keeps its protection: with the same evidence it is still
// admitted on its typed figure. A neighbour admitted on its small learned
// charge that grows into the room it was lent yields; production does not.
func TestTypedMemoryChargeProductionKeepsProtection(t *testing.T) {
	q, clock, m := typedChargeFixture(t)
	prod := typedSpec("render", 16)
	prod.Priority, prod.Project = "production", "prod"
	learnTyped(t, q, clock, m, prod, 3, 4)
	neighbour := typedSpec("suite", 16)
	learnTyped(t, q, clock, m, neighbour, 3, 4)

	submit(t, q, prod)
	p := assignNext(t, q, clock, m, 31, nil, nil)
	if p == nil || p.MemoryEnvelope.Basis != "declared" || p.MemoryEnvelope.HostGB != 16.125 {
		t.Fatalf("production not admitted on its typed figure: %+v", p)
	}
	jobs := []*Job{p}
	// Running at its accepted peak, production lends the rest of its envelope.
	submit(t, q, neighbour)
	n := assignNext(t, q, clock, m, 27, jobs, func(*Job) *CurrentHostMemory { return cgroupUse(clock, 4, 0) })
	if n == nil || n.MemoryEnvelope.Basis != "learned" || n.MemoryEnvelope.HostGB != 5.125 {
		t.Fatalf("neighbour not admitted on its learned charge: %+v", n)
	}
	jobs = append(jobs, n)
	// Both grow toward their typed limits while live room runs out.
	typedCheckin(t, q, clock, m, 4, jobs, func(*Job) *CurrentHostMemory { return cgroupUse(clock, 13, .5) })
	if got := job(t, q, p.ID); got.State != "running" {
		t.Fatalf("production stopped for a lent neighbour's growth: %s %q", got.State, got.Reason)
	}
	got := job(t, q, n.ID)
	if stop := priorityStop(got); got.State != "stopping" || stop == nil || stop.Cause != memoryOvercommitCause {
		t.Fatalf("lent neighbour did not yield: %s %+v %q", got.State, stop, got.Reason)
	}
}

// Two jobs admitted on learned charges grow toward their typed limits, which
// jointly exceed the host. The backstop stops one while the remaining room
// still holds both jobs' measured growth until the stop lands: their joint
// use never passes capacity by more than the measured headroom (drift).
func TestTypedMemoryChargeJointGrowthYieldsInTime(t *testing.T) {
	q, clock, m := typedChargeFixture(t)
	m.HostMemoryBudgetGB = 24
	check(t, q, m, nil)
	s := typedSpec("suite", 16)
	learnTyped(t, q, clock, m, s, 3, 4)
	const capacity, rate = 23.0, .5
	submit(t, q, s)
	submit(t, q, s)
	var jobs []*Job
	for range 2 {
		j := assignNext(t, q, clock, m, capacity-4*float64(len(jobs)), jobs, func(*Job) *CurrentHostMemory { return cgroupUse(clock, 4, 0) })
		if j == nil {
			t.Fatalf("assigned %d of two learned jobs on 23 GiB", len(jobs))
		}
		jobs = append(jobs, j)
	}
	horizon := q.preemptionGrace().Seconds()
	drift := exploratoryDriftMarginGB(&m)
	for used := 4.0; used < 16; used += rate {
		available := capacity - 2*used
		typedCheckin(t, q, clock, m, available, jobs, func(*Job) *CurrentHostMemory { return cgroupUse(clock, used, rate) })
		stopping := 0
		for _, j := range jobs {
			if job(t, q, j.ID).State == "stopping" {
				stopping++
			}
		}
		if stopping == 0 {
			if available <= 0 {
				t.Fatalf("no yield before both jobs used all %g GiB", capacity)
			}
			continue
		}
		// Until the stop lands both jobs keep growing at their measured rate.
		landing := 2 * min(16-used, rate*horizon)
		t.Logf("yield at %g GiB each, %g GiB available, %g GiB of growth before it lands, drift %g", used, available, landing, drift)
		if stopping != 1 || available+drift < landing {
			t.Fatalf("yield at %g GiB each (%d stopping) leaves %g GiB for %g GiB of growth before it lands", used, stopping, available, landing)
		}
		return
	}
	t.Fatal("jobs grew to their typed limits without a yield")
}

// The worker launches on the charge: with 10 GiB live it admits a job charged
// 5 GiB below its typed 16, still enforces 16, and refuses the same job
// charged its typed figure.
func TestTypedMemoryChargeWorkerLaunchesOnCharge(t *testing.T) {
	m := cpuMachine("worker", "worker-host", "cpu", 16, 32)
	m.HostMemoryBudgetGB, m.MemoryEnforcement, m.MemoryCeilingSlackGB = 32, cgroupMemoryEnforcement, .125
	w := Worker{Machine: m, Clock: RealClock{}, HostMemorySource: func(context.Context) (float64, error) { return 10, nil }}
	j := Job{ID: "j", Attempt: 1, Session: m.Session, State: "assigned", Spec: typedSpec("suite", 16)}
	if err := normalizeResources(&j.Spec); err != nil {
		t.Fatal(err)
	}
	j.MemoryEnvelope = &JobMemoryEnvelope{Basis: "learned", HostGB: 5.125, ChargeHostGB: 5, LimitHostGB: 16}
	if err := localAttemptAdmission(w, nil, j, nil); err != nil {
		t.Fatal("worker refused a learned charge that fits:", err)
	}
	if err := w.checkHostRAM(j.Spec.Needs.MemoryGB, chargedHostGB(&j)); err != nil {
		t.Fatal("launch check refused a learned charge that fits:", err)
	}
	if bytes, err := w.jobCeilingBytes(j); err != nil || bytes != int64(16.125*(1<<30)) {
		t.Fatalf("ceiling %d bytes (%v), want the typed 16.125 GiB", bytes, err)
	}
	j.MemoryEnvelope.ChargeHostGB, j.MemoryEnvelope.LimitHostGB = 0, 0
	if err := localAttemptAdmission(w, nil, j, nil); !errors.Is(err, errWorkerMemory) {
		t.Fatal("typed figure admitted on 10 GiB:", err)
	}
}

// Per-item work never repeats exactly; accepted family rows forecast their
// own peak plus measured growth (#1466, #1580), not a spread across key maxima.
func TestTypedMemoryChargeFamilyEvidence(t *testing.T) {
	q, clock, m := typedChargeFixture(t)
	item := func(n string) Spec {
		s := typedSpec("render", 20)
		s.Run.Command = []string{"render", n}
		return s
	}
	learnTyped(t, q, clock, m, item("a"), 2)
	second := learnTyped(t, q, clock, m, item("b"), 3)
	if e := second.MemoryEnvelope; e.Basis != "declared" {
		t.Fatalf("one family run replaced the typed charge: %+v", e)
	}
	learnTyped(t, q, clock, m, item("c"), 4)
	fourth := learnTyped(t, q, clock, m, item("d"), 4)
	if e := fourth.MemoryEnvelope; e.Basis != "learned" || e.Evidence != 3 || e.ChargeHostGB != 4+.25 || e.LimitHostGB != 20 || !strings.Contains(e.Source, "family") {
		t.Fatalf("per-row family forecast not the charge: %+v", e)
	}
}

// The ledger's charge of a typed job admitted at 5 GiB below its typed 16:
// the learned charge until use passes it, then use plus the launch margin,
// never past the typed ceiling (on macOS plus the sampled guard's measured
// overshoot, rate times its reaction).
func TestTypedMemoryChargeFollowsUseToLimit(t *testing.T) {
	_, clock := fixture(t)
	at := clock.Now()
	j := usedChargeRunning(16, 4)
	j.MemoryEnvelope = &JobMemoryEnvelope{Basis: "learned", HostGB: 5.125, ChargeHostGB: 5, LimitHostGB: 16}
	linux := cpuMachine("worker", "host", "cpu", 16, 64)
	linux.MemoryEnforcement, linux.MemoryCeilingSlackGB = cgroupMemoryEnforcement, .125
	darwin := cpuMachine("mac", "mac", "cpu", 8, 16)
	darwin.CapabilityProfile.OS, darwin.MemoryEnforcement, darwin.MemoryCeilingSlackGB = "darwin", darwinRSSGuardStatus, .125
	for _, tc := range []struct {
		name   string
		m      *Machine
		sample hostMemorySample
		want   func(m *Machine) float64
	}{
		{"linux before any sample", &linux, hostMemorySample{}, func(*Machine) float64 { return 5.125 }},
		{"linux below the charge", &linux, hostMemorySample{gb: 2, footprintGB: 2, peakGB: 2, at: at}, func(*Machine) float64 { return 5.125 }},
		{"linux above the charge", &linux, hostMemorySample{gb: 9, footprintGB: 9, peakGB: 9, at: at}, func(m *Machine) float64 { return 9 + exploratoryDriftMarginGB(m) }},
		{"linux at the typed ceiling", &linux, hostMemorySample{gb: 15.9, footprintGB: 15.9, peakGB: 15.9, growthGB: 2, rateGB: 4, at: at}, func(m *Machine) float64 { return hostNeed(m, 0, 16) }},
		{"darwin above the charge", &darwin, hostMemorySample{footprintGB: 9, peakGB: 9, rateGB: 1, at: at}, func(m *Machine) float64 {
			return 9 + exploratoryDriftMarginGB(m) + darwinGuardReaction.Seconds()
		}},
		{"darwin past the typed ceiling", &darwin, hostMemorySample{footprintGB: 16.2, peakGB: 16.2, rateGB: 1, at: at}, func(m *Machine) float64 {
			return hostNeed(m, 0, 16) + darwinGuardReaction.Seconds()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			charge, _ := memoryCharge(&j, tc.m, hostNeed(tc.m, 0, 16), tc.sample, at)
			if want := tc.want(tc.m); !near(charge, want) {
				t.Fatalf("charge %g GiB, want %g", charge, want)
			}
		})
	}
}

// A job that grows past its learned charge, within its typed limit, and
// succeeds is accepted evidence: the next run is charged its new peak plus
// spread, still below the typed figure.
func TestTypedMemoryChargeRelearnsFromGrowth(t *testing.T) {
	q, clock, m := typedChargeFixture(t)
	s := typedSpec("suite", 16)
	learnTyped(t, q, clock, m, s, 3, 4)
	grown := learnTyped(t, q, clock, m, s, 9)
	if e := grown.MemoryEnvelope; e.ChargeHostGB != 5 || e.LimitHostGB != 16 {
		t.Fatalf("grown run was not admitted on its learned charge: %+v", e)
	}
	if facts := job(t, q, grown.ID).MemoryAttempts; len(facts) != 1 || facts[0].LearningReason != "eligible" {
		t.Fatalf("growth past the charge was not accepted evidence: %+v", facts)
	}
	queued := submit(t, q, s)
	q.mu.Lock()
	next := q.memoryCandidate(q.s.Jobs[queued.ID], q.s.Machines[m.ID])
	q.mu.Unlock()
	if e := next.MemoryEnvelope; e.Basis != "learned" || e.ChargeHostGB != 15 || e.LimitHostGB != 16 {
		t.Fatalf("next run not charged the new 9 GiB peak plus its 6 GiB spread: %+v", e)
	}
}
