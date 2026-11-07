package myce

import (
	"testing"
	"time"
)

func programMemoryFixture(t *testing.T) (*Queue, *fakeClock, Extension, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("synthetic", "separate", 256, 256, e)
	check(t, q, m, nil)
	return q, clock, e, m, placementSpec(true)
}

func startProgramMemory(t *testing.T, q *Queue, m Machine, s Spec) *Job {
	t.Helper()
	queued := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != queued.ID {
		t.Fatal("program not assigned", run, job(t, q, queued.ID).Reason)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	return run
}

func finishProgramMemory(t *testing.T, q *Queue, clock *fakeClock, run *Job, usage SizeHint, growth *float64) {
	t.Helper()
	clock.Advance(time.Second)
	r := result(run, true)
	r.Usage = &usage
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &usage.HostMemoryGB, DeviceGB: &usage.MemoryGB, GrowthGB: growth, DeviceGrowthGB: growth, Events: &MemoryEventCounts{}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
}

func TestProgramMemoryCorrectedForecastSurvivesRestart(t *testing.T) {
	q, clock, _, m, s := programMemoryFixture(t)
	usage := SizeHint{MemoryGB: 2, HostMemoryGB: 4}
	inflated, corrected := 100.0, 0.25
	finishProgramMemory(t, q, clock, startProgramMemory(t, q, m, s), usage, &inflated)
	run := startProgramMemory(t, q, m, s)
	waiting := submit(t, q, s) // Already carries the inflated learned charge.
	finishProgramMemory(t, q, clock, run, usage, &corrected)
	q = memoryFactsRestart(t, q, clock)
	next := submit(t, q, s)
	got := next.Spec.Needs
	t.Logf("accepted usage=2/4 GiB, forecasts=100 then 0.25 GiB; after SQLite reopen learned=%.2f/%.2f GiB", got.MemoryGB, got.HostMemoryGB)
	if got.MemoryGB != 2.25 || got.HostMemoryGB != 4.25 {
		t.Fatalf("corrected forecast remained a permanent demand floor: %+v", got)
	}
	if got := job(t, q, waiting.ID).Spec.Needs; got.MemoryGB != 2.25 || got.HostMemoryGB != 4.25 {
		t.Fatal("already queued program retained old uncertainty", got)
	}
	// On this 256 GiB host, the corrected program and a 200 GiB CPU neighbor
	// fit; the historical 104 GiB charge would refuse that neighbor.
	run = check(t, q, m, nil).Job
	if run == nil || run.ID != waiting.ID {
		t.Fatal("corrected program not admitted", run, job(t, q, waiting.ID).Reason)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	cpu := cpuMachine("cpu-neighbor", m.ID, "cpu", 8, 256)
	cpu.CapabilityProfile.SystemMemoryBytes = 256 << 30
	check(t, q, cpu, nil)
	neighbor := submit(t, q, cpuSpec(1, 200))
	if got := check(t, q, cpu, nil).Job; got == nil || got.ID != neighbor.ID {
		t.Fatal("fitting neighbor not admitted beside corrected program", got, job(t, q, neighbor.ID).Reason)
	}
}

func TestProgramMemoryForecastValidityAndFloors(t *testing.T) {
	q, clock, _, m, s := programMemoryFixture(t)
	usage := SizeHint{MemoryGB: 2, HostMemoryGB: 4}
	inflated, growing, zero := 100.0, 3.0, 0.0
	finishProgramMemory(t, q, clock, startProgramMemory(t, q, m, s), usage, &inflated)
	finishProgramMemory(t, q, clock, startProgramMemory(t, q, m, s), usage, &growing)
	// Continuing positive growth is still added to demand on both pools.
	next := startProgramMemory(t, q, m, s)
	if next.Spec.Needs.MemoryGB != 5 || next.Spec.Needs.HostMemoryGB != 7 {
		t.Fatal("positive growth lost", next.Spec.Needs)
	}
	finishProgramMemory(t, q, clock, next, usage, &growing)
	clock.Advance(workCurveMaxAge + time.Second)
	check(t, q, m, nil)
	retained := startProgramMemory(t, q, m, s)
	if retained.Spec.Needs.MemoryGB != 5 || retained.Spec.Needs.HostMemoryGB != 7 {
		t.Fatal("matching forecast expired during idle time", retained.Spec.Needs)
	}
	finishProgramMemory(t, q, clock, retained, usage, &growing)
	// Elapsed age alone is not staleness (#875/#998). Evidence from an
	// obsolete estimator revision cannot license a reduced charge.
	for key, evidence := range q.s.ProgramMemory {
		for hardware, forecast := range evidence.Forecasts {
			forecast.Revision = programForecastRevision - 1
			evidence.Forecasts[hardware] = forecast
		}
		q.s.ProgramMemory[key] = evidence
	}
	stale := startProgramMemory(t, q, m, s)
	if stale.Spec.Needs.MemoryGB != 102 || stale.Spec.Needs.HostMemoryGB != 104 {
		t.Fatal("stale evidence spent credit", stale.Spec.Needs)
	}
	// An accepted old-worker result with unknown growth cannot renew credit.
	finishProgramMemory(t, q, clock, stale, usage, nil)
	unknown := startProgramMemory(t, q, m, s)
	if unknown.Spec.Needs.MemoryGB != 102 || unknown.Spec.Needs.HostMemoryGB != 104 {
		t.Fatal("unknown growth spent credit", unknown.Spec.Needs)
	}
	// Renew valid evidence, then correct uncertainty without discarding a
	// genuinely higher accepted demand.
	finishProgramMemory(t, q, clock, unknown, SizeHint{MemoryGB: 8, HostMemoryGB: 10}, &growing)
	finishProgramMemory(t, q, clock, startProgramMemory(t, q, m, s), usage, &zero)
	lower := startProgramMemory(t, q, m, s)
	if lower.Spec.Needs.MemoryGB != 8 || lower.Spec.Needs.HostMemoryGB != 10 {
		t.Fatal("accepted demand shrank with uncertainty", lower.Spec.Needs)
	}
	finishProgramMemory(t, q, clock, lower, usage, &zero)
	authored := s
	authored.SizeHint = SizeHint{MemoryGB: 9, HostMemoryGB: 11}
	floored := startProgramMemory(t, q, m, authored)
	if floored.Spec.Needs.MemoryGB != 9 || floored.Spec.Needs.HostMemoryGB != 11 {
		t.Fatal("authored floor lost", floored.Spec.Needs)
	}
	finishProgramMemory(t, q, clock, floored, usage, &zero)
	retry := submit(t, q, s)
	retry.MemoryOOMFloor = &SizeHint{MemoryGB: 12, HostMemoryGB: 14}
	q.applyLearnedNeeds(retry)
	if retry.Spec.Needs.MemoryGB != 12 || retry.Spec.Needs.HostMemoryGB != 14 {
		t.Fatal("OOM floor lost", retry.Spec.Needs)
	}
	// A changed hardware identity cannot borrow the corrected margin.
	profile := *m.CapabilityProfile
	profile.SystemMemoryBytes = 128 << 30
	m.CapabilityProfile = &profile
	check(t, q, m, nil)
	changed := submit(t, q, s)
	if changed.Spec.Needs.MemoryGB != 102 || changed.Spec.Needs.HostMemoryGB != 104 {
		t.Fatal("unknown hardware spent credit", changed.Spec.Needs)
	}
}

func TestProgramMemoryQualificationKeepsLearnedDemand(t *testing.T) {
	q, clock, e, m, s := programMemoryFixture(t)
	finishProgramMemory(t, q, clock, startProgramMemory(t, q, m, s), SizeHint{MemoryGB: 40, HostMemoryGB: 4}, nil)
	small := placementMachine("small", "separate", 64, 24, e)
	check(t, q, small, nil)
	s.RequireTarget, s.AllowedMachines = true, []string{small.ID}
	if _, err := q.Submit(s); err == nil {
		t.Fatal("qualified a 40 GiB learned program on a 24 GiB accelerator")
	}
}
