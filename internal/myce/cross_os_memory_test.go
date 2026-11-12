package myce

import (
	"strings"
	"testing"
)

// #1486, 2026-11-12: pieces measured at about 3 MB on macOS were refused on
// two idle Linux hosts ("no qualified target can fit this job"): the only way
// a peak from another OS could size a run was one bounded probe of the
// largest host's whole allowance. A foreign peak is a floor, not a bound, so
// the Linux run is an ordinary first run there, floored at that peak, and the
// next Linux run is sized from Linux's own measurement.
func TestForeignPeakFloorsFirstRunOnAnotherOS(t *testing.T) {
	q, clock, linux, darwin, s := memoryExplorationFixture(t)
	s.Priority, s.Name = "", "pieces"
	const macPeak = 3.0 / 1024 // About 3 MB.
	memoryExplorationComplete(t, q, clock, memoryExplorationRun(t, q, darwin, s), macPeak, false, false)

	s.Targets = []ExecutionTarget{machineTarget(&linux)}
	queued := submit(t, q, s)
	run := check(t, q, linux, nil).Job
	if run == nil || run.ID != queued.ID {
		t.Fatalf("Linux refused work measured on macOS: %q", job(t, q, queued.ID).Reason)
	}
	e := run.MemoryEnvelope
	if e.Refusal != "" || e.Basis != "exploratory" || !strings.Contains(e.Source, "transfer error unavailable") || e.ProbeFloorGB < macPeak || len(q.s.MemoryExplorations) != 0 {
		t.Fatalf("not an ordinary first run floored at the macOS peak: %+v claims=%d", e, len(q.s.MemoryExplorations))
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	memoryExplorationComplete(t, q, clock, run, .25, false, false)

	// Linux now has its own accepted peak.
	next := memoryCandidateForMachine(t, q, submit(t, q, s), linux.ID)
	if e := next.MemoryEnvelope; e.Basis != "learned" || e.Refusal != "" || next.Spec.Needs.HostMemoryGB > 1 {
		t.Fatalf("next Linux run not sized from Linux's measurement: %+v needs %g", e, next.Spec.Needs.HostMemoryGB)
	}
}

// A foreign peak or hint larger than the target's allowance still refuses
// there: the floor is a requirement, and only the bounded exception may probe
// below it.
func TestForeignPeakAboveAllowanceStillRefuses(t *testing.T) {
	q, clock, linux, darwin, s := memoryExplorationFixture(t)
	s.Priority, s.Name = "", "large"
	memoryExplorationComplete(t, q, clock, memoryExplorationRun(t, q, linux, s), 12, false, false)
	s.Targets = []ExecutionTarget{machineTarget(&darwin)}
	j := &Job{ID: "probe", State: "queued", Spec: s}
	if err := normalizeResources(&j.Spec); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	got := q.memoryCandidate(j, q.s.Machines[darwin.ID])
	q.mu.Unlock()
	if got.MemoryEnvelope.Refusal != memoryCapacityRemedies {
		t.Fatalf("12 GiB foreign peak on an 8 GiB allowance was not refused: %+v", got.MemoryEnvelope)
	}
}
