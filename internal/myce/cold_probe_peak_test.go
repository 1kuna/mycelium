package myce

import (
	"strings"
	"testing"
)

// Runs of one family that vary widely (4.5 and 1 GiB here) give a first-run
// bound of peak plus spread (8 GiB), above the host's 8 GiB allowance, although
// every accepted run fitted it. That evidence must not make the host less
// eligible than having none: it probes its allowance, floored at the measured
// peak, and waits while less than the peak is free.
func TestColdMemoryBoundOverAllowanceProbesAtMeasuredPeak(t *testing.T) {
	q, clock, _, darwin, s := memoryExplorationFixture(t)
	s.Priority = ""
	command := func(name string) {
		s.Run.Command = []string{"sh", "-ec", "bash scripts/" + name + ".sh"}
	}
	command("wide-suite")
	memoryExplorationComplete(t, q, clock, memoryExplorationRun(t, q, darwin, s), 4.5, false, false)
	command("narrow-suite")
	memoryExplorationComplete(t, q, clock, memoryExplorationRun(t, q, darwin, s), 1, false, false)

	command("next-selection")
	s.Targets = []ExecutionTarget{machineTarget(&darwin)}
	queued := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, darwin))
	if run == nil || run.ID != queued.ID {
		t.Fatalf("host that ran the work was refused: %+v", job(t, q, queued.ID).MemoryEnvelope)
	}
	m := q.s.Machines[darwin.ID]
	e, need := run.MemoryEnvelope, run.Spec.Needs.HostMemoryGB
	if e.Basis != "exploratory" || e.Evidence != 0 || strings.Contains(e.Source, boundedFirstRun) {
		t.Fatalf("not a physical probe: %+v", e)
	}
	if e.ProbeFloorGB != hostNeed(m, 0, 4.5) || need < 4.5 || hostNeed(m, 0, need) > hostMemoryAllowanceGB(m) {
		t.Fatalf("probe need %v GiB, floor %v GiB; want the allowance, floored at the 4.5 GiB peak", need, e.ProbeFloorGB)
	}

	// With less than the peak free, the probe waits rather than shrinking.
	busy := *m
	busy.FreeHostMemoryGB = 3
	candidate := q.memoryCandidate(&Job{ID: "busy", Spec: queued.Spec, State: "queued"}, &busy)
	if hostFits(&busy, 0, candidate.Spec.Needs.HostMemoryGB) {
		t.Fatalf("busy host offered %v GiB below the measured peak", candidate.Spec.Needs.HostMemoryGB)
	}
}
