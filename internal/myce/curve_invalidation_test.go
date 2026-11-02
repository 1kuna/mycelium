package myce

import (
	"testing"
	"time"
)

// #620: a failed ceiling-bound auto attempt invalidates comparable memory on
// every host's key and keeps its own diagnostic peak there, without a sample.
// The release guard accepted only sample-backed changes, so ordinary memory
// learning over a deploy window failed it. Each rewritten key now carries the
// failing attempt's ledger provenance; a key changed without it still fails.
func TestCurvePreservationAcceptsMemoryInvalidationWithProvenance(t *testing.T) {
	q, clock, a := learnedMemoryFixture(t)
	b := cpuMachine("worker-b", "worker-b-host", "cpu", 8, 32)
	b.HostMemoryBudgetGB, b.ExtensionFingerprint = 32, "cpu-v1"
	check(t, q, b, nil)
	s := cpuSpec(1, 0)
	s.Name = "suite"
	s.AllowedMachines = []string{a.ID}
	onB := s
	onB.AllowedMachines = []string{b.ID}
	var pages []JobLedgerPage
	addPage := func(j *Job) {
		p, err := q.JobLedger(j.ID, 0, -1, 256)
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, p)
	}
	addPage(runMemory(t, q, clock, a, s, 2, nil))
	addPage(runMemory(t, q, clock, b, onB, 2, nil))
	learned := preservationSnapshot(t, q)
	if len(learned.WorkCurves) != 2 {
		t.Fatalf("want one learned key per host: %+v", learned.WorkCurves)
	}
	constrained := func(r *Result) {
		r.Success = false
		r.MemoryPeak.Constrained = true
		r.MemoryPeak.Events = &MemoryEventCounts{Max: 12}
		r.MemoryPeak.Pressure = &MemoryPressureEvidence{Reason: "host_swap_growth"}
	}
	clock.Advance(time.Minute)
	failed := runMemory(t, q, clock, a, s, 6, constrained)
	invalidated := preservationSnapshot(t, q)
	for _, p := range invalidated.WorkCurves {
		if p.MemoryPeak == nil || p.MemoryPeak.Pressure == nil || !p.MemoryRecordedAt.IsZero() || p.Samples != 1 {
			t.Fatalf("fixture did not invalidate both hosts' keys: %+v", p)
		}
	}
	if _, err := VerifyCurvePreservation(learned, invalidated, pages); err == nil {
		t.Fatal("invalidation accepted without its ledger provenance")
	}
	addPage(failed)
	receipt, err := VerifyCurvePreservation(learned, invalidated, pages)
	if err != nil || len(receipt.Changes) != 2 {
		t.Fatalf("provenanced invalidation rejected: %+v %v", receipt, err)
	}
	for _, c := range receipt.Changes {
		if len(c.Evidence) != 1 || c.Evidence[0].Job != failed.ID || c.Evidence[0].Reason != "memory_invalidated" || c.SamplesAfter != c.SamplesBefore {
			t.Fatalf("invalidation receipt: %+v", c)
		}
	}

	// The live shape: keys already invalidated, a later failure replaces the
	// diagnostic peak with no sample and no clock change; then host B learns.
	clock.Advance(time.Minute)
	again := runMemory(t, q, clock, a, s, 7, constrained)
	clock.Advance(time.Minute)
	relearned := runMemory(t, q, clock, b, onB, 3, nil)
	window := preservationSnapshot(t, q)
	addPage(again)
	addPage(relearned)
	if _, err = VerifyCurvePreservation(learned, window, pages); err != nil {
		t.Fatalf("ordinary memory learning failed the guard: %v", err)
	}
	if _, err = VerifyCurvePreservation(invalidated, window, pages); err != nil {
		t.Fatalf("re-invalidation without a clock change failed the guard: %v", err)
	}
	// Another host's key still cannot change without its own evidence.
	tampered := window
	tampered.WorkCurves = append([]WorkObservation(nil), window.WorkCurves...)
	for i, p := range tampered.WorkCurves {
		if p.Key.Host == hostID(&a) {
			peak := 1.0
			p.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
			tampered.WorkCurves[i] = p
		}
	}
	if _, err = VerifyCurvePreservation(learned, tampered, pages); err == nil {
		t.Fatal("unprovenanced peak change accepted")
	}
}
