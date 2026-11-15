package myce

import (
	"testing"
	"time"
)

// A failed ceiling-bound attempt invalidates comparable memory on every host
// and records each rewritten key on the failing attempt's ledger (#620).
func TestMemoryInvalidationLedgerProvenance(t *testing.T) {
	q, clock, a := learnedMemoryFixture(t)
	b := guardedCPUMachine("worker-b", "worker-b-host", "cpu", 8, 32)
	b.HostMemoryBudgetGB, b.ExtensionFingerprint = 32, "cpu-v1"
	check(t, q, b, nil)
	s := cpuSpec(1, 0)
	s.Name = "suite"
	s.AllowedMachines = []string{a.ID}
	onB := s
	onB.AllowedMachines = []string{b.ID}
	runMemory(t, q, clock, a, s, 2, nil)
	runMemory(t, q, clock, b, onB, 2, nil)
	learned := workHistorySnapshot(t, q)
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
	invalidated := workHistorySnapshot(t, q)
	for _, p := range invalidated.WorkCurves {
		if p.MemoryPeak == nil || p.MemoryPeak.Pressure == nil || !p.MemoryRecordedAt.IsZero() || p.Samples != 1 {
			t.Fatalf("fixture did not invalidate both hosts' keys: %+v", p)
		}
	}
	page, err := q.JobLedger(failed.ID, 0, -1, 256)
	if err != nil {
		t.Fatal(err)
	}
	changes := map[ObservationKey]*WorkLearningChange{}
	for _, entry := range page.Entries {
		if entry.WorkLearning != nil && entry.Reason == "memory_invalidated" {
			changes[entry.WorkLearning.Key] = entry.WorkLearning
		}
	}
	for _, old := range learned.WorkCurves {
		next := q.s.WorkObservations[identityHash(old.Key)]
		change := changes[old.Key]
		if change == nil || change.Before != workMeasurementDigest(old) || change.After != workMeasurementDigest(next) {
			t.Fatal("memory invalidation lost exact ledger provenance", old.Key, change)
		}
	}
}

// A memory-only success updates its own count/clock and keeps sibling peaks.
// Snapshot reads and restart cannot rewrite those measurements.
func TestMemoryOnlyLearningKeepsSiblingUnchanged(t *testing.T) {
	q, clock, a := learnedMemoryFixture(t)
	b := guardedCPUMachine("worker-b", "worker-b-host", "cpu", 8, 32)
	b.HostMemoryBudgetGB, b.ExtensionFingerprint = 32, "cpu-v1"
	check(t, q, b, nil)
	s := cpuSpec(1, 0)
	s.AllowedMachines = []string{a.ID}
	onB := s
	onB.AllowedMachines = []string{b.ID}
	runMemory(t, q, clock, a, s, 2, nil)
	runMemory(t, q, clock, b, onB, 2, nil)
	before := workHistorySnapshot(t, q)
	learned := runMemory(t, q, clock, a, s, 3, nil)
	after := workHistorySnapshot(t, q)
	for _, old := range before.WorkCurves {
		for _, next := range after.WorkCurves {
			if next.Key != old.Key {
				continue
			}
			if next.Key.Host == hostID(&b) {
				if workMeasurementDigest(next) != workMeasurementDigest(old) {
					t.Fatal("another host's peak changed without evidence")
				}
			} else if next.Samples != old.Samples+1 || next.MemorySamples != old.MemorySamples+1 || !next.MemoryRecordedAt.After(old.MemoryRecordedAt) || !next.RecordedAt.Equal(old.RecordedAt) || *next.MemoryPeak.HostGB != 3 {
				t.Fatalf("memory-only update lost its count/clock: before=%+v after=%+v", old, next)
			}
		}
	}
	page, err := q.JobLedger(learned.ID, 0, -1, 256)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) == 0 {
		t.Fatal("missing learning ledger")
	}
	q = reopenQueue(t, q, clock)
	check(t, q, a, nil)
	check(t, q, b, nil)
	restored := workHistorySnapshot(t, q)
	for _, old := range after.WorkCurves {
		if workMeasurementDigest(old) != workMeasurementDigest(q.s.WorkObservations[identityHash(old.Key)]) {
			t.Fatal("read/restart changed stored peaks", restored)
		}
	}
}
