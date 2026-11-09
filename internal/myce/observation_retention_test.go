package myce

import (
	"testing"
	"time"
)

func TestObservationRetentionDropsOnlyUnreadRows(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	named := func(name string) Spec {
		s := cpuSpec(1, 0)
		s.Name = name
		return s
	}
	constrained := func(r *Result) {
		r.Success = false
		r.MemoryPeak.Constrained = true
		r.MemoryPeak.Events = &MemoryEventCounts{Max: 1}
	}
	runMemory(t, q, clock, m, named("old"), 2, nil)
	// A constrained failure after an accepted run leaves provenance on its row.
	runMemory(t, q, clock, m, named("limited"), 2, nil)
	runMemory(t, q, clock, m, named("limited"), 6, constrained)
	forbidden := WorkObservation{Key: ObservationKey{Work: "synthetic-forbidden"}, Forbidden: true, RecordedAt: clock.Now()}
	if err := q.update(func() error {
		q.s.WorkObservations[identityHash(forbidden.Key)] = forbidden
		q.s.StorageObservations = map[string]StorageObservation{"stale": {RecordedAt: clock.Now()}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(workCurveMaxAge + time.Hour)
	check(t, q, m, nil)
	runMemory(t, q, clock, m, named("fresh"), 2, nil)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	check(t, q, m, nil)
	if _, ok := q.s.WorkObservations[identityHash(forbidden.Key)]; !ok {
		t.Fatal("forbidden point dropped; sweeps would retry it")
	}
	// Retained constrained provenance still raises the bound at any age.
	if j := runMemory(t, q, clock, m, named("limited"), 3, nil); j.Spec.Needs.HostMemoryGB < 6 || j.MemoryEnvelope.Source != acceptedMemoryEscalation {
		t.Fatalf("constrained peak lost: %+v", j.MemoryEnvelope)
	}
	if j := runMemory(t, q, clock, m, named("fresh"), 2, nil); j.MemoryEnvelope.Basis != "learned" || j.Spec.Needs.HostMemoryGB != 2+minimumHostMemoryGB {
		t.Fatalf("fresh learned sizing changed: %+v", j.MemoryEnvelope)
	}
	if j := runMemory(t, q, clock, m, named("old"), 2, nil); j.MemoryEnvelope.Basis != "learned" {
		t.Fatalf("age alone expired a learned bound: %+v", j.MemoryEnvelope)
	}
}
