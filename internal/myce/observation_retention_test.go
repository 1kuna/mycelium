package myce

import (
	"strings"
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
	rows := len(q.s.WorkObservations)

	clock.Advance(workCurveMaxAge + time.Hour)
	check(t, q, m, nil)
	runMemory(t, q, clock, m, named("fresh"), 2, nil)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	check(t, q, m, nil)
	if got := len(q.s.WorkObservations); got != rows {
		t.Fatalf("want old row gone and the fresh one added: %d rows, had %d", got, rows)
	}
	if len(q.s.StorageObservations) != 0 {
		t.Fatal("expired storage observation kept")
	}
	if _, ok := q.s.WorkObservations[identityHash(forbidden.Key)]; !ok {
		t.Fatal("forbidden point dropped; sweeps would retry it")
	}
	// Retained constrained provenance still raises the bound at any age.
	if j := runMemory(t, q, clock, m, named("limited"), 3, nil); j.Spec.Needs.HostMemoryGB != 12 {
		t.Fatalf("constrained peak lost: %+v", j.MemoryEnvelope)
	}
	if j := runMemory(t, q, clock, m, named("fresh"), 2, nil); j.MemoryEnvelope.Basis != "learned" || j.Spec.Needs.HostMemoryGB != 2.5 {
		t.Fatalf("fresh learned sizing changed: %+v", j.MemoryEnvelope)
	}
	if j := runMemory(t, q, clock, m, named("old"), 2, nil); !strings.HasSuffix(j.MemoryEnvelope.Source, "(bounded: 25% of host allowance)") {
		t.Fatalf("expired evidence still sized a run: %+v", j.MemoryEnvelope)
	}
}
