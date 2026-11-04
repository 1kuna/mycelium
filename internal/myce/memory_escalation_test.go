package myce

import (
	"math"
	"testing"
	"time"
)

// Replay the incident's retained accepted range with a genuine payload failure.
// 2x the largest conservative peak gives 37.4 GiB, 60% more than its ordinary
// 23.375 GiB envelope, independent of how much allowance the host exposes.
func TestMemoryEscalationMeasuredIncidentRange(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	var first Job
	for _, host := range []float64{15, 17, 18.7} {
		first = startObservation(t, q, m, s)
		clock.Advance(time.Second)
		r := result(&first, true)
		resident := host / 2
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &resident, CgroupGB: &host}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	run := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&run, false)
	peak := 18.7
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	candidate := first
	candidate.State = "queued"
	for _, allowance := range []float64{64, 128} {
		target := *q.s.Machines[m.ID]
		target.EffectiveHostMemoryBudgetGB = allowance
		e := q.memoryCandidate(&candidate, &target).MemoryEnvelope
		if math.Abs(e.HostGB-37.4) > 1e-9 || e.Basis != "exploratory" || e.Evidence != 0 {
			t.Fatalf("allowance %.0f changed measured bound: %+v", allowance, e)
		}
	}
	// A larger explicit retry floor always wins; no synthetic smaller retry.
	candidate.MemoryOOMFloor = &SizeHint{HostMemoryGB: 45}
	if e := q.memoryCandidate(&candidate, q.s.Machines[m.ID]).MemoryEnvelope; e.HostGB != 45 {
		t.Fatalf("floor lost: %+v", e)
	}
	// A prior keeps its original measurement clock, so invalidation cannot renew
	// old evidence indefinitely. No accepted history means normal cold sizing.
	candidate.MemoryOOMFloor = nil
	clock.Advance(workCurveMaxAge + time.Second)
	e := q.memoryCandidate(&candidate, q.s.Machines[m.ID]).MemoryEnvelope
	if e.Source == acceptedMemoryEscalation {
		t.Fatalf("expired prior reused: %+v", e)
	}
}

func TestMemoryEscalationTransferMargin(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	peak := 2.0
	r := result(&first, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	run := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r = result(&run, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// A different CPU family on the same OS retains the existing 2.5x margin.
	for id, point := range q.s.WorkObservations {
		point.Key.Target.Arch = "other-arch"
		q.putWorkObservation(id, point)
	}
	estimate := q.memoryEstimate(&first, q.s.Machines[m.ID])
	if estimate.host == nil || !estimate.escalation || estimate.hostFactor != 2.5 {
		t.Fatalf("transfer margin lost: %+v", estimate)
	}
}
