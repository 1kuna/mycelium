package myce

import (
	"fmt"
	"testing"
	"time"
)

// The batched status walk must retain removed waves and consumers, including
// a consumer's own descendants. Only exact release evidence clears custody.
func TestStatusReadFanOutRetainedCustody(t *testing.T) {
	q, clock := fixture(t)
	now := clock.Now()
	parent := &Job{ID: "parent", State: "failed", CancelledAt: &now,
		Spec:   Spec{FanOut: &FanOut{Pieces: []FanOutPiece{{Key: "piece"}}}},
		FanOut: &FanOutProgress{Children: []string{"removed-wave"}}}
	wave := &Job{ID: "removed-wave", State: "succeeded", Attempt: 1, ReleaseConfirmed: true,
		Batch: &BatchWork{Parent: parent.ID, Pieces: []FanOutPiece{{Key: "piece"}}}}
	consumer := &Job{ID: "removed-consumer", State: "failed",
		Spec:   Spec{After: &PieceDependency{Job: parent.ID, Piece: "piece"}},
		FanOut: &FanOutProgress{Children: []string{"nested-wave"}}}
	nested := &Job{ID: "nested-wave", State: "failed", Attempt: 1}
	q.s.Jobs[parent.ID], q.s.Jobs[nested.ID] = parent, nested
	q.s.Submissions = map[string]*submissionRecord{
		"wave":     {Kind: submissionBinding, JobID: wave.ID, Job: wave},
		"consumer": {Kind: submissionBinding, JobID: consumer.ID, Job: consumer},
	}
	read := func(release string) *Job {
		t.Helper()
		view, _, err := q.captureObservation("")
		if err != nil {
			t.Fatal(err)
		}
		got := view.s.Jobs[parent.ID]
		if got.Cancellation == nil || *got.Cancellation.Release != release {
			t.Fatalf("parent release: %+v", got.Cancellation)
		}
		scope := got.CancellationScope
		if scope == nil || len(scope.Members) != 3 || len(scope.Pieces) != 1 {
			t.Fatalf("retained/nested members lost: %+v", scope)
		}
		if p := scope.Pieces[0]; !p.Materialized || p.WaveID != wave.ID || p.Release != release || len(p.Dependents) != 1 || p.Dependents[0] != consumer.ID {
			t.Fatalf("retained piece custody: %+v", p)
		}
		return got
	}
	before := read("pending")
	clock.Advance(RestartGrace + ProductionLossAfter)
	read("pending") // Time alone is never release evidence.
	q.mu.Lock()
	nested.ReleaseConfirmed = true
	q.mu.Unlock()
	read("confirmed")
	if *before.Cancellation.Release != "pending" || before.CancellationScope.Pieces[0].Release != "pending" {
		t.Fatal("later release mutated an earlier observation")
	}
}

// Approximate the retained fan-out cardinality measured in #343. Report lock
// hold separately from JSON decoding; compare before/after without a flaky
// wall-time assertion on shared hosts.
func BenchmarkStatusReadFanOutHistory(b *testing.B) {
	clock := &fakeClock{now: time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)}
	q := &Queue{clock: clock, s: state{Jobs: map[string]*Job{}, Submissions: map[string]*submissionRecord{}}}
	for i := range 3000 {
		id := fmt.Sprintf("history-%04d", i)
		j := &Job{ID: id, State: "succeeded", Attempt: 1, ReleaseConfirmed: true}
		q.s.Jobs[id] = j
		q.s.Submissions[id] = &submissionRecord{Kind: submissionBinding, JobID: id, Job: j}
	}
	for i := range 400 {
		parent := q.s.Jobs[fmt.Sprintf("history-%04d", i)]
		parent.Spec.FanOut = &FanOut{Pieces: []FanOutPiece{{Key: "piece"}}}
		parent.FanOut = &FanOutProgress{Children: []string{fmt.Sprintf("history-%04d", i+400)}}
		consumer := q.s.Jobs[fmt.Sprintf("history-%04d", i+800)]
		consumer.Spec.After = &PieceDependency{Job: parent.ID, Piece: "piece"}
	}
	var hold time.Duration
	q.observeRead = func(_ string, _, duration time.Duration) { hold += duration }
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := q.captureObservation(""); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(hold.Nanoseconds())/float64(b.N), "lock-ns/op")
}
