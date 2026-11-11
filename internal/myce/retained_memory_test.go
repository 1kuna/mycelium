package myce

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"
)

// Compare with the original full-history evaluator through the mutations that
// can invalidate retained evidence. New passes retain no admission answers.
func TestRetainedMemoryMutationEquivalence(t *testing.T) {
	q, _ := candidateCostFixture(t)
	source := storedTestJob(t, q, "retained-001")
	key := *source.Timing.Key
	configuration := memoryConfiguration(source.Spec.Needs, source.Spec.Args, source.Spec.Program)
	compare := func(stage string) memoryConstraintEvidence {
		t.Helper()
		q.memoryPlanning = newMemoryPlanningCache()
		defer func() { q.memoryPlanning = nil }()
		got := q.memoryConstraintEvidence(key, configuration, false)
		want := q.memoryConstraintEvidenceFrom(key, configuration, maps.Values(q.s.Jobs), false, time.Time{})
		if got != want {
			t.Fatalf("%s: cached %+v, original %+v", stage, got, want)
		}
		return got
	}
	transaction := func(fn func() error) {
		t.Helper()
		if err := q.update(fn); err != nil {
			t.Fatal(err)
		}
	}
	transaction(func() error {
		if compare("warm").lower == 0 {
			t.Fatal("missing fixture evidence")
		}
		return nil
	})
	original := q.s.Machines[source.Timing.Machine]
	transaction(func() error {
		original.Gone = true
		compare("owner gone")
		original.Gone = false
		original.ExtensionFingerprint = "changed"
		original.CPUExtensionFingerprint = "changed"
		compare("changed identity")
		original.ExtensionFingerprint = "synthetic-cpu"
		original.CPUExtensionFingerprint = "synthetic-cpu"
		q.unreported[original.ID] = true
		compare("unreported owner")
		delete(q.unreported, original.ID)
		return nil
	})
	transaction(func() error {
		j := q.touchJob(source.ID)
		peak := *j.MemoryAttempts[0].MemoryPeak
		large := 50.0
		peak.HostGB = &large
		j.MemoryAttempts[0].MemoryPeak = &peak
		if compare("new evidence").lower != 50 {
			t.Fatal("cold write was not reindexed")
		}
		j.MemoryAttempts[0].Attempt++
		if compare("exact attempt").lower == 50 {
			t.Fatal("stale execution lent evidence")
		}
		return nil
	})
	compare("after commit")
	prepared := q.snapshotCheckinMemoryIdentities(context.Background())
	abort := errors.New("rollback")
	if err := q.update(func() error {
		j := q.touchJob(source.ID)
		j.MemoryAttempts[0].Attempt--
		compare("speculative cold mutation")
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	prepared.installRetainedMemory(q)
	transaction(func() error { compare("rollback and stale preparation"); return nil })
	transaction(func() error {
		for id, j := range q.s.Jobs {
			if j.State == "succeeded" {
				q.deleteJob(id)
			}
		}
		if compare("collection").lower != 0 {
			t.Fatal("collected history lent evidence")
		}
		return nil
	})
}

func TestRetainedMemoryLiveEvidenceAndTime(t *testing.T) {
	q, _ := candidateCostFixture(t)
	// The fixture originally aliases stored timestamps to its fake clock field.
	// Freeze those execution times before advancing the clock independently.
	if err := q.update(func() error {
		for id, j := range q.s.Jobs {
			if j.State == "succeeded" {
				j = q.touchJob(id)
				started, finished := *j.StartedAt, *j.FinishedAt
				j.StartedAt, j.FinishedAt = &started, &finished
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	source := storedTestJob(t, q, "retained-001")
	source.ID = "live-evidence"
	source.State = "running"
	source.Timing.Execution.ID = source.ID
	key := *source.Timing.Key
	configuration := memoryConfiguration(source.Spec.Needs, source.Spec.Args, source.Spec.Program)
	if err := q.update(func() error { q.putJob(source.ID, &source); return nil }); err != nil {
		t.Fatal(err)
	}
	compare := func(after time.Time) float64 {
		t.Helper()
		q.memoryPlanning = newMemoryPlanningCache()
		defer func() { q.memoryPlanning = nil }()
		got := q.memoryConstraintEvidenceAfter(key, configuration, false, after)
		want := q.memoryConstraintEvidenceFrom(key, configuration, maps.Values(q.s.Jobs), false, after)
		if got != want {
			t.Fatalf("cached %+v, original %+v", got, want)
		}
		return got.lower
	}
	if err := q.update(func() error {
		compare(time.Time{})
		j := q.touchJob(source.ID)
		peak := *j.MemoryAttempts[0].MemoryPeak
		large := 60.0
		peak.HostGB = &large
		j.MemoryAttempts[0].MemoryPeak = &peak
		if compare(time.Time{}) != 60 {
			t.Fatal("live mutation lent stale evidence")
		}
		if compare(q.clock.Now().Add(time.Second)) != 0 {
			t.Fatal("after fence lent old evidence")
		}
		clock := q.clock.(*fakeClock)
		now := clock.now
		clock.now = now.Add(-time.Second)
		if compare(time.Time{}) != 0 {
			t.Fatal("future execution lent evidence")
		}
		clock.now = now
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
