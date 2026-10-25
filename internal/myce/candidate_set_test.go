package myce

import (
	"fmt"
	"testing"
	"time"
)

func TestCandidateSetExclusionsKeepFairCompetitors(t *testing.T) {
	q, clock := fixture(t)
	q.s.Policies = map[string]ProjectPolicy{"a": {Weight: 1}, "b": {Weight: 1}, "c": {Weight: 1}}
	m := machine("worker", "fake")
	for _, project := range []string{"a", "b", "c"} {
		for i := range 3 {
			spec := projectSpec(project)
			spec.Needs.CPUCores = 1 + i%2
			j := submit(t, q, spec)
			q.s.FairUsage[q.fairKey(j.Spec)] = FairUsage{Seconds: map[string]float64{"a": 100, "b": 10, "c": 30}[project], At: clock.Now()}
		}
	}
	set := q.candidates(&m)
	excluded := map[string]bool{}
	for range 9 {
		got := set.pick(excluded)
		// Existing standalone comparator includes excluded-but-runnable projects.
		var want *Job
		for _, original := range q.s.Jobs {
			j := q.batchCandidateWithResources(original, &m, nil)
			if j != nil && !excluded[j.ID] && (want == nil || q.before(j, want, &m)) {
				want = j
			}
		}
		if got == nil || want == nil || got.ID != want.ID {
			t.Fatalf("cached pass changed fairness: got %v, want %v", got, want)
		}
		excluded[got.ID] = true
	}
}

// Reproduce the occupied-worker backlog from the seven-day simulator: queued
// work fits after a command completes, but cannot preempt its own priority tier.
// Report scaling without a machine-dependent wall-time assertion.
func BenchmarkCandidateBacklog(b *testing.B) {
	for _, count := range []int{100, 500} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			clock := &fakeClock{now: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
			m := machine("worker", "fake")
			m.FreeMemoryGB, m.TotalMemoryGB = 8, 16
			q := &Queue{simulation: true, clock: clock, root: "/simulated-checkpoints", nasAvailable: true, unreported: map[string]bool{}, s: state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{m.ID: &m}}}
			q.s.Jobs["running"] = &Job{ID: "running", State: "running", Machine: m.ID, Session: m.Session, Spec: projectSpec("a")}
			for i := range count {
				id := fmt.Sprintf("queued-%06d", i)
				q.s.Jobs[id] = &Job{ID: id, State: "queued", Queued: clock.Now(), Spec: projectSpec(string(rune('a' + i%3)))}
			}
			q.refreshHostCPU()
			b.ResetTimer()
			for range b.N {
				q.maintainModels()
			}
		})
	}
}
