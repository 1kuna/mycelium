package myce

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// Measure the status projection's read-lock hold for a multi-project backlog.
// No wall-time assertion: compare the same workload before/after the fix.
func BenchmarkQueueMutexProjection(b *testing.B) {
	for _, count := range []int{100, 300} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			clock := &fakeClock{now: time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)}
			m := machine("worker", "fake")
			m.TotalMemoryGB = 8
			q := &Queue{simulation: true, clock: clock, nasAvailable: true, s: state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{m.ID: &m}}}
			for i := range count {
				id := fmt.Sprintf("queued-%06d", i)
				s := projectSpec(fmt.Sprintf("project-%d", i%4))
				s.Needs.MemoryGB = 1
				q.s.Jobs[id] = &Job{ID: id, State: "queued", Queued: clock.Now(), Spec: s}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				q.mu.RLock()
				order, _ := q.queueProjection()
				q.mu.RUnlock()
				if len(order) != count {
					b.Fatalf("projected %d of %d jobs", len(order), count)
				}
			}
		})
	}
}

func projectSpec(project string) Spec { s := spec("experiment"); s.Project = project; return s }
func TestFairSharingUsesClockAndExplains(t *testing.T) {
	q, c := fixture(t)
	a := submit(t, q, projectSpec("verso"))
	c.Advance(time.Second)
	b := submit(t, q, projectSpec("atlas"))
	m := machine("fake", "fake")
	if e := q.update(func() error {
		q.s.FairUsage = map[string]FairUsage{fairUsageKey("experiment", "gpu", "verso"): {Seconds: 100, At: c.Now()}}
		q.s.Machines[m.ID] = &m
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	s, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	for _, j := range s.Jobs {
		if j.ID == a.ID && !strings.Contains(j.Reason, "atlas goes first (verso is ahead of its fair share)") {
			t.Fatal(j.Reason)
		}
	}
	if got := check(t, q, m, nil).Job; got == nil || got.ID != b.ID {
		t.Fatalf("wanted atlas, got %+v", got)
	}
	c.Advance(24 * time.Hour)
	if got := decayed(FairUsage{Seconds: 100, At: c.Now().Add(-24 * time.Hour), HorizonSeconds: (24 * time.Hour).Seconds(), QuantumSeconds: (24 * time.Hour).Seconds()}, c.Now()); math.Abs(got-50) > 1e-9 {
		t.Fatal(got)
	}
}
