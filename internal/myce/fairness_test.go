package myce

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
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
				order, _, _ := q.queueProjection()
				q.mu.RUnlock()
				if len(order) != count {
					b.Fatalf("projected %d of %d jobs", len(order), count)
				}
			}
		})
	}
}

func projectSpec(project string) Spec { s := spec("experiment"); s.Project = project; return s }

func TestMemoryShareCap(t *testing.T) {
	q, _ := fixture(t)
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": {Weight: 1, MaxMemoryShare: .25}}); e != nil {
		t.Fatal(e)
	}
	j := submit(t, q, projectSpec("atlas"))
	m := machine("one", "fake")
	m.TotalMemoryGB = 16
	check(t, q, m, nil)
	q.mu.Lock()
	reason := q.capReason(q.s.Jobs[j.ID])
	q.mu.Unlock()
	if !strings.Contains(reason, "25%") {
		t.Fatal("memory cap not explained")
	}
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": {Weight: 1, MaxMemoryShare: .5}}); e != nil {
		t.Fatal(e)
	}
	if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("boundary share must fit")
	}
}

func TestModelCallMemoryCapCountsSharedModelOnce(t *testing.T) {
	q, _, m := modelFixture(t)
	m.TotalMemoryGB = 8
	q.mu.Lock()
	q.s.Machines[m.ID].TotalMemoryGB = 8
	q.mu.Unlock()
	if e := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": {Weight: 1, MaxMemoryShare: 1}}); e != nil {
		t.Fatal(e)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls["active"] = &ModelCall{ID: "active", Project: "atlas", Priority: "experiment", Model: "test", Machine: m.ID, Session: m.Session}
	next := callJob(&ModelCall{ID: "next", Project: "atlas", Priority: "experiment", Model: "test"})
	if reason := q.capReasonFor(next, &m); reason != "" {
		t.Fatal("same reservation counted twice", reason)
	}
	other := machine("two", "fake")
	if reason := q.capReasonFor(next, &other); reason == "" {
		t.Fatal("new device reservation bypassed memory cap")
	}
}
func TestProjectPolicyValidation(t *testing.T) {
	q, _ := fixture(t)
	for _, p := range []ProjectPolicy{{Weight: -1}, {Weight: math.NaN()}, {Weight: 1, MaxConcurrentExperiments: -1}, {Weight: 1, MaxMemoryShare: 1.1}} {
		if e := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": p}); e == nil {
			t.Fatal("invalid policy accepted", p)
		}
	}
	path := filepath.Join(t.TempDir(), "projects.yaml")
	if e := os.WriteFile(path, []byte("atlas:\n  weight: 2\n  max_concurrent_experiments: 1\n"), 0600); e != nil {
		t.Fatal(e)
	}
	p, e := ReadProjectPolicies(path)
	if e != nil || p["atlas"].Weight != 2 {
		t.Fatal(p, e)
	}
	if e = os.WriteFile(path, []byte("atlas:\n  weight: 2\n---\nverso:\n  weight: 3\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = ReadProjectPolicies(path); e == nil {
		t.Fatal("multiple configs silently accepted")
	}
}

func TestProjectionUsesAffinityDeadlineAndDoesNotMutateJobs(t *testing.T) {
	q, c := fixture(t)
	a := submit(t, q, projectSpec("atlas"))
	c.Advance(time.Second)
	s := projectSpec("atlas")
	s.Needs.Model = "cached"
	b := submit(t, q, s)
	m := machine("one", "fake")
	m.CachedModels = []string{"cached"}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.s.Machines[m.ID] = &m
	order, _, _ := q.queueProjection()
	if order[b.ID] != 1 || order[a.ID] != 2 || q.s.Jobs[b.ID].State != "queued" || q.pick(&m).ID != b.ID {
		t.Fatal(order)
	}
}
