package myce

import (
	"errors"
	"strings"
	"testing"
)

func TestTierFairnessSharesSimultaneousCapacity(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	if _, err := q.SetPriorityPolicy(0, examplePriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		s := spec("production")
		s.Project = "atlas"
		s.Needs.Model = "warm"
		submit(t, q, s)
	}
	v := submit(t, q, projectSpec("verso"))
	a, b := machine("one", "fake"), machine("two", "fake")
	a.Models = []string{"warm"}
	b.Models = []string{"warm"}
	first := check(t, q, a, nil).Job
	if first == nil || first.Spec.Project != "atlas" {
		t.Fatal(first)
	}
	// No time passes: active reservations, rather than affinity, break equal usage.
	second := check(t, q, b, nil).Job
	if second == nil || second.ID != v.ID {
		t.Fatal("one project took both fresh devices", second)
	}
	if job(t, q, first.ID).State != "running" {
		t.Fatal("same-tier work was preempted")
	}
}

func TestTierFairnessFitBackfillAndHistoryIsolation(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	if _, err := q.SetPriorityPolicy(0, examplePriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	a := spec("production")
	a.Project = "atlas"
	a.Needs.MemoryGB = 16
	big := submit(t, q, a)
	v := submit(t, q, projectSpec("verso"))
	urgent := spec("production")
	urgent.Project = "verso"
	urgent.Needs.Platform = Platform{Required: []string{"cuda"}}
	submit(t, q, urgent)
	q.mu.Lock()
	defer q.mu.Unlock()
	m := machine("one", "fake")
	q.s.Machines[m.ID] = &m
	// Neither a higher incompatible tier nor an under-served oversized job idles
	// this worker. Unrelated CPU and urgent-tier usage cannot charge the GPU tier.
	q.s.FairUsage = map[string]FairUsage{
		fairUsageKey("shared", "gpu", "verso"): {Seconds: 100, At: c.Now()},
		fairUsageKey("shared", "cpu", "atlas"): {Seconds: 1e9, At: c.Now()},
		fairUsageKey("urgent", "gpu", "atlas"): {Seconds: 1e9, At: c.Now()},
	}
	if got := q.pick(&m); got == nil || got.ID != v.ID {
		t.Fatal("backfill", got)
	}
	q.s.Jobs[big.ID].Spec.Needs.MemoryGB = 8
	if got := q.pick(&m); got == nil || got.ID != big.ID {
		t.Fatal("unrelated history affected sharing", got)
	}
	order, reasons, _ := q.queueProjection()
	if order[big.ID] != 1 || order[v.ID] != 2 || !strings.Contains(reasons[v.ID], "atlas goes first (verso is ahead of its fair share)") {
		t.Fatal(order, reasons)
	}
}

func reopenTierQueue(t *testing.T, q *Queue, c Clock) *Queue {
	t.Helper()
	var seq int
	var name, path string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { next.Close() })
	return next
}

func TestTierFairnessOwnerCapCASAndCalls(t *testing.T) {
	t.Parallel()
	q, c, m := modelFixture(t)
	policy := examplePriorityPolicy()
	policy.Projects = map[string]ProjectPolicy{"atlas": {MaxConcurrent: 1}}
	record, err := q.SetPriorityPolicy(0, policy)
	if err != nil || record.Policy.Projects["atlas"].Weight != 1 {
		t.Fatal(record, err)
	}
	if _, err = q.SetPriorityPolicy(0, policy); !errors.Is(err, ErrPolicyRevision) {
		t.Fatal("stale policy replaced cap", err)
	}
	s := spec("production")
	s.Project = "atlas"
	run := submit(t, q, s)
	// Private cap helpers share state with asynchronous admission capture.
	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.s.Jobs[run.ID].State = "stopping"
		q.s.Jobs[run.ID].Machine = m.ID
		call := callJob(&ModelCall{ID: "next", Project: "atlas", Priority: "production", Model: "test"})
		if reason := q.capReasonFor(call, &m); !strings.Contains(reason, "concurrent work cap") {
			t.Fatal(reason)
		}
		q.s.Jobs[run.ID].State = "succeeded"
		q.calls["active"] = &ModelCall{ID: "active", Project: "atlas", Priority: "production", Model: "test", Machine: m.ID}
	}()
	queued := submit(t, q, s)
	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		if reason := q.capReason(q.s.Jobs[queued.ID]); !strings.Contains(reason, "concurrent work cap") {
			t.Fatal(reason)
		}
		q.calls["active"].Error = "finished"
		if reason := q.capReason(q.s.Jobs[queued.ID]); reason != "" {
			t.Fatal(reason)
		}
	}()
	policy.Projects["atlas"] = ProjectPolicy{MaxConcurrent: -1}
	if _, err = q.SetPriorityPolicy(1, policy); err == nil {
		t.Fatal("negative cap accepted")
	}
	if q.PriorityPolicy().Policy.Projects["atlas"].MaxConcurrent != 1 {
		t.Fatal("invalid update changed cap")
	}
	q = reopenTierQueue(t, q, c)
	if got := q.PriorityPolicy(); got.Revision != 1 || got.Policy.Projects["atlas"].MaxConcurrent != 1 {
		t.Fatal("cap/revision lost on restart", got)
	}

}

func TestTierFairnessRetiredUsageRefused(t *testing.T) {
	q, c := fixture(t)
	if err := q.update(func() error { q.s.FairUsageVersion = 0; return nil }); err != nil {
		t.Fatal(err)
	}
	path := strings.TrimSuffix(q.lock.Name(), ".lock")
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, q.root, c)
	if err == nil {
		reopened.Close()
		t.Fatal("unversioned fair usage accepted")
	}
	if !strings.Contains(err.Error(), "unsupported fair-usage format") {
		t.Fatal(err)
	}
}
