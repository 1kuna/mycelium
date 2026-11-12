package myce

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestTierFairnessInterleavesClasses(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	if _, err := q.SetPriorityPolicy(0, examplePriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		s := spec("production")
		s.Project = "atlas"
		submit(t, q, s)
		s = projectSpec("verso")
		submit(t, q, s)
	}
	urgent := spec("production")
	urgent.Project = "verso"
	p := submit(t, q, urgent)
	m := machine("one", "fake")
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != p.ID {
		t.Fatal("production did not win", run)
	}
	finish(t, q, run, true)
	var previous string
	for range 8 {
		run = check(t, q, m, nil).Job
		if run == nil {
			t.Fatal("idle despite fitting work")
		}
		if run.Spec.Project == previous {
			t.Fatalf("shared tier repeated %s instead of interleaving", previous)
		}
		previous = run.Spec.Project
		if err := q.Started(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: run.ID, Attempt: run.Attempt}}); err != nil {
			t.Fatal(err)
		}
		c.Advance(time.Second)
		finish(t, q, run, true)
	}
}

func TestTierFairnessWeightsAcrossClasses(t *testing.T) {
	t.Parallel()
	for _, first := range []string{"atlas", "verso"} {
		t.Run(first, func(t *testing.T) {
			q, c := fixture(t)
			policy := examplePriorityPolicy()
			policy.Projects = map[string]ProjectPolicy{"atlas": {Weight: 3}, "verso": {Weight: 1}}
			if _, err := q.SetPriorityPolicy(0, policy); err != nil {
				t.Fatal(err)
			}
			for range 8 {
				s := spec("production")
				s.Project = "atlas"
				submit(t, q, s)
				submit(t, q, projectSpec("verso"))
			}
			// Force each possible initial tie winner without changing policy or
			// elapsed service. Subsequent comparisons must repay the authored 3:1 share.
			drainAdmissionWait(t, q)
			q.mu.Lock()
			for _, j := range q.liveJobs() {
				if j.Spec.Project == first {
					j.Front = 1
				}
			}
			q.mu.Unlock()
			counts := map[string]int{}
			m := machine("one", "fake")
			for turn := range 8 {
				run := check(t, q, m, nil).Job
				if run == nil {
					t.Fatal("fitting work left idle")
				}
				if turn == 0 && run.Spec.Project != first {
					t.Fatal("initial winner was not forced", run.Spec.Project, first)
				}
				counts[run.Spec.Project]++
				c.Advance(time.Second)
				finish(t, q, run, true)
			}
			if counts["atlas"] != 6 || counts["verso"] != 2 {
				t.Fatal("weighted service", counts)
			}
		})
	}
}

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

func TestTierFairnessWarmthCannotStarveColdProject(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	p := PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "shared", Selectors: []string{"*/*"}}}}
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	warm := projectSpec("atlas")
	warm.Priority = ""
	warm.Class = "training"
	warm.Needs.Model = "warm"
	cold := projectSpec("verso")
	cold.Priority = ""
	cold.Class = "preview"
	a := submit(t, q, warm)
	b := submit(t, q, cold)
	q.mu.Lock()
	defer q.mu.Unlock()
	m := machine("one", "fake")
	m.Models = []string{"warm"}
	q.s.Machines[m.ID] = &m
	q.s.FairUsage[q.fairKey(a.Spec)] = FairUsage{Seconds: 60, At: c.Now()}
	if got := q.pick(&m); got == nil || got.ID != b.ID {
		t.Fatal("warm project bypassed fair turn", got)
	}
	order, reasons, _ := q.queueProjection()
	if order[b.ID] != 1 || !strings.Contains(reasons[a.ID], "fair share") {
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

func TestTierFairnessDecayRestartAndStableNames(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	if _, err := q.SetPriorityPolicy(0, examplePriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	s := spec("production")
	s.Project = "atlas"
	a := submit(t, q, s)
	m := machine("one", "fake")
	run := check(t, q, m, nil).Job
	c.Advance(time.Second)
	finish(t, q, run, true)
	q.mu.RLock()
	key := q.fairKey(a.Spec)
	before := q.s.FairUsage[key].Seconds
	q.mu.RUnlock()
	if math.Abs(before-1) > 1e-9 {
		t.Fatal(before)
	}
	c.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenTierQueue(t, q, c)
	q.mu.RLock()
	got, version := q.s.FairUsage[key].Seconds, q.s.FairUsageVersion
	q.mu.RUnlock()
	if math.Abs(got-before/2) > 1e-9 {
		t.Fatal("decay/restart", got, before)
	}
	if version != 1 {
		t.Fatal("version not persisted")
	}
	p := examplePriorityPolicy()
	p.Tiers = []PriorityTier{p.Tiers[1], p.Tiers[0], p.Tiers[2]}
	if _, err := q.SetPriorityPolicy(1, p); err != nil {
		t.Fatal(err)
	}
	q.mu.RLock()
	currentKey := q.fairKey(a.Spec)
	q.mu.RUnlock()
	if currentKey != key {
		t.Fatal("history moved with tier index")
	}
	// Removing a tier leaves its usage inactive; reintroducing the same name keeps it.
	p.Tiers = []PriorityTier{{Name: "other", Selectors: []string{"*/*"}}}
	if _, err := q.SetPriorityPolicy(2, p); err != nil {
		t.Fatal(err)
	}
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.s.FairUsage[key].Seconds == 0 || q.s.FairUsage[q.fairKey(a.Spec)].Seconds != 0 {
		t.Fatal("tier history renamed or lost")
	}
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

func TestTierFairnessModelPreparationChargedOnce(t *testing.T) {
	t.Parallel()
	q, c, m := modelFixture(t)
	q.mu.Lock()
	q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Project: "atlas", Priority: "experiment", Machine: m.ID, Session: m.Session, State: "loading"}
	q.mu.Unlock()
	c.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	key := fairUsageKey("experiment", "gpu", "atlas")
	if got := q.s.FairUsage[key].Seconds; math.Abs(got-1) > 1e-9 {
		t.Fatal("load accounting", got)
	}
	// Multiple calls sharing a loaded model are one allocation, including loading.
	q.calls["one"] = &ModelCall{Project: "atlas", Priority: "experiment", Model: "test", Machine: m.ID}
	q.calls["two"] = &ModelCall{Project: "atlas", Priority: "experiment", Model: "test", Machine: m.ID}
	total := 0.0
	for _, row := range q.deviceUsage(&m) {
		total += row.Fair
	}
	if total != 1 {
		t.Fatal("shared load/calls double charged", total)
	}
}

func TestTierFairnessReclaimsIdleWarmthBeforeServingOverspentProject(t *testing.T) {
	t.Parallel()
	q, c, m := modelFixture(t)
	if _, err := q.SetPriorityPolicy(0, examplePriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	slot := &ModelSlot{Model: "test", Project: "verso", Priority: "experiment", Machine: m.ID, Session: m.Session, State: "ready", LastUsed: c.Now()}
	q.mu.Lock()
	q.s.ModelSlots[slotKey(m.ID, "test")] = slot
	q.s.Machines[m.ID].FreeMemoryGB = 0
	q.s.FairUsage[fairUsageKey("shared", "gpu", "verso")] = FairUsage{Seconds: 100, At: c.Now()}
	q.mu.Unlock()
	a := spec("production")
	a.Project = "atlas"
	batch := submit(t, q, a)
	call := &ModelCall{Model: "test", Project: "verso", Priority: "experiment"}
	if err := q.addCall(call); err != nil {
		t.Fatal(err)
	}
	q.mu.RLock()
	defer q.mu.RUnlock()
	if slot.State != "draining" || call.Machine != "" {
		t.Fatalf("warmth bypassed %s fair turn: slot=%s call=%s", batch.ID, slot.State, call.Machine)
	}
	if q.s.Jobs[batch.ID].State != "queued" {
		t.Fatal("assigned before exact model release")
	}
}

func TestTierFairnessCappedDemandDoesNotStopBackfill(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	p := examplePriorityPolicy()
	p.Tiers[0].Selectors = append(p.Tiers[0].Selectors, "atlas/urgent")
	p.Projects = map[string]ProjectPolicy{"atlas": {MaxConcurrent: 1}}
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	a := submit(t, q, projectSpec("atlas"))
	m1, m2 := machine("one", "fake"), machine("two", "fake")
	if got := check(t, q, m1, nil).Job; got == nil || got.ID != a.ID {
		t.Fatal(got)
	}
	low := submit(t, q, projectSpec("other"))
	if got := check(t, q, m2, nil).Job; got == nil || got.ID != low.ID {
		t.Fatal(got)
	}
	urgent := projectSpec("atlas")
	urgent.Priority = ""
	urgent.Class = "urgent"
	waiting := submit(t, q, urgent)
	if job(t, q, low.ID).State != "running" || job(t, q, a.ID).State != "running" {
		t.Fatal("capped demand interrupted useful work")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if reason := q.capReason(q.s.Jobs[waiting.ID]); !strings.Contains(reason, "concurrent work cap") {
		t.Fatal(reason)
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
