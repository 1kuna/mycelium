package myce

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestTierFairnessInterleavesClasses(t *testing.T) {
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
	q, c := fixture(t)
	policy := examplePriorityPolicy()
	policy.Projects = map[string]ProjectPolicy{"atlas": {Weight: 3}, "verso": {Weight: 1}}
	if _, err := q.SetPriorityPolicy(0, policy); err != nil {
		t.Fatal(err)
	}
	for range 24 {
		s := spec("production")
		s.Project = "atlas"
		submit(t, q, s)
		submit(t, q, projectSpec("verso"))
	}
	counts := map[string]int{}
	m := machine("one", "fake")
	for range 24 {
		run := check(t, q, m, nil).Job
		if run == nil {
			t.Fatal("fitting work left idle")
		}
		counts[run.Spec.Project]++
		c.Advance(time.Second)
		finish(t, q, run, true)
	}
	if counts["atlas"] != 18 || counts["verso"] != 6 {
		t.Fatal("weighted service", counts)
	}
}

func TestTierFairnessSharesSimultaneousCapacity(t *testing.T) {
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
	order, reasons := q.queueProjection()
	if order[big.ID] != 1 || order[v.ID] != 2 || !strings.Contains(reasons[v.ID], "atlas goes first (verso is ahead of its fair share)") {
		t.Fatal(order, reasons)
	}
}

func TestTierFairnessWarmthCannotStarveColdProject(t *testing.T) {
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
	m := machine("one", "fake")
	m.Models = []string{"warm"}
	q.s.Machines[m.ID] = &m
	q.s.FairUsage[q.fairKey(a.Spec)] = FairUsage{Seconds: 60, At: c.Now()}
	if got := q.pick(&m); got == nil || got.ID != b.ID {
		t.Fatal("warm project bypassed fair turn", got)
	}
	order, reasons := q.queueProjection()
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
	key := q.fairKey(a.Spec)
	before := q.s.FairUsage[key].Seconds
	if before <= .99 || before > 1 {
		t.Fatal(before)
	}
	c.Advance(FairHalfLife)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenTierQueue(t, q, c)
	if got := q.s.FairUsage[key].Seconds; math.Abs(got-before/2) > 1e-9 {
		t.Fatal("decay/restart", got, before)
	}
	if q.s.FairUsageVersion != 1 {
		t.Fatal("version not persisted")
	}
	p := examplePriorityPolicy()
	p.Tiers = []PriorityTier{p.Tiers[1], p.Tiers[0], p.Tiers[2]}
	if _, err := q.SetPriorityPolicy(1, p); err != nil {
		t.Fatal(err)
	}
	if q.fairKey(a.Spec) != key {
		t.Fatal("history moved with tier index")
	}
	// Removing a tier leaves its usage inactive; reintroducing the same name keeps it.
	p.Tiers = []PriorityTier{{Name: "other", Selectors: []string{"*/*"}}}
	if _, err := q.SetPriorityPolicy(2, p); err != nil {
		t.Fatal(err)
	}
	if q.s.FairUsage[key].Seconds == 0 || q.s.FairUsage[q.fairKey(a.Spec)].Seconds != 0 {
		t.Fatal("tier history renamed or lost")
	}
}

func TestTierFairnessLegacyUsageMigration(t *testing.T) {
	q, c := fixture(t)
	// Emulate the old persisted record, whose project totals had no tier metadata.
	if err := q.update(func() error {
		q.s.FairUsageVersion = 0
		q.s.FairUsage = map[string]FairUsage{"atlas": {Seconds: 120, At: c.Now()}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q = reopenTierQueue(t, q, c)
	key := fairUsageKey("experiment", "gpu", "atlas")
	if q.s.FairUsageVersion != 1 || len(q.s.FairUsage) != 1 || q.s.FairUsage[key].Seconds != 120 {
		t.Fatal(q.s.FairUsage)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q = reopenTierQueue(t, q, c)
	if len(q.s.FairUsage) != 1 || q.s.FairUsage[key].Seconds != 120 {
		t.Fatal("migration repeated", q.s.FairUsage)
	}
}

func TestTierFairnessOwnerCapCASAndCalls(t *testing.T) {
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
	q.s.Jobs[run.ID].State = "stopping"
	q.s.Jobs[run.ID].Machine = m.ID
	call := callJob(&ModelCall{ID: "next", Project: "atlas", Priority: "production", Model: "test"})
	if reason := q.capReasonFor(call, &m); !strings.Contains(reason, "concurrent work cap") {
		t.Fatal(reason)
	}
	q.s.Jobs[run.ID].State = "succeeded"
	q.calls["active"] = &ModelCall{ID: "active", Project: "atlas", Priority: "production", Model: "test", Machine: m.ID}
	queued := submit(t, q, s)
	if reason := q.capReason(q.s.Jobs[queued.ID]); !strings.Contains(reason, "concurrent work cap") {
		t.Fatal(reason)
	}
	q.calls["active"].Error = "finished"
	if reason := q.capReason(q.s.Jobs[queued.ID]); reason != "" {
		t.Fatal(reason)
	}
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
	q, c, m := modelFixture(t)
	q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Project: "atlas", Priority: "experiment", Machine: m.ID, Session: m.Session, State: "loading"}
	c.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	key := fairUsageKey("experiment", "gpu", "atlas")
	if got := q.s.FairUsage[key].Seconds; got <= .99 || got > 1 {
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

func TestTierFairnessSimulatorUsesPolicy(t *testing.T) {
	a := simTestStream("atlas", "production", 0, 1, 100)
	v := simTestStream("verso", "experiment", 0, 1, 100)
	w := simTestWork(a, v)
	p := examplePriorityPolicy()
	p.Projects = map[string]ProjectPolicy{"atlas": {Weight: 3}, "verso": {Weight: 1}}
	w.Policy = &p
	r := runSimTest(t, simTestFleet(1), w, 24)
	nearSim(t, r.Projects["atlas"].GPUHours, 18.0/3600)
	nearSim(t, r.Projects["verso"].GPUHours, 6.0/3600)
	nearSim(t, r.IdleWaitingHours, 0)
	// Arbitrary batch classes use the same catchall planner.
	a.Priority = ""
	a.Class = "training"
	v.Priority = ""
	v.Class = "preview"
	w = simTestWork(a, v)
	w.Policy = &PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "shared", Selectors: []string{"*/*"}}}}
	r = runSimTest(t, simTestFleet(1), w, 24)
	nearSim(t, r.Projects["atlas"].GPUHours, 12.0/3600)
	nearSim(t, r.Projects["verso"].GPUHours, 12.0/3600)
}

func TestTierFairnessReclaimsIdleWarmthBeforeServingOverspentProject(t *testing.T) {
	q, c, m := modelFixture(t)
	if _, err := q.SetPriorityPolicy(0, examplePriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	slot := &ModelSlot{Model: "test", Project: "verso", Priority: "experiment", Machine: m.ID, Session: m.Session, State: "ready", LastUsed: c.Now()}
	q.s.ModelSlots[slotKey(m.ID, "test")] = slot
	q.s.Machines[m.ID].FreeMemoryGB = 0
	q.s.FairUsage[fairUsageKey("shared", "gpu", "verso")] = FairUsage{Seconds: 100, At: c.Now()}
	a := spec("production")
	a.Project = "atlas"
	batch := submit(t, q, a)
	call := &ModelCall{Model: "test", Project: "verso", Priority: "experiment"}
	if err := q.addCall(call); err != nil {
		t.Fatal(err)
	}
	if slot.State != "draining" || call.Machine != "" {
		t.Fatalf("warmth bypassed %s fair turn: slot=%s call=%s", batch.ID, slot.State, call.Machine)
	}
	if q.s.Jobs[batch.ID].State != "queued" {
		t.Fatal("assigned before exact model release")
	}
}

func TestTierFairnessIdleReclamationProtectsActiveAndHigherTiers(t *testing.T) {
	q, _, m := modelFixture(t)
	demand := projectSpec("atlas")
	for _, tc := range []struct {
		state, priority string
		calls           int
		want            bool
	}{
		{"ready", "experiment", 0, true}, {"draining", "experiment", 0, true},
		{"ready", "experiment", 1, false}, {"loading", "experiment", 0, false},
		{"unload_failed", "experiment", 0, false}, {"ready", "production", 0, false},
	} {
		slot := &ModelSlot{Model: "test", Machine: m.ID, State: tc.state, Priority: tc.priority, InFlight: tc.calls}
		if got := q.slotReclaimableFor(slot, demand); got != tc.want {
			t.Fatalf("%+v: %v", tc, got)
		}
	}
	slot := &ModelSlot{Model: "test", Machine: m.ID, State: "ready", Priority: "experiment"}
	q.s.ModelSlots[slotKey(m.ID, "test")] = slot
	q.s.Pins = []ModelPin{{Model: "test", Project: "verso", Assigned: m.ID, Machine: m.ID}}
	if q.slotReclaimableFor(slot, demand) {
		t.Fatal("pin reclaimed")
	}
}

func TestTierFairnessCappedDemandDoesNotStopBackfill(t *testing.T) {
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
	if reason := q.capReason(q.s.Jobs[waiting.ID]); !strings.Contains(reason, "concurrent work cap") {
		t.Fatal(reason)
	}
}
