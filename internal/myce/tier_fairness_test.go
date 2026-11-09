package myce

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTierFairnessInterleavesClasses(t *testing.T) {
	t.Parallel()

	t.Run("contract", func(t *testing.T) {
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

	})

	t.Run("TierFairnessSharesSimultaneousCapacity", func(t *testing.T) {
		// Source phase stays serial within the contract journey.
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

	})
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

	t.Run("contract", func(t *testing.T) {
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

	})

	t.Run("TierFairnessCappedDemandDoesNotStopBackfill", func(t *testing.T) {
		// Source phase stays serial within the contract journey.
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

	})
}

func TestTierFairnessReclaimsIdleWarmthBeforeServingOverspentProject(t *testing.T) {
	t.Parallel()
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
