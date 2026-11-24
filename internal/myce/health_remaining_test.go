package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRecipeAlertClosesWhenQueuedDemandEnds(t *testing.T) {
	q, clock := fixture(t)
	const kind, condition = "environment_recipe_failed", "recipe/worker/recipe"
	worker := cpuMachine("worker", "worker", "cpu", 4, 16)
	worker.EnvironmentBuilder = true
	alternate := cpuMachine("alternate", "alternate", "cpu", 4, 16)
	alternate.EnvironmentBuilder = true
	check(t, q, worker, nil)
	check(t, q, alternate, nil)
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{
		"tools": {"cpu": {Key: "recipe", Platform: "cpu"}},
	}}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 1)
	s.Environment.Name = "tools"
	job := submit(t, q, s)
	worker.EnvironmentReports = []EnvironmentReport{{Key: "recipe", State: "failed", FailureKind: "deterministic", Error: "private diagnostic"}}
	check(t, q, worker, nil)
	requireIncident(t, q, kind, condition, 1, true)
	if err := q.Cancel(job.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	check(t, q, worker, nil)
	requireIncident(t, q, kind, condition, 1, false)
	state, err := q.cachedCompactStatusJSON(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(state, "recipe: failed") || strings.Contains(state, `"kind":"recipe"`) || strings.Contains(state, "private diagnostic") {
		t.Fatalf("retained failure showed as active preparation: %s", state)
	}
	worker.EnvironmentReports = nil
	check(t, q, worker, nil)
	if got := q.s.Machines[worker.ID].EnvironmentReports; len(got) != 1 || got[0].State != "failed" {
		t.Fatalf("lost retained scheduler exclusion: %+v", got)
	}
	submit(t, q, s)
	requireIncident(t, q, kind, condition, 2, true)
}

func TestRemainingHealthAttemptStreak(t *testing.T) {
	for _, hosts := range []int{2, 200} {
		t.Run(fmt.Sprint(hosts), func(t *testing.T) {
			q, c := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 4, 16)
			if err := q.update(func() error {
				q.touchMachine(m.ID)
				q.s.Machines[m.ID] = &m
				for i := 1; i < hosts; i++ {
					peer := cpuMachine(fmt.Sprintf("peer-%d", i), fmt.Sprintf("peer-%d", i), "cpu", 4, 16)
					q.touchMachine(peer.ID)
					q.s.Machines[peer.ID] = &peer
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			// Exercise the committed exact-attempt observer, including duplicate and
			// enriched replay. This hook also receives queue-caused quiet losses.
			j := &Job{ID: "job", State: "queued", Spec: cpuSpec(1, 1)}
			record := func(attempt int, infra, planned, staging bool) {
				t.Helper()
				c.Advance(time.Second)
				o := AttemptOutcome{Attempt: attempt, Machine: m.ID, Session: m.Session, Interrupted: infra, PoliteStop: planned, inputsStaged: !infra}
				if staging {
					o.ExecutionCause = &RuntimeCause{Operation: "input_staging", Class: "storage_full"}
				}
				if err := q.update(func() error { q.recordAttemptOutcome(j, o); return nil }); err != nil {
					t.Fatal(err)
				}
			}
			for i := 1; i <= 4; i++ {
				record(i, false, false, false)
			}
			if firings(q, "infrastructure_loss_streak") != 0 {
				t.Fatal("healthy endings alerted")
			}
			record(5, true, false, true)
			record(5, true, false, true)
			requireIncident(t, q, "staging_cache_refused", "attempt-staging/worker", 1, true)
			if firings(q, "infrastructure_loss_streak") != 0 {
				t.Fatal("same attempt counted twice")
			}
			record(6, true, true, false)
			record(7, true, false, true)
			requireIncident(t, q, "infrastructure_loss_streak", "infrastructure-streak/worker", 1, true)
			record(8, true, false, true)
			requireIncident(t, q, "infrastructure_loss_streak", "infrastructure-streak/worker", 1, true)
			record(9, false, false, false)
			requireIncident(t, q, "infrastructure_loss_streak", "infrastructure-streak/worker", 1, false)
			requireIncident(t, q, "staging_cache_refused", "attempt-staging/worker", 1, false)
		})
	}
}
