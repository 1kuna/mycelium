package myce

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRemainingHealthReportsRecipesCachesUpdates(t *testing.T) {
	for _, hosts := range []int{2, 200} {
		t.Run(fmt.Sprint(hosts), func(t *testing.T) {
			q, c := fixture(t)
			// Install in-memory synthetic inventory in one transaction, avoiding O(n)
			// SQLite writes per worker merely to construct the large replay.
			m := cpuMachine("worker", "worker", "cpu", 4, 16)
			m.LastSeen = c.Now()
			if err := q.update(func() error {
				for i := range hosts {
					peer := cpuMachine(fmt.Sprintf("peer-%d", i), fmt.Sprintf("peer-%d", i), "cpu", 4, 16)
					peer.LastSeen = c.Now()
					q.touchMachine(peer.ID)
					q.s.Machines[peer.ID] = &peer
					q.acceptContactPeriod(&peer, time.Second, nil) // Accepted synthetic current-worker inventory.
				}
				q.touchMachine(m.ID)
				q.s.Machines[m.ID] = &m
				q.acceptContactPeriod(&m, time.Second, nil)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(q.s.Health.Events) != 0 {
				t.Fatal("healthy inventory alerted", q.s.Health.Events)
			}
			// Current accepted failures, with arbitrary private text that must never
			// enter notification payloads. No live database or filesystem evidence.
			if err := q.update(func() error {
				q.touchMachine(m.ID)
				m.CapabilityProfile = nil
				m.EnvironmentReports = []EnvironmentReport{{Key: "recipe", State: "failed", FailureKind: "deterministic", Error: "private diagnostic"}}
				m.CacheError = "private cache error"
				q.s.FleetUpgrade = &fleetUpgrade{State: "failed", Hosts: []*fleetUpgradeHost{{FleetHostStatus: FleetHostStatus{Host: "worker", Phase: "failed", Failure: "private update error", FailureCode: "fleet_staging_write_failed"}}}}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			cases := []struct{ kind, key string }{{"worker_reports_missing", "reports/worker/cpu_capacity_report_missing"}, {"environment_recipe_failed", "recipe/worker/recipe"}, {"staging_cache_refused", "cache/worker"}, {"update_failed", "update/worker"}}
			for _, tc := range cases {
				requireIncident(t, q, tc.kind, tc.key, 1, true)
			}
			for range 3 {
				c.Advance(time.Second)
				if err := q.update(func() error { return nil }); err != nil {
					t.Fatal(err)
				}
			}
			for _, tc := range cases {
				requireIncident(t, q, tc.kind, tc.key, 1, true)
			}
			payload, _ := json.Marshal(q.s.Health.Events)
			if strings.Contains(string(payload), "private") {
				t.Fatal("raw diagnostic exposed", string(payload))
			}
			if err := q.update(func() error {
				q.touchMachine(m.ID)
				m.CapabilityProfile = cpuMachine("worker", "worker", "cpu", 4, 16).CapabilityProfile
				m.EnvironmentReports[0].State = "ready"
				m.CacheError = ""
				q.s.FleetUpgrade = nil
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for _, tc := range cases {
				requireIncident(t, q, tc.kind, tc.key, 1, false)
			}
		})
	}
}

func TestRemainingHealthPinsTargetsAndHealthyOccupancy(t *testing.T) {
	for _, hosts := range []int{2, 200} {
		t.Run(fmt.Sprint(hosts), func(t *testing.T) {
			q, c := fixture(t)
			if err := q.update(func() error {
				for i := range hosts {
					m := cpuMachine(fmt.Sprintf("worker-%d", i), fmt.Sprintf("worker-%d", i), "cpu", 4, 16)
					m.LastSeen = c.Now()
					q.touchMachine(m.ID)
					q.s.Machines[m.ID] = &m
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			s := cpuSpec(1, 1)
			s.AllowedMachines = []string{"worker-0"}
			j := submit(t, q, s)
			requireIncident(t, q, "pin_reason_missing", "pin-reason/"+j.ID, 1, true)
			if err := q.Cancel(j.ID); err != nil {
				t.Fatal(err)
			}
			requireIncident(t, q, "pin_reason_missing", "pin-reason/"+j.ID, 1, false)
			// A justified pin, healthy cold recipe and occupied peers don't diagnose
			// permanent incompatibility. All capacity predicates use resolve's view.
			s.RequirementReasons = map[string]string{"host": "hardware validation"}
			healthy := submit(t, q, s)
			if len(alertsOf(q, "pin_reason_missing")) != 1 || len(alertsOf(q, "job_target_unsatisfied")) != 0 {
				t.Fatal("healthy intent alerted")
			}
			if err := q.Cancel(healthy.ID); err != nil {
				t.Fatal(err)
			}
			s = cpuSpec(20, 1)
			bad := submit(t, q, s)
			requireIncident(t, q, "job_target_unsatisfied", "target/"+bad.ID, 1, true)
			if err := q.update(func() error { q.touchMachine("worker-0").CapabilityProfile.LogicalCPUs = 32; return nil }); err != nil {
				t.Fatal(err)
			}
			requireIncident(t, q, "job_target_unsatisfied", "target/"+bad.ID, 1, false)
		})
	}
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
			if len(alertsOf(q, "infrastructure_loss_streak")) != 0 {
				t.Fatal("healthy endings alerted")
			}
			record(5, true, false, true)
			record(5, true, false, true)
			requireIncident(t, q, "staging_cache_refused", "attempt-staging/worker", 1, true)
			if len(alertsOf(q, "infrastructure_loss_streak")) != 0 {
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
