package myce

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// Replays the no-builder observations in #710/#727 and retained failure in
// #717 without changing the already-fixed scheduling/admission contracts.
func TestActionableBlockerPreparationObservationsAgree(t *testing.T) {
	for _, state := range []string{"unprepared", "offered", "building", "failed", "uploading", "busy", "stale"} {
		t.Run(state, func(t *testing.T) {
			q, clock := fixture(t)
			r := binaryRecipe(t, "cpu")
			if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
				t.Fatal(err)
			}
			m := cpuMachine("worker", "worker", "cpu", 8, 32)
			m.EnvironmentBuilder = true
			check(t, q, m, nil)
			s := cpuSpec(1, 1)
			s.Environment.Name = "tools"
			j := submit(t, q, s)
			live := q.s.Machines[m.ID]
			want := "unprepared"
			if err := q.update(func() error {
				switch state {
				case "offered":
					q.s.PreparationPlans = map[string]HostLaunch{"offer": {Machine: m.ID, Session: live.Session, Key: r.Key, Kind: "environment"}}
					want = "waiting_for_builder"
				case "building":
					live.EnvironmentReports = []EnvironmentReport{{Key: r.Key, State: "building"}}
					want = "building"
				case "failed", "uploading":
					live.EnvironmentReports = []EnvironmentReport{{Key: r.Key, State: "failed", FailureKind: "deterministic", Error: "build exit 1", Log: "No solution found: package absent from first index"}}
					if state == "uploading" {
						live.EnvironmentReports[0].State = "uploading"
					}
					want = "deterministically_failed"
				case "busy":
					live.CPUCapacityReason = "no preparation core available"
					want = "waiting_for_builder"
				case "stale":
					clock.Advance(24 * time.Hour)
					want = "unknown_observation"
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			got, err := q.jobSnapshot(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := q.publicSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			var aggregate *Blocker
			for _, row := range snapshot.Jobs {
				if row.ID == j.ID {
					aggregate = row.Blocker
				}
			}
			resolved, err := q.Resolve(ResolveRequest{Spec: s})
			if err != nil {
				t.Fatal(err)
			}
			if got.Blocker == nil || got.Blocker.Code != want || !reflect.DeepEqual(got.Blocker, aggregate) || !reflect.DeepEqual(got.Blocker, resolved.Blocker) {
				t.Fatalf("%s: GET=%+v aggregate=%+v resolve=%+v", state, got.Blocker, aggregate, resolved.Blocker)
			}
			if state == "failed" && !strings.Contains(got.Blocker.Evidence, "first index") {
				t.Fatal(got.Blocker)
			}
			if state != "building" && strings.Contains(got.Blocker.Text(), "builder reports building") {
				t.Fatal(got.Blocker)
			}
		})
	}
}

// #644: preserve the precise inline validation error and consumer remedy.
func TestActionableBlockerInlineValidation(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _ := fixture(t)
		s := cpuResolveSpec()
		s.Environment = Environment{LockFile: "x-requirements.txt", Python: "3.12"}
		r, err := q.Resolve(ResolveRequest{Spec: s})
		if err != nil {
			t.Fatal(err)
		}
		if r.Blocker == nil || r.Blocker.Owner != "consumer" || !strings.Contains(r.Blocker.Evidence, "lock_file must be") {
			t.Fatalf("%+v", r)
		}

	})

	t.Run("ActionableBlockerNativeResolveQualification", func(t *testing.T) {
		q, _, _, s := observationFixture(t)
		e := q.s.Extensions[s.Extension]
		r, err := q.Resolve(ResolveRequest{Spec: Spec{Project: s.Project}, Recipe: &e})
		if err != nil {
			t.Fatal(err)
		}
		if r.Blocker == nil || r.Blocker.Owner != "consumer" || !strings.Contains(r.Blocker.Action, "qualify") {
			t.Fatalf("%+v", r.Blocker)
		}

	})
}

func TestActionableBlockerResolveCurrentMemory(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _ := fixture(t)
		m := cpuMachine("worker", "worker", "cpu", 8, 0.5)
		check(t, q, m, nil)
		s := cpuSpec(1, 4)
		j := submit(t, q, s)
		got, err := q.jobSnapshot(j.ID)
		if err != nil {
			t.Fatal(err)
		}
		r, err := q.Resolve(ResolveRequest{Spec: s})
		if err != nil {
			t.Fatal(err)
		}
		if got.Blocker == nil || got.Blocker.Code != "resource_pressure" || !reflect.DeepEqual(got.Blocker, r.Blocker) {
			t.Fatalf("GET=%+v resolve=%+v", got.Blocker, r.Blocker)
		}

	})

	t.Run("ActionableBlockerKnownSharedPressure", func(t *testing.T) {
		q, _ := fixture(t)
		m := sharedHostFixture(t, q)
		if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
			t.Fatal(err)
		}
		m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
		m.SharedHostPressure.MemoryLevel = 4
		check(t, q, m, nil)
		s := cpuSpec(1, 1)
		j := submit(t, q, s)
		got, err := q.jobSnapshot(j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Blocker == nil || got.Blocker.Code != "resource_pressure" || !strings.Contains(got.Blocker.Evidence, "memory pressure") {
			t.Fatalf("%+v", got.Blocker)
		}

	})
}
