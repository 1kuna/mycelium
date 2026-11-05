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
	for _, state := range []string{"unprepared", "offered", "building", "failed", "busy", "stale"} {
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
			switch state {
			case "offered":
				q.s.PreparationPlans = map[string]HostLaunch{"offer": {Machine: m.ID, Session: live.Session, Key: r.Key, Kind: "environment"}}
				want = "waiting_for_builder"
			case "building":
				live.EnvironmentReports = []EnvironmentReport{{Key: r.Key, State: "building"}}
				want = "building"
			case "failed":
				live.EnvironmentReports = []EnvironmentReport{{Key: r.Key, State: "failed", FailureKind: "deterministic", Error: "build exit 1", Log: "No solution found: package absent from first index"}}
				want = "deterministically_failed"
			case "busy":
				live.CPUCapacityReason = "no preparation core available"
				want = "waiting_for_builder"
			case "stale":
				clock.Advance(24 * time.Hour)
				want = "unknown_observation"
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

func TestActionableBlockerCPUResolveOmitsAcceleratorViews(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	r, err := q.Resolve(ResolveRequest{Spec: cpuResolveSpec()})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Targets) != 1 || !r.Targets[0].Eligible || r.Blocker != nil {
		t.Fatalf("%+v", r)
	}
	var out strings.Builder
	if err := WriteResolve(&out, r, false); err != nil {
		t.Fatal(err)
	}
	for _, noise := range []string{"select-none", "one card holds", "0 GiB"} {
		if strings.Contains(out.String(), noise) {
			t.Fatal(out.String())
		}
	}
}

// #644: preserve the precise inline validation error and consumer remedy.
func TestActionableBlockerInlineValidation(t *testing.T) {
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
}
