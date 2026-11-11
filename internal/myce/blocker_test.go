package myce

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestActionableBlockerImplementationCoverage(t *testing.T) {
	for _, scenario := range []string{"unqualified", "missing_variant", "command_target", "stale", "stale_alternative", "missing_registration", "parity"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock := fixture(t)
			e, a, _ := variantsFixture(t, false)
			if scenario != "missing_registration" {
				if err := q.AddExtension(e); err != nil {
					t.Fatal(err)
				}
			}
			want := "native_qualification"
			s := variantSpec(e, "experiment")
			switch scenario {
			case "unqualified":
				a.ExtensionReports = nil
			case "missing_variant":
				a.CapabilityProfile.Arch = "arm64"
				want = "execution_coverage"
			case "command_target":
				s = cpuSpec(1, 1)
				s.Targets = []ExecutionTarget{{"darwin", "arm64", "cpu"}}
				want = "execution_coverage"
			case "stale", "stale_alternative":
				a.ExtensionReports = nil
				want = "unknown_observation"
			case "missing_registration":
				// A kind can wait for a registration to arrive later.
				s.Extension, s.Kind = "", "not-registered"
				want = "missing_extension"
			case "parity":
				s.Class = "production"
				want = "production_parity"
			}
			check(t, q, a, nil)
			j := submit(t, q, s)
			if scenario == "parity" {
				if err := q.update(func() error {
					q.s.Jobs[j.ID].RequireProductionParity = true
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "stale" || scenario == "stale_alternative" {
				clock.Advance(24 * time.Hour)
				if scenario == "stale_alternative" {
					other := valueMachine("incompatible", "fake", 32)
					other.CapabilityProfile.Arch = "arm64"
					check(t, q, other, nil)
				}
			}
			got, err := q.jobSnapshot(j.ID)
			if err != nil || got.Blocker == nil || got.Blocker.Code != want {
				t.Fatalf("want %s: job=%+v error=%v", want, got, err)
			}
			if scenario == "unqualified" || scenario == "missing_variant" {
				if !strings.Contains(got.Blocker.Evidence, "linux/amd64/fake") || strings.Contains(got.Blocker.Action, a.ID) {
					t.Fatal("coverage advice omitted declared target or selected a host", got.Blocker)
				}
			}
			if strings.HasPrefix(scenario, "stale") && strings.Contains(got.Blocker.Evidence, "only one freshly") {
				t.Fatal("stale inventory claimed unique hardware", got.Blocker)
			}
		})
	}
}

func TestActionableBlockerSingleHostCoverage(t *testing.T) {
	for _, restriction := range []string{"hardware", "authored", "parity"} {
		t.Run(restriction, func(t *testing.T) {
			q, _ := fixture(t)
			e, a, b := variantsFixture(t, false)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			check(t, q, a, nil)
			check(t, q, b, nil)
			busy := spec("experiment")
			busy.AllowedMachines = []string{a.ID}
			submit(t, q, busy)
			running := check(t, q, a, nil).Job
			if running == nil {
				t.Fatal("missing occupancy fixture")
			}
			s := variantSpec(e, "experiment")
			if restriction == "authored" {
				s.AllowedMachines = []string{a.ID}
			}
			if restriction == "parity" {
				s.Class = "production"
				policy := q.PriorityPolicy()
				policy.Policy.Projects[s.Project] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{s.workType(): {hostID(&a)}}}
				if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
					t.Fatal(err)
				}
			}
			j := submit(t, q, s)
			before := cloneJob(q.s.Jobs[j.ID])
			got, err := q.jobSnapshot(j.ID)
			if err != nil || got.Blocker == nil {
				t.Fatal(got, err)
			}
			unique := strings.Contains(got.Blocker.Evidence, "only one freshly observed physical host")
			if unique != (restriction != "authored") {
				t.Fatal("confused authored pin with observed coverage", got.Blocker)
			}
			if restriction == "parity" && !strings.Contains(got.Blocker.Evidence, "policy approves 1 physical hosts") {
				t.Fatal("owner's restriction not explained", got.Blocker)
			}
			if !reflect.DeepEqual(before, cloneJob(q.s.Jobs[j.ID])) {
				t.Fatal("diagnostic read changed accepted placement intent")
			}
		})
	}
}

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

func TestActionableBlockerProducerPrecedesPreparation(t *testing.T) {
	q, _ := fixture(t)
	j := &Job{State: "queued", Spec: cpuSpec(1, 1)}
	j.Spec.After = &PieceDependency{Piece: "part"}
	j.Spec.Environment.Name = "cold"
	b := q.actionableBlocker(j)
	if b == nil || b.Owner != "producer" || b.Dependency != "accepted upstream result" {
		t.Fatalf("%+v", b)
	}
}

func TestActionableBlockerNativePreparationIdentity(t *testing.T) {
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	m := machine("worker", "fake")
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
	m.LastSeen = clock.Now()
	m.EnvironmentBuilder = true
	m.EnvironmentReports = []EnvironmentReport{{Key: extensionPreparationKey(e, &m), State: "uploading", FailureKind: "deterministic", Error: "dependency resolution failed", Log: "No solution found"}}
	q.s.Machines[m.ID] = &m
	j := &Job{State: "queued", Spec: Spec{ExtensionCheck: &ExtensionCheckRequest{Work: "extension:" + e.Name}}, Extensions: []Extension{e}}
	check(t, q, m, nil)
	m = *q.s.Machines[m.ID]
	b := q.environmentBlocker(j, &m)
	if b == nil || b.Code != "deterministically_failed" || !strings.Contains(b.Evidence, "No solution found") {
		t.Fatalf("%+v", b)
	}
}

func TestActionableBlockerResolveCurrentMemory(t *testing.T) {
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
}

func TestActionableBlockerKnownSharedPressure(t *testing.T) {
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
}

func TestActionableBlockerNativeResolveQualification(t *testing.T) {
	q, _, _, s := observationFixture(t)
	e := q.s.Extensions[s.Extension]
	r, err := q.Resolve(ResolveRequest{Spec: Spec{Project: s.Project}, Recipe: &e})
	if err != nil {
		t.Fatal(err)
	}
	if r.Blocker == nil || r.Blocker.Owner != "consumer" || !strings.Contains(r.Blocker.Action, "qualify") {
		t.Fatalf("%+v", r.Blocker)
	}
}
