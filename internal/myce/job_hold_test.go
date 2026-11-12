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
					q.touchJob(j.ID)
					for i := range q.s.Jobs[j.ID].Extensions {
						q.s.Jobs[j.ID].Extensions[i].ProductionParityRequired = true
					}
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
			if err != nil || got.Hold == nil || got.Hold.Kind != want {
				t.Fatalf("want %s: blocker=%+v error=%v", want, got.Hold, err)
			}
			if scenario == "unqualified" || scenario == "missing_variant" {
				if !strings.Contains(got.Hold.Detail, "linux/amd64/fake") || strings.Contains(got.Hold.Next, a.ID) {
					t.Fatal("coverage advice omitted declared target or selected a host", got.Hold)
				}
			}
			if strings.HasPrefix(scenario, "stale") && strings.Contains(got.Hold.Detail, "only one freshly") {
				t.Fatal("stale inventory claimed unique hardware", got.Hold)
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
				setParityHosts(t, q, s.Project, s.workType(), hostID(&a))
			}
			j := submit(t, q, s)
			before := storedTestJob(t, q, j.ID)
			got, err := q.jobSnapshot(j.ID)
			if err != nil || got.Hold == nil {
				t.Fatal(got, err)
			}
			unique := strings.Contains(got.Hold.Detail, "only one freshly observed physical host")
			if unique != (restriction != "authored") {
				t.Fatal("confused authored pin with observed coverage", got.Hold)
			}
			if restriction == "parity" && !strings.Contains(got.Hold.Detail, "list approves 1 physical hosts") {
				t.Fatal("project's parity restriction not explained", got.Hold)
			}
			after := storedTestJob(t, q, j.ID)
			if !reflect.DeepEqual(before.Spec, after.Spec) || !reflect.DeepEqual(before.PlacementIntent, after.PlacementIntent) {
				t.Fatal("diagnostic read changed frozen spec or authored placement intent")
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
			want := "unprepared"
			if err := q.update(func() error {
				live := q.s.Machines[m.ID]
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
			var aggregate *Hold
			for _, row := range snapshot.Jobs {
				if row.ID == j.ID {
					aggregate = row.Hold
				}
			}
			resolved, err := q.Resolve(ResolveRequest{Spec: s})
			if err != nil {
				t.Fatal(err)
			}
			if got.Hold == nil || got.Hold.Kind != want || !reflect.DeepEqual(got.Hold, aggregate) || !reflect.DeepEqual(got.Hold, resolved.Hold) {
				t.Fatalf("%s: GET=%+v aggregate=%+v resolve=%+v", state, got.Hold, aggregate, resolved.Hold)
			}
			if state == "failed" && !strings.Contains(got.Hold.Detail, "first index") {
				t.Fatal(got.Hold)
			}
			if state != "building" && strings.Contains(got.Hold.Text(), "builder reports building") {
				t.Fatal(got.Hold)
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
	if len(r.Targets) != 1 || !r.Targets[0].Eligible || r.Hold != nil {
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
	if r.Hold == nil || r.Hold.Owner != "consumer" || !strings.Contains(r.Hold.Detail, "lock_file must be") {
		t.Fatalf("%+v", r)
	}
}

func TestActionableBlockerProducerPrecedesPreparation(t *testing.T) {
	q, _ := fixture(t)
	j := &Job{State: "queued", Spec: cpuSpec(1, 1)}
	j.Spec.After = &PieceDependency{Piece: "part"}
	j.Spec.Environment.Name = "cold"
	b := q.jobHold(j)
	if b == nil || b.Owner != "producer" || b.Kind != "upstream" {
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
	b := q.environmentHold(j, &m)
	if b == nil || b.Kind != "deterministically_failed" || !strings.Contains(b.Detail, "No solution found") {
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
	if got.Hold == nil || got.Hold.Kind != "host_memory_headroom" || !reflect.DeepEqual(got.Hold, r.Hold) {
		t.Fatalf("GET=%+v resolve=%+v", got.Hold, r.Hold)
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
	// Owner pressure and RAM together are mixed capacity causes; detail names both.
	if got.Hold == nil || got.Hold.Kind != "capacity" || !strings.Contains(got.Hold.Detail, "memory pressure") {
		t.Fatalf("%+v", got.Hold)
	}
}

func TestActionableBlockerNativeResolveQualification(t *testing.T) {
	q, _, _, s := observationFixture(t)
	e := q.s.Extensions[s.Extension]
	r, err := q.Resolve(ResolveRequest{Spec: Spec{Project: s.Project}, Recipe: &e})
	if err != nil {
		t.Fatal(err)
	}
	if r.Hold == nil || r.Hold.Owner != "consumer" || !strings.Contains(r.Hold.Next, "qualify") {
		t.Fatalf("%+v", r.Hold)
	}
}

// An unreachable checkpoint root is the scheduler's hold, so the hold, its
// reason and every read agree instead of status overriding the text.
func TestCheckpointRootHold(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("worker", "worker", "cpu", 8, 16)
	check(t, q, m, nil)
	s := cpuSpec(1, 1)
	j := submit(t, q, s)
	fakeCheckpointAvailability(t, q, false)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("checkpoint-dependent job started without its root")
	}
	got, err := q.jobSnapshot(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var aggregate *Job
	for _, row := range snapshot.Jobs {
		if row.ID == j.ID {
			aggregate = row
		}
	}
	if got.Hold == nil || got.Hold.Kind != "checkpoint_root" || got.Reason != "saved progress storage is unavailable" || aggregate == nil || !reflect.DeepEqual(got.Hold, aggregate.Hold) || aggregate.Reason != got.Reason {
		t.Fatalf("GET=%+v %q aggregate=%+v", got.Hold, got.Reason, aggregate)
	}
	fakeCheckpointAvailability(t, q, true)
	q.mu.Lock()
	wait := q.waitHold(q.s.Jobs[j.ID])
	q.mu.Unlock()
	if wait.Kind == "checkpoint_root" {
		t.Fatalf("restored root kept its hold: %+v", wait)
	}
}

// A job authored for one platform waits on its busy host while a host of
// another platform has room. The hold names that platform (never the host),
// so the consumer widens targets instead of steering to a box. Platforms
// without room now, and jobs without authored targets, get no advice.
func TestHoldNamesPlatformWithRoomNow(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	mac := cpuMachine("m-1", "h-1", "cpu", 4, 16)
	mac.CapabilityProfile.OS, mac.CapabilityProfile.Arch = "darwin", "arm64"
	roomy := cpuMachine("m-2", "h-2", "cpu", 4, 16)
	roomy.CapabilityProfile.Arch = "arm64"
	tight := cpuMachine("m-3", "h-3", "cpu", 4, 1) // linux/amd64 without memory for the job
	darwinOnly := cpuSpec(4, 2)
	darwinOnly.Targets = []ExecutionTarget{{"darwin", "arm64", "cpu"}}
	queued := submit(t, q, darwinOnly)
	running := check(t, q, mac, nil).Job
	if running == nil || running.ID != queued.ID {
		t.Fatal("darwin job not placed on the darwin host", running)
	}
	check(t, q, mac, running)
	check(t, q, roomy, nil)
	check(t, q, tight, nil)
	waiting := submit(t, q, darwinOnly)
	got, err := q.jobSnapshot(waiting.ID)
	if err != nil || got.State != "queued" || got.Hold == nil {
		t.Fatalf("darwin job should wait with a hold: %+v %v", got.Hold, err)
	}
	if got.Hold.RunsNowOn != "linux/arm64/cpu" {
		t.Fatalf("hold should name only the platform with room now: %+v", got.Hold)
	}
	// Default status prints reason and next only, so the advice rides in next.
	if next := got.Hold.Next; !strings.Contains(next, "would run now on linux/arm64/cpu") || strings.Contains(next, "h-2") || strings.Contains(next, "m-2") {
		t.Fatal("next must name the platform, not the host:", next)
	}
	free := cpuSpec(1, 1)
	free.Project = "other"
	unrestricted, err := q.jobSnapshot(submit(t, q, free).ID)
	if err != nil || unrestricted.Hold != nil && unrestricted.Hold.RunsNowOn != "" {
		t.Fatalf("a job without authored targets needs no platform advice: %+v %v", unrestricted.Hold, err)
	}
}
