package myce

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
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

func TestNativeQualificationHoldDispatchability(t *testing.T) {
	for _, scenario := range []string{"shared", "failed", "alternative", "ready", "unprepared"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock := fixture(t)
			e, _, m := variantsFixture(t, true)
			e.Variants = e.Variants[1:]
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			r := newExtensionReport(e, &m, "extension:"+e.Name)
			r.Setup = scenario != "unprepared"
			if scenario == "failed" || scenario == "alternative" {
				r.CheckID, r.CheckState = automaticExtensionCheckID(m.ID, r), "failed"
			}
			m.ExtensionReports = []ExtensionReport{r}
			check(t, q, m, nil)
			if scenario == "shared" {
				if err := q.update(func() error {
					q.s.Machines[m.ID].SharedHost = &SharedHostProfile{InteractiveReserveGB: 1}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "alternative" {
				other := m
				other.ID, other.HostID, other.Session = "eligible", "eligible", "eligible-session"
				r = newExtensionReport(e, &other, "extension:"+e.Name)
				r.Setup = true
				other.ExtensionReports = []ExtensionReport{r}
				check(t, q, other, nil)
			}
			s := variantSpec(e, "ci")
			if scenario == "ready" {
				report, err := q.Resolve(ResolveRequest{Spec: s})
				if err != nil || report.Hold == nil || report.Hold.Owner != ownerConsumer ||
					report.Hold.Next != "submit the job without require_target to request native qualification" ||
					!strings.Contains(report.Hold.Reason, "resolve creates no check") ||
					strings.Contains(report.Hold.Reason, "next check-in") ||
					len(q.extensionQualificationPlan(q.s.Machines[m.ID])) != 0 {
					t.Fatalf("resolve promised work with no submitted demand: %+v (%v)", report.Hold, err)
				}
			}
			j := submit(t, q, s)
			clock.Advance(time.Second)
			got, err := q.jobSnapshot(j.ID)
			if err != nil || got.Hold == nil {
				t.Fatal(got, err)
			}
			switch scenario {
			case "shared", "failed":
				if got.Hold.Kind != "no_eligible_host" || !strings.Contains(got.Hold.Reason, "no eligible host") || strings.Contains(got.Hold.Reason, "next check-in") || got.Hold.Next == "wait; nothing to do" {
					t.Fatal(got.Hold)
				}
				if plans := q.extensionQualificationPlan(q.s.Machines[m.ID]); len(plans) != 0 {
					t.Fatal("offered an ineligible check", plans)
				}
			case "alternative":
				if !strings.Contains(got.Hold.Reason, "on eligible at that host's next check-in") {
					t.Fatal(got.Hold)
				}
			case "ready":
				if !strings.Contains(got.Hold.Reason, "next check-in") {
					t.Fatal(got.Hold)
				}
			case "unprepared":
				if strings.Contains(got.Hold.Reason, "next check-in") || !strings.Contains(got.Hold.Reason, "preparation") {
					t.Fatal(got.Hold)
				}
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
			if restriction == "parity" && !strings.Contains(got.Hold.Detail, "approves these machine kinds for this work's production: "+targetLabel(machineTarget(&a))+";") {
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
					want = "no_buildable_worker"
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
	b := q.jobHold(j, false)
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
	for _, priority := range []string{"experiment", "deadline", "production"} {
		for _, checkpoint := range []string{"none", "resumable", "declared", "ephemeral"} {
			for _, unavailable := range []string{"queue", "worker", "both"} {
				t.Run(fmt.Sprintf("%s/%s/%s", priority, checkpoint, unavailable), func(t *testing.T) {
					q, clock := fixture(t)
					m := cpuMachine("worker", "worker", "cpu", 8, 16)
					if checkpoint == "ephemeral" {
						if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
							t.Fatal(err)
						}
						m.Capabilities = append(m.Capabilities, ephemeralCapability, nativeCICapability, "native_isolation_namespace")
					}
					check(t, q, m, nil)
					s := cpuSpec(1, 1)
					s.Priority, s.Resumable = priority, checkpoint == "resumable"
					if checkpoint == "declared" || checkpoint == "ephemeral" {
						s.Disk = &DiskNeeds{CheckpointBytes: 1024}
					}
					if checkpoint == "ephemeral" {
						s.WorkerData = "ephemeral"
						s.Outputs = map[string]string{"out.txt": "results"}
						s.Run.Command = []string{"sh", "-c", "echo done > out.txt"}
					}
					if priority == "deadline" {
						deadline := clock.Now().Add(time.Hour)
						s.Deadline = &deadline
					}
					j := submit(t, q, s)
					fakeCheckpointAvailability(t, q, unavailable == "worker")
					m.CheckpointUnavailable = unavailable != "queue"
					if checkpoint == "none" || checkpoint == "ephemeral" {
						if reply := check(t, q, m, nil); reply.Job == nil || reply.Job.ID != j.ID {
							t.Fatalf("non-checkpointing %s job blocked by %s checkpoint storage", priority, unavailable)
						}
						return
					}
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
					if got.Hold == nil || aggregate == nil || !reflect.DeepEqual(got.Hold, aggregate.Hold) || aggregate.Reason != got.Reason {
						t.Fatalf("GET=%+v %q aggregate=%+v", got.Hold, got.Reason, aggregate)
					}
					if unavailable == "worker" {
						if !strings.Contains(got.Reason, "waiting for saved progress storage") {
							t.Fatalf("worker storage hold changed: %+v %q", got.Hold, got.Reason)
						}
					} else if got.Hold.Kind != "checkpoint_root" || got.Hold.Reason != "saved progress storage is unavailable" || !strings.HasPrefix(got.Reason, got.Hold.Reason) {
						t.Fatalf("queue storage hold changed: %+v %q", got.Hold, got.Reason)
					}
					fakeCheckpointAvailability(t, q, true)
					m.CheckpointUnavailable = false
					clock.Advance(time.Second)
					if reply := check(t, q, m, nil); reply.Job == nil || reply.Job.ID != j.ID {
						t.Fatal("restored storage did not release the checkpoint-dependent job")
					}
				})
			}
		}
	}
}

// Exercise the resolve and public status projections against the same fake
// inventory and frozen command environments. Capacity stays a capacity hold.
func TestHoldNamesCPUCandidateFromEnvironmentEvidence(t *testing.T) {
	for _, coverage := range []string{"declared", "unknown", "missing"} {
		t.Run(coverage, func(t *testing.T) {
			q, _ := fixture(t)
			mac := cpuMachine("darwin-worker", "darwin-host", "cpu", 4, 16)
			mac.CapabilityProfile.OS, mac.CapabilityProfile.Arch = "darwin", "arm64"
			linux := cpuMachine("linux-worker", "linux-host", "cpu", 4, 16)
			linux.CapabilityProfile.Arch = "arm64"
			s := cpuSpec(4, 2)
			s.Targets = []ExecutionTarget{{"darwin", "arm64", "cpu"}}
			first := submit(t, q, s)
			running := check(t, q, mac, nil).Job
			if running == nil || running.ID != first.ID {
				t.Fatal("Darwin capacity fixture did not start", running)
			}
			want, source := "verify target-native environment coverage for linux/arm64/cpu", "available member environment metadata"
			if coverage != "unknown" {
				r := breadthRecipe("osx-arm64")
				want, source = "cover linux/arm64/cpu in the required member environments", "selected member recipes or locks"
				if coverage == "declared" {
					r = breadthRecipe("osx-arm64", "linux-aarch64")
					want, source = "verify the complete workload on linux/arm64/cpu before widening targets", "selected immutable member locks"
				}
				q.environments = EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}
				s.Environment = Environment{Name: "tools"}
				mac.Environments, linux.Environments = []string{r.Key}, []string{r.Key}
			}
			check(t, q, mac, running)
			check(t, q, linux, nil)
			waiting := submit(t, q, s)
			if coverage != "unknown" {
				stored := storedTestJob(t, q, waiting.ID)
				recipe := stored.EnvironmentRecipes["cpu"]
				if recipe.FilesRef == nil || len(recipe.Files) != 0 {
					t.Fatal("fixture must exercise status with lock bytes outside the job record")
				}
			}
			report, err := q.Resolve(ResolveRequest{Spec: s})
			if err != nil || report.Hold == nil || report.Hold.Kind != "capacity" {
				t.Fatalf("wanted a capacity hold plus compatibility advice: %+v %v", report.Hold, err)
			}
			assertCompatibilityAction(t, q, waiting.ID, report, want, source)
		})
	}
}

func TestHoldPreservesDarwinDeveloperToolsRequirement(t *testing.T) {
	q, _ := fixture(t)
	e, _, linux := variantsFixture(t, true)
	e.Variants = e.Variants[1:]
	e.Variants[0].Target = ExecutionTarget{"darwin", "arm64", "cpu"}
	e.Variants[0].Needs.Tools = []string{"xcodebuild", "xcrun"}
	e.Variants[0].Check = []string{"xcrun", "--sdk", "iphonesimulator", "--show-sdk-path"}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	linux.ExtensionReports = nil
	check(t, q, linux, nil)
	s := variantSpec(e, "experiment")
	waiting := submit(t, q, s)
	report, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal(err)
	}
	assertCompatibilityAction(t, q, waiting.ID, report, darwinToolsAction, darwinToolsSource)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	got, err := q.jobSnapshot(waiting.ID)
	if err != nil || got.State != "failed" || got.Finding == nil || got.Finding.Action != darwinToolsAction || !strings.Contains(got.Finding.What, darwinToolsSource) {
		t.Fatalf("terminal status lost the Darwin requirement: %+v, %v", got, err)
	}
}

func assertCompatibilityAction(t *testing.T, q *Queue, id string, report ResolveReport, action, source string) {
	t.Helper()
	if h := report.Hold; h == nil || h.Next != action || !strings.Contains(h.Reason, "source: ") || !strings.Contains(h.Reason, source) || h.RunsNowOn != "" {
		t.Fatalf("resolve hold: %+v", h)
	}
	var out strings.Builder
	if err := WriteResolve(&out, report, false); err != nil || strings.Count(out.String(), "next: ") != 1 || !strings.Contains(out.String(), source) || !strings.Contains(out.String(), action) || strings.Contains(out.String(), "  - ") {
		t.Fatalf("resolve text: %s (%v)", out.String(), err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "compatibility-fixture"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "compatibility-fixture"}
	ctx := context.Background()
	got, err := client.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hold == nil || got.Hold.Next != action || !strings.Contains(JobReason(got), source) || strings.Count(JobReason(got), "next: ") != 1 {
		t.Fatalf("job status: %+v", got.Hold)
	}
	var snapshot Snapshot
	if err := client.Call(ctx, "GET", "/api/status?sections=jobs", nil, &snapshot); err != nil {
		t.Fatal(err)
	}
	for _, j := range snapshot.Jobs {
		if j.ID == id {
			if j.Hold == nil || j.Hold.Next != action || !strings.Contains(j.Hold.Reason, source) {
				t.Fatalf("detailed status: %+v", j.Hold)
			}
			return
		}
	}
	t.Fatal("aggregate status omitted job", id)
}
