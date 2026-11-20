package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func discoverySpec() Spec {
	s := cpuSpec(1, 1)
	s.Portable, s.RequireTarget = true, true
	s.Environment = Environment{Name: "tools"}
	return s
}

func discoveryCatalog(t *testing.T, q *Queue, r EnvironmentRecipe) {
	t.Helper()
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
}

func TestCommandDiscoveryIntersectionAndUnknownEvidence(t *testing.T) {
	s := discoverySpec()
	r := breadthRecipe("linux-64", "osx-arm64", "win-64")
	main := map[string]EnvironmentRecipe{"cpu": r}
	side := map[string]map[string]EnvironmentRecipe{"helper": {"cpu": breadthRecipe("osx-arm64", "win-64")}}
	d, err := deriveCommandDiscovery(s, main, side)
	if err != nil || !slices.Equal(d.Targets, []ExecutionTarget{{"darwin", "arm64", "cpu"}}) {
		t.Fatal(d, err)
	}
	for _, r := range []EnvironmentRecipe{{Backend: "uv", Key: "unknown"}, {Backend: "pixi", Key: "missing"}, {Binary: "program", Key: "binary"}} {
		if _, err := deriveCommandDiscovery(s, main, map[string]map[string]EnvironmentRecipe{"helper": {"cpu": r}}); err == nil {
			t.Fatal("unknown member granted coverage", r.Backend)
		}
	}
	s.Targets = []ExecutionTarget{{"linux", "amd64", "cpu"}}
	if d, err := deriveCommandDiscovery(s, main, side); err != nil || d != nil {
		t.Fatal("explicit target reinterpreted", d, err)
	}
	s.Targets, s.Portable = nil, false
	if d, err := deriveCommandDiscovery(s, nil, nil); err != nil || d != nil {
		t.Fatal("legacy omitted-target command reinterpreted", d, err)
	}
}

func TestCommandDiscoveryFailureIsolationReplayAndChangedRecipe(t *testing.T) {
	q, clock := fixture(t)
	r := breadthRecipe("linux-64", "osx-arm64")
	discoveryCatalog(t, q, r)
	linux, darwin := cpuMachine("linux", "linux", "cpu", 8, 32), cpuMachine("darwin", "darwin", "cpu", 8, 32)
	darwin.CapabilityProfile.OS, darwin.CapabilityProfile.Arch = "darwin", "arm64"
	// Control builder availability rather than relying on a placement tie.
	linux.EnvironmentBuilder = true
	check(t, q, linux, nil)
	check(t, q, darwin, nil)
	s := discoverySpec()
	s.SubmissionKey = "discovery-original"
	j := submit(t, q, s)
	if j.CommandDiscovery == nil || len(j.CommandDiscovery.Targets) != 2 || len(j.Spec.Targets) != 0 {
		t.Fatal(j)
	}
	first := check(t, q, linux, nil).EnvironmentPlan
	if len(first) != 1 || first[0].Target == nil || first[0].Target.OS != "linux" || first[0].Key == r.Key {
		t.Fatal(first)
	}
	if err := q.AuthorizeLaunch(*first[0].Launch); err != nil {
		t.Fatal(err)
	}
	linux.EnvironmentReports = []EnvironmentReport{{Key: first[0].Key, Attempt: first[0].Launch.Attempt, State: "failed", FailureKind: "deterministic", Error: "No solution found"}}
	if got := check(t, q, linux, nil); len(got.EnvironmentPlan) != 0 || got.Job != nil {
		t.Fatal("failed identity retried", got)
	}
	requireState(t, q, j.ID, "queued")
	clock.Advance(time.Second)
	darwin.EnvironmentBuilder = true
	second := check(t, q, darwin, nil).EnvironmentPlan
	if len(second) != 1 || second[0].Key == first[0].Key || second[0].Target.OS != "darwin" {
		t.Fatal(second)
	}
	if err := q.AuthorizeLaunch(*second[0].Launch); err != nil {
		t.Fatal(err)
	}
	darwin.Environments = []string{second[0].Key}
	darwin.EnvironmentReports = []EnvironmentReport{{Key: second[0].Key, Attempt: second[0].Launch.Attempt, State: "ready"}}
	assigned := check(t, q, darwin, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("viable target failed to execute", assigned)
	}
	finish(t, q, assigned, true)
	darwin.EnvironmentBuilder = false
	check(t, q, darwin, nil)
	if replay := submit(t, q, s); replay.ID != j.ID || !replay.SubmissionReused {
		t.Fatal(replay)
	}
	// Frozen original intent survives catalog change; only the new identity reopens.
	r.Files["pixi.toml"] = []byte("[workspace]\nname = 'changed'\n")
	r.Key = identityHash(r)
	discoveryCatalog(t, q, r)
	if replay := submit(t, q, s); replay.ID != j.ID {
		t.Fatal(replay)
	}
	s.SubmissionKey = "discovery-changed"
	changed := submit(t, q, s)
	plan := check(t, q, linux, nil).EnvironmentPlan
	if len(plan) != 1 || plan[0].Key == first[0].Key || plan[0].Launch.Demand != changed.ID {
		t.Fatal("changed recipe kept old negative evidence", plan)
	}
	if !q.environmentReferenced(context.Background(), plan[0].Key) {
		t.Fatal("target cache not protected")
	}
	if err := q.Cancel(changed.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.AuthorizeLaunch(*plan[0].Launch); err == nil {
		t.Fatal("cached discovery offer launched after cancellation without a heartbeat")
	}
	if got := check(t, q, linux, nil); got.Job != nil || len(got.EnvironmentPlan) != 0 {
		t.Fatal("cancelled discovery still offered", got)
	}
	// Persisted server marker and frozen recipes must survive restart/key replay.
	path, root := q.dbPathForTest(t), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if replay := submit(t, reopened, s); replay.ID != changed.ID || replay.CommandDiscovery == nil || replay.State != "cancelled" {
		t.Fatal(replay)
	}
}

func TestCommandDiscoveryStartedPreparationSurvivesSuspicion(t *testing.T) {
	q, clock := fixture(t)
	discoveryCatalog(t, q, breadthRecipe("linux-64"))
	m := cpuMachine("worker", "linux", "cpu", 8, 32)
	m.EnvironmentBuilder = true
	check(t, q, m, nil)
	j := submit(t, q, discoverySpec())
	plan := check(t, q, m, nil).EnvironmentPlan
	if len(plan) != 1 || plan[0].Launch == nil || plan[0].Launch.Demand != j.ID {
		t.Fatal("missing discovery preparation", plan)
	}
	launch := *plan[0].Launch
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	started := q.s.PreparationPlans[launch.Attempt].StartedAt
	clock.Advance(q.workerArrivalAllowance(q.s.Machines[m.ID]))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if !q.workerSuspect(q.s.Machines[m.ID], clock.Now()) {
		t.Fatal("fixture did not become suspect")
	}
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal("discovery freshness interrupted started preparation", err)
	}
	if got := q.s.PreparationPlans[launch.Attempt]; got.StartedAt == nil || !got.StartedAt.Equal(*started) {
		t.Fatal("continuation renewed discovery custody", got)
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
		t.Fatal("cancelled discovery continued preparation", err)
	}
}

func TestCommandDiscoveryUnpinningPreservesCoverageAndParity(t *testing.T) {
	for _, production := range []bool{false, true} {
		q, _ := fixture(t)
		r := breadthRecipe("linux-64")
		if production {
			r = breadthRecipe("linux-64", "osx-arm64")
		}
		discoveryCatalog(t, q, r)
		for _, os := range []string{"linux", "darwin"} {
			m := cpuMachine(os, os, "cpu", 8, 32)
			m.CapabilityProfile.OS = os
			if os == "darwin" {
				m.CapabilityProfile.Arch = "arm64"
			}
			m.EnvironmentBuilder = true
			check(t, q, m, nil)
		}
		s := discoverySpec()
		s.AllowedMachines = []string{"linux"}
		if production {
			s.Class, s.Priority = "production", ""
		}
		before := identityHash(q.s)
		report, err := q.Resolve(ResolveRequest{Spec: s})
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range report.Findings {
			if f.Field == "allowed_machines" && strings.Contains(f.Problem, "could also run it") {
				t.Fatalf("unpinning bypassed discovery coverage/parity: %+v", f)
			}
		}
		if identityHash(q.s) != before {
			t.Fatal("unpinning advice mutated demand")
		}
	}
}

func TestCommandDiscoveryReadOnlyProductionAndMaintenance(t *testing.T) {
	q, clock := fixture(t)
	r := breadthRecipe("linux-64", "osx-arm64", "win-64")
	discoveryCatalog(t, q, r)
	m := cpuMachine("worker", "host", "cpu", 8, 32)
	m.EnvironmentBuilder = true
	check(t, q, m, nil)
	s := discoverySpec()
	before := identityHash(q.s)
	report, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil || len(report.Targets) != 1 || !report.Targets[0].Eligible || report.Targets[0].Environment.State != "prepare-needed" || identityHash(q.s) != before {
		t.Fatal(report, err)
	}
	if got := portabilityRow(t, *report.Portability, ExecutionTarget{"windows", "amd64", "cpu"}); !strings.Contains(got.Runtime, "unsupported") || !strings.HasPrefix(got.AuthoredEligibility, "excluded:") {
		t.Fatal(got)
	}
	clock.Advance(q.machineLossTimeout(m.ID) + time.Second)
	report, err = q.Resolve(ResolveRequest{Spec: s})
	if err != nil || report.Targets[0].Eligible {
		t.Fatal("stale evidence admitted", report, err)
	}
	check(t, q, m, nil)
	s.Class, s.Priority = "production", ""
	report, err = q.Resolve(ResolveRequest{Spec: s})
	if err != nil || report.Targets[0].Eligible || !strings.Contains(report.Targets[0].Reason, "MY-19") {
		t.Fatal("production widened without exact parity", report, err)
	}
	if _, err := q.Submit(s); err == nil {
		t.Fatal("production discovery accepted without parity")
	}
	policy := defaultPriorityPolicy()
	policy.Tiers[0].Selectors = append(policy.Tiers[0].Selectors, "*/customer")
	if _, err := q.SetPriorityPolicy(0, policy); err != nil {
		t.Fatal(err)
	}
	s.Class = "customer"
	report, err = q.Resolve(ResolveRequest{Spec: s})
	if err != nil || report.Targets[0].Eligible || !strings.Contains(report.Targets[0].Reason, "MY-19") {
		t.Fatal("policy-promoted production bypassed parity", report, err)
	}
	s.Class = "experiment"
	j := submit(t, q, s)
	if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "drain", DrainOwner: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if got := check(t, q, m, nil); got.Job != nil || len(got.EnvironmentPlan) != 0 {
		t.Fatal("maintenance bypassed", got)
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
}

func TestCommandDiscoveryPreparesEveryMemberAndIsolatesSidecarFailure(t *testing.T) {
	q, _ := fixture(t)
	main, helper := breadthRecipe("linux-64", "osx-arm64"), breadthRecipe("osx-arm64")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": main}, "helper": {"cpu": helper}}}); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("worker", "host", "cpu", 8, 32)
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "darwin", "arm64"
	m.EnvironmentBuilder = true
	check(t, q, m, nil)
	s := discoverySpec()
	s.Run, s.Environment = RunSpec{}, Environment{}
	s.Execution = &ExecutionSpec{Main: ExecutionMember{Command: []string{"main"}, Environment: "tools", MemoryGB: 0.25}, Sidecars: []NativeSidecar{{Name: "helper", Environment: "helper", Command: []string{"helper"}, MemoryGB: 0.25}}}
	j := submit(t, q, s)
	if len(j.CommandDiscovery.Targets) != 1 || j.CommandDiscovery.Targets[0].OS != "darwin" {
		t.Fatal(j.CommandDiscovery)
	}
	first := check(t, q, m, nil).EnvironmentPlan
	if len(first) != 1 {
		t.Fatal(first)
	}
	if err := q.AuthorizeLaunch(*first[0].Launch); err != nil {
		t.Fatal(err)
	}
	m.Environments = []string{first[0].Key}
	m.EnvironmentReports = []EnvironmentReport{{Key: first[0].Key, Attempt: first[0].Launch.Attempt, State: "ready"}}
	reply := check(t, q, m, nil)
	if reply.Job != nil || len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key == first[0].Key {
		t.Fatal("main masked unprepared helper", reply)
	}
	second := reply.EnvironmentPlan[0]
	if err := q.AuthorizeLaunch(*second.Launch); err != nil {
		t.Fatal(err)
	}
	m.EnvironmentReports = []EnvironmentReport{{Key: second.Key, Attempt: second.Launch.Attempt, State: "failed", FailureKind: "deterministic", Error: "No solution found"}}
	if got := check(t, q, m, nil); got.Job != nil || len(got.EnvironmentPlan) != 0 {
		t.Fatal("failed helper retried or launched", got)
	}
	requireState(t, q, j.ID, "queued")
	if reason := q.environmentFailureWaitingReason(q.s.Jobs[j.ID], map[string]*Machine{m.ID: &m}); !strings.Contains(reason, "No solution found") {
		t.Fatal("sidecar failure hidden", reason)
	}
	m.Environments = nil // Main eviction must not hide the known failed helper.
	if got := check(t, q, m, nil); got.Job != nil || len(got.EnvironmentPlan) != 0 {
		t.Fatal("rebuilding a failed candidate after main eviction", got)
	}
}

func projectDiscoveryEnvironment(platforms ...string) Environment {
	files := breadthRecipe(platforms...).Files
	files["pixi.lock"] = []byte(strings.Replace(string(files["pixi.lock"]), "  selected:", "  default:", 1))
	return Environment{Backend: "pixi", LockFile: "env/pixi.lock", Platforms: []string{"cpu"}, Files: files}
}

func TestCommandDiscoveryProjectPixiTargets(t *testing.T) {
	fakeWorkerPixi(t, workerPixiVersion())
	q, clock := fixture(t)
	want := []ExecutionTarget{{"darwin", "arm64", "cpu"}, {"linux", "amd64", "cpu"}, {"linux", "arm64", "cpu"}}
	outside := ExecutionTarget{"darwin", "amd64", "cpu"}
	machines := map[ExecutionTarget]Machine{}
	for _, target := range append(slices.Clone(want), outside) {
		m := cpuMachine(target.OS+"-"+target.Arch, target.OS+"-"+target.Arch, "cpu", 8, 32)
		m.CapabilityProfile.OS, m.CapabilityProfile.Arch = target.OS, target.Arch
		m.EnvironmentBuilder = true
		check(t, q, m, nil)
		machines[target] = m
	}
	platforms := []string{"linux-64", "linux-aarch64", "osx-arm64"}
	discoveryCatalog(t, q, breadthRecipe(platforms...))
	projectEnv := projectDiscoveryEnvironment(platforms...)
	report := func(s Spec, expected []ExecutionTarget) ResolveReport {
		t.Helper()
		r, err := q.Resolve(ResolveRequest{Spec: s})
		if err != nil || len(r.Targets) != 4 || len(r.Findings) != 0 {
			t.Fatalf("resolve targets: %+v, findings: %+v, err: %v", r.Targets, r.Findings, err)
		}
		for _, row := range r.Targets {
			if row.Eligible != slices.Contains(expected, row.Target) {
				t.Fatalf("want eligible %v, got %+v", expected, row)
			}
		}
		return r
	}

	s := discoverySpec()
	// The same named-recipe command before and after dropping its target rows.
	s.Targets = slices.Clone(want)
	report(s, want)
	s.Targets = nil
	report(s, want)

	// A project carries one lock and its companion manifest with no catalog
	// name. The authored target rows are gone, but the eligible set is the same.
	s.Environment = projectEnv
	r := report(s, want)
	for _, row := range r.Targets {
		if row.Target == outside && !strings.Contains(row.Reason, "selected immutable lock") {
			t.Fatalf("uncovered target lacked lock explanation: %+v", row)
		}
	}
	j := submit(t, q, s)
	if j.CommandDiscovery == nil || !slices.Equal(j.CommandDiscovery.Targets, want) || len(j.Spec.Targets) != 0 || j.EnvironmentLock == nil || len(j.EnvironmentLock.SHA256["pixi.lock"]) != 64 {
		t.Fatalf("bad frozen discovery or lock: %+v, %+v", j.CommandDiscovery, j.EnvironmentLock)
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	s.Targets = []ExecutionTarget{want[1]}
	report(s, s.Targets)
	s.Targets = nil

	// A separately managed sidecar narrows the project's main lock.
	s.Run.MemoryGB = 0.25
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", Environment: "tools", Command: []string{"helper"}, MemoryGB: 0.25}}
	discoveryCatalog(t, q, breadthRecipe("linux-64", "linux-aarch64"))
	report(s, want[1:])

	// Another lock environment cannot stand in for the missing default one.
	unknown := s
	unknown.Environment.Files = breadthRecipe(platforms...).Files
	r, err := q.Resolve(ResolveRequest{Spec: unknown})
	if err != nil || len(r.Findings) != 1 || r.Findings[0].Field != "portable" || !strings.Contains(r.Findings[0].Problem, "unknown member lock coverage") {
		t.Fatalf("unknown coverage: %+v, %v", r.Findings, err)
	}
	if _, err := q.Submit(unknown); err == nil || !strings.Contains(err.Error(), "declare explicit targets") {
		t.Fatalf("unknown coverage did not require explicit targets: %v", err)
	}

	s.Class, s.Priority = "production", ""
	r = report(s, nil)
	if !slices.ContainsFunc(r.Targets, func(row ResolveTarget) bool {
		return row.Target == want[1] && strings.Contains(row.Reason, "MY-19")
	}) {
		t.Fatalf("production resolve lost parity reason: %+v", r.Targets)
	}
	if _, err := q.Submit(s); err == nil || !strings.Contains(err.Error(), "MY-19") {
		t.Fatalf("production crossed the discovery parity boundary: %v", err)
	}

	s.Class = "experiment"
	s.Run.MemoryGB, s.Run.Sidecars = 0, nil
	amd64, arm64 := machines[want[1]], machines[want[2]]
	// Keep only the first builder available, then introduce the fallback.
	for _, target := range []ExecutionTarget{want[0], want[2]} {
		m := machines[target]
		m.EnvironmentBuilder = false
		check(t, q, m, nil)
	}
	j = submit(t, q, s)
	first := check(t, q, amd64, nil).EnvironmentPlan
	if len(first) != 1 || first[0].Target == nil || *first[0].Target != want[1] {
		t.Fatalf("expected amd64 preparation: %+v", first)
	}
	if err := q.AuthorizeLaunch(*first[0].Launch); err != nil {
		t.Fatal(err)
	}
	amd64.EnvironmentReports = []EnvironmentReport{{Key: first[0].Key, Attempt: first[0].Launch.Attempt, State: "failed", FailureKind: "deterministic", Error: "No solution found"}}
	if reply := check(t, q, amd64, nil); reply.Job != nil || len(reply.EnvironmentPlan) != 0 {
		t.Fatal("failed preparation retried", reply)
	}
	clock.Advance(time.Second)
	second := check(t, q, arm64, nil).EnvironmentPlan
	if len(second) != 1 || second[0].Target == nil || *second[0].Target != want[2] || second[0].Key == first[0].Key {
		t.Fatalf("expected independent arm64 preparation: %+v", second)
	}
	if err := q.AuthorizeLaunch(*second[0].Launch); err != nil {
		t.Fatal(err)
	}
	arm64.Environments = []string{second[0].Key}
	arm64.EnvironmentReports = []EnvironmentReport{{Key: second[0].Key, Attempt: second[0].Launch.Attempt, State: "ready"}}
	if assigned := check(t, q, arm64, nil).Job; assigned == nil || assigned.ID != j.ID {
		t.Fatal("viable target failed to execute", assigned)
	} else {
		finish(t, q, assigned, true)
	}
}

// Cached native preparation is a fixture, not proof that Pixi installs on every
// OS. The command, API, transport, keyed replay and exact release are real.
func TestCommandDiscoveryRealCPUResult(t *testing.T) {
	t.Run("named", func(t *testing.T) { testCommandDiscoveryRealCPUResult(t, false) })
	t.Run("project_pixi", func(t *testing.T) { testCommandDiscoveryRealCPUResult(t, true) })
}

func testCommandDiscoveryRealCPUResult(t *testing.T, inline bool) {
	t.Helper()
	root := t.TempDir()
	platforms := []string{"linux-64", "linux-aarch64", "osx-64", "osx-arm64"}
	r := breadthRecipe(platforms...)
	s := discoverySpec()
	catalog := EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}
	if inline {
		fakeWorkerPixi(t, workerPixiVersion())
		s.Environment = projectDiscoveryEnvironment(platforms...)
		catalog = EnvironmentCatalog{}
		recipes, err := catalog.resolveEnvironment(s.Environment)
		if err != nil {
			t.Fatal(err)
		}
		r = recipes["cpu"]
	}
	s.SubmissionKey = "discovered-result"
	s.Run.Command = []string{"sh", "-c", "printf 'discovered-cpu-ok\\n' > result.txt"}
	s.Outputs = map[string]string{"result.txt": "results"}
	s.Needs.MemoryGB = 0.125
	d, err := deriveCommandDiscovery(s, map[string]EnvironmentRecipe{"cpu": r}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := &Machine{GPU: "cpu", CapabilityProfile: &CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH}}
	recipe, _ := commandEnvironmentRecipe(&Job{CommandDiscovery: d}, map[string]EnvironmentRecipe{"cpu": r}, m)
	cache := filepath.Join(root, "workers", "fake-1", "environment-cache", recipe.Key)
	dev, err := startTestDev(t, DevConfig{Workers: 1, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "store"), Token: "discovery-fixture", EnvironmentConfig: catalog, Interval: 10 * time.Millisecond}, func(*Queue) {
		// Seed fixture readiness after the ordinary state-root safety gate,
		// before the dev workers start; never disguise existing state as fresh.
		if err := os.MkdirAll(cache, 0700); err != nil {
			t.Fatal(err)
		}
		if err := saveWorkerJSON(cache, "ready.json", EnvironmentReport{Key: recipe.Key, State: "ready"}); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()
	_, input := retentionJobInput(t, dev.Client(), "portable source fixture\n")
	s.Files = map[string]FileRef{"source.txt": input}
	// startDev returns before the worker's first inventory report. Preserve
	// require_target by waiting for observable admission, not an arbitrary delay.
	await(t, func() bool {
		report, err := dev.Queue.Resolve(ResolveRequest{Spec: s})
		return err == nil && slices.ContainsFunc(report.Targets, func(target ResolveTarget) bool {
			return target.Eligible && target.Environment != nil && target.Environment.State == "ready"
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	j, err := dev.Client().Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := dev.Client().Wait(ctx, j.ID)
	if err != nil || completed.State != "succeeded" {
		t.Fatal(completed, err)
	}
	dest := filepath.Join(root, "accepted.txt")
	if err := dev.Client().DownloadResult(ctx, j.ID, "result.txt", dest); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(dest); err != nil || string(b) != "discovered-cpu-ok\n" {
		t.Fatalf("accepted bytes %q %v", b, err)
	}
	await(t, func() bool {
		current, err := dev.Client().GetJob(ctx, j.ID)
		return err == nil && current.ReleaseConfirmed
	})
	if replay, err := dev.Client().Submit(ctx, s); err != nil || replay.ID != j.ID || !replay.SubmissionReused {
		t.Fatal(replay, err)
	}
}

func TestCommandDiscoveryPreparesAheadOfJobMemory(t *testing.T) {
	q, clock := fixture(t)
	discoveryCatalog(t, q, breadthRecipe("linux-64"))
	m := cpuMachine("builder", "builder", "cpu", 8, 32)
	m.EnvironmentBuilder = true
	check(t, q, m, nil)
	s := discoverySpec()
	s.Needs.HostMemoryGB = 8
	j := submit(t, q, s)
	m.HostAvailableMemoryGB = 1
	reply := check(t, q, m, nil)
	if reply.Job != nil || len(reply.EnvironmentPlan) != 1 {
		t.Fatal("discovery build depended on useful job admission", reply)
	}
	p := reply.EnvironmentPlan[0]
	if p.Target == nil || p.Target.OS != "linux" || p.Launch.Demand != j.ID {
		t.Fatal("lost target or demand", p)
	}
	if err := q.AuthorizeLaunch(*p.Launch); err != nil {
		t.Fatal(err)
	}
	m.EnvironmentReports = []EnvironmentReport{{Key: p.Key, Attempt: p.Launch.Attempt, State: "ready"}}
	m.Environments = []string{p.Key}
	m.HostAvailableMemoryGB = 32
	clock.Advance(time.Second)
	if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("prepared work did not run", got)
	}
}
