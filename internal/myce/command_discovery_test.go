package myce

import (
	"context"
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

// Cached native preparation is a fixture, not proof that Pixi installs on every
// OS. The command, API, transport, keyed replay and exact release are real.
func TestCommandDiscoveryRealCPUResult(t *testing.T) {
	root := t.TempDir()
	r := breadthRecipe("linux-64", "linux-aarch64", "osx-64", "osx-arm64")
	s := discoverySpec()
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
	dev, err := startDev(DevConfig{Workers: 1, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "store"), Token: "discovery-fixture", EnvironmentConfig: EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}, Interval: 10 * time.Millisecond}, func(*Queue) {
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
