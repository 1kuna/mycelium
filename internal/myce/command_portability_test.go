package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestCommandPortabilityExcludedEnvironmentCandidates(t *testing.T) {
	q, c := fixture(t)
	r := breadthRecipe("linux-64", "osx-arm64", "win-64")
	q.environments = EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}
	linux := ExecutionTarget{"linux", "amd64", "cpu"}
	darwin := ExecutionTarget{"darwin", "arm64", "cpu"}
	for i, target := range []ExecutionTarget{linux, darwin, {"freebsd", "riscv64", "cpu"}} {
		m := machine([]string{"linux-worker", "darwin-worker", "unknown-worker"}[i], "cpu")
		m.LastSeen = c.Now()
		m.CapabilityProfile.OS, m.CapabilityProfile.Arch = target.OS, target.Arch
		m.EnvironmentBuilder = true
		if target == linux {
			m.Environments = []string{r.Key}
		}
		q.s.Machines[m.ID] = &m
	}
	s := cpuResolveSpec()
	s.Targets, s.Portable, s.RequireTarget = []ExecutionTarget{linux}, true, true
	s.Environment = Environment{Name: "tools"}
	before, specBefore := identityHash(q.s), identityHash(s)
	p, err := q.Portability(PortabilityRequest{Spec: &s})
	if err != nil {
		t.Fatal(err)
	}
	d := portabilityRow(t, p, darwin)
	if d.AuthoredEligibility != "excluded by targets" || !strings.HasPrefix(d.EnvironmentCoverage, "declared:") || d.Declared || d.Hosts[0].Builds["main"] != "prepare-needed" || !strings.Contains(d.Hosts[0].Qualification, "not applicable") || d.Hosts[0].Admission == "can admit now" {
		t.Fatalf("authored exclusion conflated with dependencies/preparation: %+v", d)
	}
	if !strings.Contains(strings.Join(d.Actions, " "), "whole-payload portability") || !strings.Contains(d.Parity, "owner-accepted") {
		t.Fatal(d)
	}
	l := portabilityRow(t, p, linux)
	if l.AuthoredEligibility != "included" || l.Hosts[0].Builds["main"] != "ready" {
		t.Fatal(l)
	}
	w := portabilityRow(t, p, ExecutionTarget{"windows", "amd64", "cpu"})
	if w.Observed || !strings.HasPrefix(w.Runtime, "unsupported:") {
		t.Fatal(w)
	}
	u := portabilityRow(t, p, ExecutionTarget{"freebsd", "riscv64", "cpu"})
	if !strings.HasPrefix(u.EnvironmentCoverage, "missing:") || !strings.HasPrefix(u.Runtime, "unsupported:") {
		t.Fatal(u)
	}
	resolved, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil || resolved.Portability == nil {
		t.Fatalf("resolve missing breadth: %+v %v", resolved, err)
	}
	if got := portabilityRow(t, *resolved.Portability, darwin); got.EnvironmentCoverage != d.EnvironmentCoverage || got.AuthoredEligibility != d.AuthoredEligibility {
		t.Fatal(got)
	}
	var text bytes.Buffer
	if err := WriteResolve(&text, resolved, false); err != nil || !strings.Contains(text.String(), "authored=excluded by targets") {
		t.Fatalf("text report: %s %v", text.String(), err)
	}
	if identityHash(q.s) != before || identityHash(s) != specBefore {
		t.Fatal("discovery mutated queue, submission intent, preparation or reservations")
	}
	// Native qualification is not required by plain-command require_target.
	j := &Job{Spec: s, EnvironmentRecipes: map[string]EnvironmentRecipe{"cpu": r}}
	j.PlacementIntent = submissionPlacementIntent(s, j)
	q.recordPlacementDecision(j, q.s.Machines["linux-worker"])
	if j.PlacementIntent.Qualification != "not_requested" || slices.Contains(j.Placement.LimitedBy, "qualification") {
		t.Fatal(j.PlacementIntent, j.Placement)
	}
	// New inventory evidence changes advice without changing authored intent.
	c.Advance(time.Minute + time.Second)
	p, err = q.Portability(PortabilityRequest{Spec: &s})
	if err != nil || portabilityRow(t, p, darwin).Hosts[0].Admission != "unknown: inventory stale" {
		t.Fatal(p, err)
	}
}

func TestCommandPortabilityIntersectsEveryMember(t *testing.T) {
	q, _ := fixture(t)
	// Inventory keeps a target observable when every selected recipe's
	// metadata becomes unavailable; unknown locks cannot invent target rows.
	m := cpuMachine("darwin-worker", "fixture-host", "cpu", 8, 32)
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "darwin", "arm64"
	check(t, q, m, nil)
	main := breadthRecipe("linux-64", "osx-arm64")
	side := breadthRecipe("linux-64")
	q.environments = EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": main}, "helper": {"cpu": side}}}
	s := cpuResolveSpec()
	s.Environment = Environment{Name: "tools"}
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", Environment: "helper", Command: []string{"helper"}}}
	s.Targets, s.Portable = []ExecutionTarget{{OS: "linux", Platform: "cpu"}}, true
	p, err := q.Portability(PortabilityRequest{Spec: &s})
	if err != nil {
		t.Fatal(err)
	}
	d := portabilityRow(t, p, ExecutionTarget{"darwin", "arm64", "cpu"})
	if len(d.Environments) != 2 || !strings.HasPrefix(d.EnvironmentCoverage, "missing:") {
		t.Fatalf("main lock masked sidecar restriction: %+v", d)
	}
	// A managed sidecar cannot manufacture dependency coverage for a main
	// command using unverified installed tools.
	s.Environment = Environment{}
	p, err = q.Portability(PortabilityRequest{Spec: &s})
	l := portabilityRow(t, p, ExecutionTarget{"linux", "amd64", "cpu"})
	if err != nil || !strings.HasPrefix(l.EnvironmentCoverage, "unknown:") || len(l.Environments) != 2 || strings.Contains(strings.Join(l.Actions, " "), "Dependency candidate only") {
		t.Fatalf("sidecar hid unknown main dependencies: %+v %v", l, err)
	}
	resolved, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil || resolved.Portability == nil || !strings.HasPrefix(portabilityRow(t, *resolved.Portability, l.Target).EnvironmentCoverage, "unknown:") {
		t.Fatal(resolved, err)
	}
	s.Environment = Environment{Name: "tools"}
	// Unknown coverage never turns into a successful dependency candidate.
	q.environments.Environments["helper"]["cpu"] = EnvironmentRecipe{Backend: "uv", LockFile: "requirements.txt", Files: map[string][]byte{"requirements.txt": []byte("# pinned\n")}}
	p, err = q.Portability(PortabilityRequest{Spec: &s})
	if err != nil || !strings.HasPrefix(portabilityRow(t, p, ExecutionTarget{"darwin", "arm64", "cpu"}).EnvironmentCoverage, "unknown:") {
		t.Fatal(p, err)
	}
	// Missing metadata is visible without reading any caller-selected path.
	s.Environment = Environment{LockFile: "/private/must-not-read"}
	p, err = q.Portability(PortabilityRequest{Spec: &s})
	b, _ := json.Marshal(p)
	if err != nil || bytes.Contains(b, []byte("/private/")) {
		t.Fatalf("advice read or exposed inline path: %s %v", b, err)
	}
	if got := portabilityRow(t, p, ExecutionTarget{"darwin", "arm64", "cpu"}); !strings.HasPrefix(got.EnvironmentCoverage, "unknown:") {
		t.Fatalf("unread inline metadata became a known exclusion: %+v", got)
	}
}

func TestCommandPortabilityCPUMemberCapsNormalizeBeforeLowering(t *testing.T) {
	q, _ := fixture(t)
	r := breadthRecipe("linux-64", "osx-arm64")
	q.environments = EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}
	for _, composed := range []bool{false, true} {
		s := cpuResolveSpec()
		s.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 4}
		if composed {
			s.Run = RunSpec{}
			s.Execution = &ExecutionSpec{Main: ExecutionMember{Command: []string{"echo", "ok"}, MemoryGB: 1, Environment: "tools"}}
		} else {
			s.Run.MemoryGB = 1
			s.Environment = Environment{Name: "tools"}
		}
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
		before := identityHash(s)
		resolved, err := q.Resolve(ResolveRequest{Spec: s})
		if err != nil || resolved.Portability == nil {
			t.Fatalf("resolve rejected valid CPU member cap: %+v %v", resolved, err)
		}
		p, err := q.Portability(PortabilityRequest{Spec: &s})
		if err != nil || len(p.Targets) != 2 || len(p.Targets[0].Environments) != 1 || p.Targets[0].Environments[0].Key != r.Key {
			t.Fatalf("CPU member cap dropped lock discovery: %+v %v", p, err)
		}
		if identityHash(s) != before {
			t.Fatal("normalization changed caller intent")
		}
	}
}

// The diagnostic is not a launch permit or a result check. A real small command
// still goes through submit, accepted output verification and exact release.
func TestCommandPortabilityRealCPUResult(t *testing.T) {
	root := t.TempDir()
	d, err := StartDev(DevConfig{Workers: 1, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "store"), Token: "breadth-fixture", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s := cpuResolveSpec()
	s.SubmissionKey = "breadth-result"
	_, input := retentionJobInput(t, d.Client(), "portable-cpu-ok\n")
	s.Files = map[string]FileRef{"source.txt": input}
	s.Run.Command = []string{"sh", "-c", "cat source.txt > result.txt"}
	s.Outputs = map[string]string{"result.txt": "results"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var report ResolveReport
	if err := d.Client().Call(ctx, "POST", "/api/resolve", ResolveRequest{Spec: s}, &report); err != nil || report.Portability == nil {
		t.Fatal(report, err)
	}
	j, err := d.Client().Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := d.Client().Wait(ctx, j.ID)
	if err != nil || completed.State != "succeeded" {
		t.Fatal(completed, err)
	}
	dest := filepath.Join(root, "accepted.txt")
	if err := d.Client().DownloadResult(ctx, j.ID, "result.txt", dest); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(dest); err != nil || string(data) != "portable-cpu-ok\n" {
		t.Fatalf("accepted bytes: %q %v", data, err)
	}
	await(t, func() bool {
		current, err := d.Client().GetJob(ctx, j.ID)
		return err == nil && current.ReleaseConfirmed
	})
	replayed, err := d.Client().Submit(ctx, s)
	if err != nil || replayed.ID != j.ID || !replayed.SubmissionReused {
		t.Fatal(replayed, err)
	}
}
