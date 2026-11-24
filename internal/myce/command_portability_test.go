package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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

// The diagnostic is not a launch permit or a result check. A real small command
// still goes through submit, accepted output verification and exact release.
func TestCommandPortabilityRealCPUResult(t *testing.T) {
	root := t.TempDir()
	d, err := startTestDev(t, DevConfig{EnvironmentCacheLimit: 1 << 30, Workers: 1, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "store"), Token: "breadth-fixture", Interval: 10 * time.Millisecond})
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
