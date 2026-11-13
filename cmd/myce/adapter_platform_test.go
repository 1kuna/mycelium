package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestAdapterPlatformNarrowing(t *testing.T) {
	a, err := decodeAdapter([]byte(`project: platform-test
entries:
  work:
    run: {command: [true]}
    needs: {cpu_cores: 1, platform: any}
    advanced: {allow_cpu_fallback: true}
`))
	if err != nil {
		t.Fatal(err)
	}
	p, original, err := adapterProfile(a, "work")
	if err != nil {
		t.Fatal(err)
	}
	for _, platform := range []string{"metal", "cuda", "xpu", "cpu"} {
		narrowed, data, err := adapterProfile(a, "work", platform)
		if err != nil || !slices.Equal(narrowed.Needs.Platform.Required, []string{platform}) || narrowed.AllowCPUFallback || bytes.Equal(original, data) {
			t.Fatalf("%s: %+v %s %v", platform, narrowed.Needs, data, err)
		}
	}
	if !p.Needs.Platform.Any || !p.AllowCPUFallback {
		t.Fatal("narrowing changed the unrestricted entry")
	}
	e := a.Entries["work"]
	e.AllowCPUFallback = false
	a.Entries["work"] = e
	_, _, err = adapterProfile(a, "work", "cpu")
	var typed *entryPlatformError
	if !errors.As(err, &typed) || slices.Contains(typed.Allowed, "cpu") || !slices.Contains(typed.Allowed, "metal") {
		t.Fatalf("CPU without advanced opt-in: %v", err)
	}
	e.Needs.Platform = myce.Platform{Required: []string{"cuda"}}
	a.Entries["work"] = e
	_, _, err = adapterProfile(a, "work", "metal")
	if !errors.As(err, &typed) || !slices.Equal(typed.Allowed, []string{"cuda"}) {
		t.Fatalf("required platform widened: %v", err)
	}
	e.Needs = defaultEntryNeeds()
	e.Targets = []myce.ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}
	a.Entries["work"] = e
	cpu, _, err := adapterProfile(a, "work", "cpu")
	if err != nil || !slices.Equal(cpu.Needs.Platform.Required, []string{"cpu"}) || !slices.Equal(cpu.Targets, e.Targets) {
		t.Fatalf("explicit CPU entry: %+v %v", cpu, err)
	}
	_, _, err = adapterProfile(a, "work", "metal")
	if !errors.As(err, &typed) || !slices.Equal(typed.Allowed, []string{"cpu"}) {
		t.Fatalf("CPU entry widened to GPU: %v", err)
	}
	e.Needs.Platform = myce.Platform{Any: true}
	e.Targets[0].Platform = "cuda"
	a.Entries["work"] = e
	_, _, err = adapterProfile(a, "work", "metal")
	if !errors.As(err, &typed) || !slices.Equal(typed.Allowed, []string{"cuda"}) {
		t.Fatalf("explicit target widened: %v", err)
	}
	// Native entries are narrowed by their complete variants, retaining the
	// selected variant's exact OS/architecture and authored execution.
	e = adapterEntry{Variants: []adapterVariant{}}
	for _, platform := range []string{"cpu", "metal"} {
		e.Variants = append(e.Variants, adapterVariant{Target: myce.ExecutionTarget{OS: "darwin", Arch: "arm64", Platform: platform}, Execution: myce.ExecutionSpec{Main: myce.ExecutionMember{Command: []string{"true"}}}, Baselines: map[string]myce.ExtensionBaseline{"extension:work": {}}})
	}
	a.Entries["work"] = e
	native, _, err := adapterProfile(a, "work", "metal")
	if err != nil || len(native.Native.Variants) != 1 || native.Native.Variants[0].Target.Platform != "metal" || len(native.Targets) != 1 {
		t.Fatalf("native narrowing: %+v %v", native, err)
	}
	_, _, err = adapterProfile(a, "work", "cuda")
	if !errors.As(err, &typed) || !slices.Equal(typed.Allowed, []string{"cpu", "metal"}) {
		t.Fatalf("native widened: %v", err)
	}
}

func TestAdapterPlatformCommandBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adapter.yaml")
	if err := os.WriteFile(path, []byte(cpuAdapterYAML("source")), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"--entry", "work", "--adapter", path, "--platform", "metal"},
		{"--entry", "work", "--adapter", path, "--platform="},
	} {
		err := runCommand(t.Context(), args, &bytes.Buffer{}, &bytes.Buffer{})
		var typed *entryPlatformError
		if !errors.As(err, &typed) || exitCode(err) != 64 || !slices.Equal(typed.Allowed, []string{"cpu"}) {
			t.Fatalf("run refusal: %v", err)
		}
	}
	err := specCommand([]string{"check", "--resolve", "--entry", "work", "--platform", "metal", path}, &bytes.Buffer{}, &bytes.Buffer{})
	var typed *entryPlatformError
	if !errors.As(err, &typed) || !slices.Equal(typed.Allowed, []string{"cpu"}) {
		t.Fatalf("resolve refusal: %v", err)
	}
	for _, args := range [][]string{
		{"--platform", "cpu", "--", "true"},
		{"--profile", "work", "--platform", "cpu"},
		{"--entry", "work", "--gpu", "metal"},
		{"--entry", "work", "--cpu", "1"},
	} {
		if err := runCommand(t.Context(), args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatalf("unsupported combination accepted: %v", args)
		}
	}
	if err := specCommand([]string{"check", "--platform", "cpu", path}, &bytes.Buffer{}, &bytes.Buffer{}); exitCode(err) != 2 {
		t.Fatalf("platform without resolve: %v", err)
	}
}

func TestAdapterPlatformDev(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte("test -z \"${MYCE_TOKEN:-}\"\nprintf accepted > result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "adapter.yaml")
	data := strings.Replace(cpuAdapterYAML(source), "platform: cpu}", "platform: any}", 1)
	data = strings.SplitN(data, "    targets:", 2)[0] + "    outputs: {result: results}\n    checks: {result: [test, -s, result]}\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	binary := devTestBinary(t)
	state := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), Token: "platform-fixture", GPU: "fake", MemoryGB: 4, Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	wait(t, ctx, func() bool { s, e := status(ctx, d.Client()); return e == nil && len(s.Machines) == 1 })
	invoke := func(args ...string) (string, error) {
		command := exec.CommandContext(ctx, binary, args...)
		command.Dir = root
		command.Env = append(os.Environ(), "MYCE_TOKEN=platform-fixture", "MYCE_NOTIFY=")
		var out, diagnostics bytes.Buffer
		command.Stdout, command.Stderr = &out, &diagnostics
		err := command.Run()
		if err != nil {
			return out.String() + diagnostics.String(), err
		}
		return out.String(), nil
	}
	snapshot, err := status(ctx, d.Client())
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, platform := range []string{"", "fake", "metal", "xpu"} {
		if platform == "metal" || platform == "xpu" {
			// Advertise synthetic inventory only: dev's real fake worker
			// executes the unrestricted/fake runs; these jobs stay queued.
			m := *snapshot.Machines[0]
			m.ID, m.HostID, m.Session = "fixture-"+platform, "host-"+platform, "session-"+platform
			m.GPU, m.Devices = platform, nil
			profile := *m.CapabilityProfile
			profile.Accelerators = []myce.AcceleratorProfile{{Type: platform, MemoryBytes: 4 << 30}}
			m.CapabilityProfile = &profile
			if _, err := fixtureQueueCheckin(d.Queue, myce.Checkin{Machine: m}); err != nil {
				t.Fatal(err)
			}
		}
		args := []string{"run", "--adapter", path, "--entry", "work", "--queue", d.URL}
		if platform != "" {
			args = append(args, "--platform", platform)
		}
		out, err := invoke(args...)
		var receipt profileReceipt
		if err != nil || json.Unmarshal([]byte(out), &receipt) != nil {
			t.Fatalf("submit %s: %v %s", platform, err, out)
		}
		job, err := d.Client().GetJob(ctx, receipt.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if seen[job.Spec.SubmissionKey] || platform != "" && !slices.Equal(job.Spec.Needs.Platform.Required, []string{platform}) {
			t.Fatalf("wrong restriction or reused key: %+v", job.Spec)
		}
		seen[job.Spec.SubmissionKey] = true
		if platform == "xpu" || platform == "metal" {
			if _, err := d.Client().Cancel(ctx, job.ID); err != nil {
				t.Fatal(err)
			}
		} else if job, err = d.Client().Wait(ctx, job.ID); err != nil || job.State != "succeeded" {
			t.Fatalf("execute %s: %+v %v", platform, job, err)
		}
	}
	out, err := invoke("spec", "check", "--resolve", "--entry", "work", "--platform", "fake", "--queue", d.URL, path)
	if err != nil {
		t.Fatalf("resolve narrowed entry: %v %s", err, out)
	}
	// Both public commands refuse widening before contacting the queue.
	if err := os.WriteFile(path, []byte(strings.Replace(data, "platform: any", "platform: cuda", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"run", "--adapter", path, "--entry", "work", "--platform", "metal"},
		{"spec", "check", "--resolve", "--entry", "work", "--platform", "metal", path},
	} {
		out, err := invoke(args...)
		if err == nil || !strings.Contains(out, "entry_platform_not_allowed:") || !strings.Contains(out, "allowed: [cuda]") {
			t.Fatalf("widening accepted or untyped refusal: %v %s", err, out)
		}
	}
}
