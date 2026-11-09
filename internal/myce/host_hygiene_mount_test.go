package myce

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"
)

func TestHostHygieneCheckpointAccessHangKeepsSingleRequest(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
	release := make(chan struct{})
	var calls atomic.Int32
	checkpointProbe := workerHealthCheckpointProbe
	t.Cleanup(func() { workerHealthCheckpointProbe = checkpointProbe })
	workerHealthCheckpointProbe = func(string, func()) error { calls.Add(1); <-release; return nil }
	p := &hostMountProbe{root: "checkpoint", path: "fake", probe: healthMountAccess("checkpoint")}
	p.sample(clock.Now(), nil)
	await(t, func() bool { return calls.Load() == 1 })
	clock.Advance(checkpointAvailabilityStallBudget) // Unchanged per-operation stall allowance.
	for i := 0; i < hostHealthConfirmation; i++ {
		clock.Advance(signalRefresh)
		p.sample(clock.Now(), nil)
	}
	if p.reason != "mount_probe_stalled" || calls.Load() != 1 {
		t.Fatal("hung checkpoint access was not caught or multiplied", p.reason, calls.Load())
	}
	close(release)
	await(t, func() bool { return len(p.pending) == 1 })
}

func TestHostHygieneMountSingleOutstandingAndScopedRecovery(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	release := make(chan struct{})
	var calls atomic.Int32
	p := &hostMountProbe{root: "checkpoint", path: "sentinel", probe: func(string, func()) error { calls.Add(1); <-release; return nil }}
	baseline := make([]HostHealthSample, hostHealthWarmup)
	for i := range baseline {
		baseline[i].Mounts = []HostMountHealth{{Root: "checkpoint", LatencySeconds: .001}}
	}
	p.sample(now, baseline)
	await(t, func() bool { return calls.Load() == 1 })
	for i := 0; i < hostHealthConfirmation; i++ {
		p.sample(now.Add(time.Hour), baseline)
	}
	for i := 0; i < 100; i++ {
		if got := p.sample(now.Add(time.Hour), baseline); got.Reason != "mount_probe_stalled" {
			t.Fatal(got)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("timeout created probe swarm")
	}
	m := Machine{HostHealth: &HostHealth{History: []HostHealthSample{{Mounts: []HostMountHealth{{Root: "checkpoint", Reason: "mount_probe_stalled"}}}}}}
	applyHostMountHealth(Worker{}, &m)
	local := &Job{Spec: cpuSpec(1, 1)}
	local.Spec.Resumable = false
	local.Spec.Priority = "production"
	if storageRootReason(local, &m) != "" {
		t.Fatal("local work blocked by remote failure")
	}
	dependent := &Job{Spec: cpuSpec(1, 1)}
	dependent.Spec.SharedInputs = map[string]SharedInput{"data": {NAS: "data", SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	if storageRootReason(dependent, &m) == "" {
		t.Fatal("dependent work not withheld")
	}
	close(release)
	await(t, func() bool {
		select {
		case <-p.pending:
			p.pending = nil
			return true
		default:
			return false
		}
	})
}

// Replay a working mount, dropped connection, permission-required remount and
// recovery using only an in-memory filesystem and injected scheduling clock.

func TestHostHygieneMountAutomountRetriggerAndMissingStub(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		mounted, accessed := false, 0
		inventory := func() ([]nasMount, error) {
			if mounted {
				return []nasMount{{root: "/storage", kind: "smbfs"}}, nil
			}
			return []nasMount{{root: "/storage", kind: "autofs"}}, nil
		}
		err := probeRecoveryMount("/storage/checkpoints", "/storage", osname, func(string) error { accessed++; mounted = true; return nil }, inventory)
		if err != nil || accessed != 1 {
			t.Fatal("existing automount not triggered", osname, err)
		}
		// A readable bare mountpoint must never establish network recovery.
		err = probeRecoveryMount("/storage/checkpoints", "/storage", osname, func(string) error { return nil }, func() ([]nasMount, error) { return nil, nil })
		if err != mountHumanReason(osname) {
			t.Fatal("local stub accepted", err)
		}
	}
}

func TestHostHygieneMountPendingReadCannotConfirmRecovery(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
	release := make(chan struct{})
	p := &hostMountProbe{root: "checkpoint", path: "fake", reason: "mount_linux_operator_required", probe: func(string, func()) error { <-release; return nil }}
	// The first fresh success starts recovery, then the next read stalls within
	// this host's measured deadline. Repeated observations are not new successes.
	p.pending = make(chan hostMountResult, 1)
	p.pending <- hostMountResult{latency: time.Second}
	baseline := []HostHealthSample{{Mounts: []HostMountHealth{{Root: "checkpoint", LatencySeconds: 1}}}}
	p.sample(clock.Now(), baseline)
	for i := 0; i < 100; i++ {
		if got := p.sample(clock.Now(), baseline); got.Reason != "mount_linux_operator_required" {
			t.Fatal("pending read cleared hold", got)
		}
	}
	close(release)
	await(t, func() bool { return len(p.pending) == 1 })
}

func TestHostHygieneMountWarmupAcceptsSuccessfulLatencyVariation(t *testing.T) {
	m, clock := healthFixture(t, 1)
	// Replay the review incident: one accepted 1ms success, then normal 2ms reads.
	m.health.Baseline[0].Mounts = []HostMountHealth{{Root: "checkpoint", LatencySeconds: .001}}
	p := &hostMountProbe{root: "checkpoint", path: "fake", measure: func(string, func()) hostMountResult { return hostMountResult{latency: 2 * time.Millisecond} }}
	m.mounts["checkpoint"] = p
	for i := 0; i < hostHealthWarmup+hostHealthConfirmation+1; i++ {
		clock.Advance(signalRefresh)
		if err := m.sample(context.Background()); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return len(p.pending) == 1 })
	}
	if got := m.health.History[len(m.health.History)-1].Mounts[0].Reason; got != "" {
		t.Fatal("healthy variation permanently withheld storage", got)
	}
	successes := 0
	for _, s := range m.health.Baseline {
		for _, mount := range s.Mounts {
			if mount.Root == "checkpoint" && mount.Reason == "" {
				successes++
			}
		}
	}
	if successes < hostHealthWarmup {
		t.Fatal("successful warmup was frozen", successes)
	}
}

// Retained regression: #895 tiny checkpoint access must not enumerate6900entry root.
func TestHostHygieneCheckpointLargeDirectoryDoesNotEnumerate(t *testing.T) {
	files := fstest.MapFS{"checkpoints": &fstest.MapFile{Mode: fs.ModeDir}}
	for i := 0; i < 6900; i++ {
		files[fmt.Sprintf("checkpoints/attempt-%d", i)] = &fstest.MapFile{Mode: fs.ModeDir}
	}
	var enumerations, accesses atomic.Int32
	readProbe, checkpointProbe := workerHealthMountProbe, workerHealthCheckpointProbe
	t.Cleanup(func() { workerHealthMountProbe, workerHealthCheckpointProbe = readProbe, checkpointProbe })
	workerHealthMountProbe = func(path string, _ func()) error {
		enumerations.Add(1)
		return errors.New("large-directory stream initialization stalled")
	}
	workerHealthCheckpointProbe = func(path string, progress func()) error {
		if _, err := fs.Stat(files, path); err != nil {
			return err
		}
		// The same fake root supports tiny exclusive access without enumerating
		// its busy attempt directories. No payload file is opened.
		name := path + "/.myce-probe-fake.tmp"
		if _, exists := files[name]; exists {
			return fs.ErrExist
		}
		files[name] = &fstest.MapFile{}
		delete(files, name)
		accesses.Add(1)
		return nil
	}
	m, clock := healthFixture(t, 1)
	access := healthMountAccess("checkpoint")
	p := &hostMountProbe{root: "checkpoint", path: "checkpoints", measure: func(path string, _ func()) hostMountResult {
		return hostMountResult{latency: time.Millisecond, err: access(path, func() {})}
	}}
	m.mounts["checkpoint"] = p
	for i := 0; i < hostHealthWarmup+hostHealthConfirmation+1; i++ {
		clock.Advance(signalRefresh)
		if err := m.sample(context.Background()); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return len(p.pending) == 1 })
	}
	got := m.health.History[len(m.health.History)-1].Mounts[0]
	if enumerations.Load() != 0 || accesses.Load() == 0 || got.Reason != "" || got.Adverse {
		t.Fatal("directory enumeration withheld responsive checkpoint access", enumerations.Load(), accesses.Load(), got)
	}
	await(t, func() bool { return len(p.pending) == 1 })
	if err := healthMountAccess("nas")("checkpoints", func() {}); err == nil || enumerations.Load() != 1 {
		t.Fatal("NAS root lost its read-only directory check")
	}
}
