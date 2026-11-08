package myce

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"
)

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

func TestHostHygieneCheckpointAccessUpgradeKeepsNASBaseline(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
	dir := t.TempDir()
	sample := HostHealthSample{Mounts: []HostMountHealth{{Root: "checkpoint", LatencySeconds: 10}, {Root: "nas", LatencySeconds: .001}}}
	old := hostHealthDisk{MountProbeVersion: "stat-readdir-v1", HostHealth: HostHealth{Baseline: []HostHealthSample{sample}, History: []HostHealthSample{sample}}}
	if err := saveWorkerJSON(dir, "host-health.json", old); err != nil {
		t.Fatal(err)
	}
	m, err := openHostHealth(dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	mounts := m.health.Baseline[0].Mounts
	if len(mounts) != 1 || mounts[0].Root != "nas" || len(m.health.History[0].Mounts) != 2 {
		t.Fatal("upgrade reused checkpoint enumeration timings or lost NAS/history", m.health)
	}
}

func TestHostHygieneMountSlowHistoryDoesNotWidenBaseline(t *testing.T) {
	m, c := healthFixture(t, 1)
	for i := range m.health.Baseline {
		m.health.Baseline[i].Mounts = []HostMountHealth{{Root: "checkpoint", LatencySeconds: .001}}
	}
	p := &hostMountProbe{root: "checkpoint", path: "sentinel", measure: func(string, func()) hostMountResult { return hostMountResult{latency: 100 * time.Millisecond} }}
	m.mounts["checkpoint"] = p
	for i := 0; i < hostHealthConfirmation+1; i++ {
		c.Advance(signalRefresh)
		if err := m.sample(context.Background()); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return len(p.pending) == 1 })
	}
	if got := m.health.History[len(m.health.History)-1].Mounts[0].Reason; got != "mount_probe_slow" {
		t.Fatal("slow probes contaminated healthy baseline", got)
	}
	for _, s := range m.health.Baseline {
		for _, mount := range s.Mounts {
			if mount.LatencySeconds > .001 {
				t.Fatal("adverse probe entered baseline")
			}
		}
	}
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
func TestHostHygieneMountRecoveryReadBackoffAndHumanAlert(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		t.Run(osname, func(t *testing.T) {
			q, clock := fixture(t)
			files := fstest.MapFS{"root/sentinel": &fstest.MapFile{Data: []byte("ok")}}
			var failure error
			var fakeMu sync.Mutex
			mounted := true
			setFailure := func(err error, present bool) { fakeMu.Lock(); failure, mounted = err, present; fakeMu.Unlock() }
			p := &hostMountProbe{root: "checkpoint", path: "root"}
			p.measure = func(string, func()) hostMountResult {
				fakeMu.Lock()
				defer fakeMu.Unlock()
				err := probeRecoveryMount("/storage/checkpoints", "/storage", osname, func(string) error {
					return readHealthMount("root", func(path string) (os.FileInfo, error) { return fs.Stat(files, path) }, func(path string) (fs.ReadDirFile, error) {
						if failure != nil {
							return nil, failure
						} // stat still succeeds from cache
						f, err := files.Open(path)
						if err != nil {
							return nil, err
						}
						return f.(fs.ReadDirFile), nil
					}, func() {})
				}, func() ([]nasMount, error) {
					if mounted {
						return []nasMount{{root: "/storage", kind: "smbfs"}}, nil
					}
					return nil, nil
				})
				return hostMountResult{latency: time.Millisecond, err: err}
			}
			baseline := make([]HostHealthSample, hostHealthWarmup)
			for i := range baseline {
				baseline[i].Mounts = []HostMountHealth{{Root: "checkpoint", LatencySeconds: .001}}
			}
			sample := func() HostMountHealth { return p.sample(clock.Now(), baseline) }
			complete := func() HostMountHealth {
				t.Helper()
				await(t, func() bool { return p.pending != nil && len(p.pending) == 1 })
				clock.Advance(signalRefresh)
				return sample()
			}
			sample()
			for i := 0; i < hostHealthConfirmation; i++ {
				complete()
			}
			// Join the outstanding healthy request before changing fake filesystem.
			await(t, func() bool { return len(p.pending) == 1 })
			setFailure(errors.New("private SMB connection failure"), true)
			complete() // consumes already-completed healthy request, starts failed read
			dropped := complete()
			if dropped.Reason != "mount_probe_failed" || p.pending != nil {
				t.Fatal(dropped, "missing backoff")
			}
			firstDelay := p.nextProbe.Sub(clock.Now())
			clock.Advance(firstDelay - time.Nanosecond)
			sample()
			if p.pending != nil {
				t.Fatal("recovery ran before backoff")
			}
			clock.Advance(time.Nanosecond)
			sample()
			await(t, func() bool { return len(p.pending) == 1 })
			setFailure(os.ErrPermission, false)
			complete() // consumes failed read; next retry requires human action
			if p.nextProbe.Sub(clock.Now()) <= firstDelay {
				t.Fatal("retry did not back off")
			}
			clock.Advance(p.nextProbe.Sub(clock.Now()))
			sample()
			human := complete()
			if human.Reason != string(mountHumanReason(osname)) {
				t.Fatal(human)
			}
			m := cpuMachine("worker", "host", "cpu", 2, 8)
			publish := func(mount HostMountHealth) {
				m.UnavailableRoots = nil
				if mount.Reason != "" {
					m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "configured NAS mount missing"}}
				}
				m.HostHealth = &HostHealth{History: []HostHealthSample{{At: clock.Now(), Mounts: []HostMountHealth{mount}}}}
				applyHostMountHealth(Worker{}, &m)
				check(t, q, m, nil)
			}
			publish(dropped)
			publish(human)
			publish(human)
			alerts := alertsOf(q, "host_mount_action_required")
			if len(alerts) != 1 || alerts[0].Reason != human.Reason {
				t.Fatal("missing/double human-action wake", alerts)
			}
			stored := q.s.Machines[m.ID]
			if len(stored.UnavailableRoots) != 1 || stored.UnavailableRoots[0].Reason != human.Reason {
				t.Fatal("status lost typed reason")
			}
			local := &Job{Spec: cpuSpec(1, 1)}
			local.Spec.Priority = "production"
			if reason := storageRootReason(local, stored); reason != "" {
				t.Fatal("local work blocked", reason)
			}
			dependent := &Job{Spec: cpuSpec(1, 1)}
			dependent.Spec.SharedInputs = map[string]SharedInput{"data": {NAS: "data", SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
			if storageRootReason(dependent, stored) == "" {
				t.Fatal("NAS work eligible after drop")
			}
			setFailure(nil, true)
			clock.Advance(p.nextProbe.Sub(clock.Now()))
			sample()
			var recovered HostMountHealth
			for i := 0; i < hostHealthConfirmation; i++ {
				recovered = complete()
			}
			if recovered.Reason != "" {
				t.Fatal("did not recover", recovered)
			}
			publish(recovered)
			if reason := storageRootReason(dependent, q.s.Machines[m.ID]); reason != "" {
				t.Fatal("NAS work did not resume", reason)
			}
			await(t, func() bool { return len(p.pending) == 1 })
		})
	}
}

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

func TestHostHygieneMountColdAndLearnedUseOperationStallAllowance(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
	for _, latency := range []time.Duration{0, time.Millisecond, time.Second} {
		release := make(chan struct{})
		calls := atomic.Int32{}
		p := &hostMountProbe{root: "nas", path: "fake", probe: func(string, func()) error { calls.Add(1); <-release; return nil }}
		var baseline []HostHealthSample
		if latency > 0 {
			baseline = []HostHealthSample{{Mounts: []HostMountHealth{{Root: "nas", LatencySeconds: latency.Seconds()}}}}
		}
		p.sample(clock.Now(), baseline)
		await(t, func() bool { return calls.Load() == 1 })
		clock.Advance(checkpointAvailabilityStallBudget - time.Nanosecond)
		if got := p.sample(clock.Now(), baseline); got.Reason == "mount_probe_stalled" {
			t.Fatal("learned latency shortened operation allowance", got)
		}
		clock.Advance(time.Nanosecond)
		for i := 0; i < hostHealthConfirmation+10; i++ {
			p.sample(clock.Now(), baseline)
		}
		if p.reason != "mount_probe_stalled" || calls.Load() != 1 {
			t.Fatal("unbounded cold/learned stall", latency, p.reason, calls.Load())
		}
		close(release)
		await(t, func() bool { return len(p.pending) == 1 })
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

func TestHostHygieneMountUpgradeDiscardsOnlyStatBaseline(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
	dir := t.TempDir()
	old := HostHealth{DrainReason: "host_memory_pressure", DiskRecovery: map[string]float64{"disk": 100}, History: []HostHealthSample{{At: clock.Now(), Mounts: []HostMountHealth{{Root: "checkpoint", LatencySeconds: .001}}}}}
	for i := 0; i < hostHealthWarmup; i++ {
		old.Baseline = append(old.Baseline, HostHealthSample{HostHealthReading: HostHealthReading{Load: 1, WakeSeconds: .001}, Mounts: []HostMountHealth{{Root: "checkpoint", LatencySeconds: .001}}})
	}
	// This is the actual unversioned stat-only disk format from #835.
	if err := saveWorkerJSON(dir, "host-health.json", old); err != nil {
		t.Fatal(err)
	}
	m, err := openHostHealth(dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	if m.health.DrainReason != old.DrainReason || m.health.DiskRecovery["disk"] != 100 || len(m.health.History) != 1 || len(m.health.History[0].Mounts) != 1 {
		t.Fatal("upgrade lost other evidence/history")
	}
	for _, s := range m.health.Baseline {
		if len(s.Mounts) != 0 || s.Load != 1 {
			t.Fatal("old probe baseline reused or other baseline lost", s)
		}
	}
	m.disks = map[string]float64{"disk": 200} // Affirmative recovery of the retained disk reserve.
	m.source = func(context.Context) (HostHealthReading, error) {
		return HostHealthReading{Load: 1, WakeSeconds: .001}, nil
	}
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
		t.Fatal("upgraded probe could not recover", got)
	}
	reopened, err := openHostHealth(dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	fresh := 0
	for _, s := range reopened.health.Baseline {
		fresh += len(s.Mounts)
	}
	if fresh < hostHealthWarmup {
		t.Fatal("current probe baseline not retained on restart", fresh)
	}
}
