package myce

import (
	"io/fs"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"
)

type steppedMountDirectory struct {
	fs.ReadDirFile
	step func()
}

func (f steppedMountDirectory) ReadDir(n int) ([]fs.DirEntry, error) {
	f.step()
	return f.ReadDirFile.ReadDir(n)
}
func (f steppedMountDirectory) Close() error {
	f.step()
	return f.ReadDirFile.Close()
}

func mountAdmissionFixture(t *testing.T, role string) (*Queue, *fakeClock, []HostHealthSample, func(HostMountHealth) string) {
	t.Helper()
	q, clock := fixture(t)
	baseline := make([]HostHealthSample, hostHealthWarmup)
	for i := range baseline {
		baseline[i].Mounts = []HostMountHealth{{Root: role, LatencySeconds: .001}}
	}
	m := cpuMachine("worker", "host", "cpu", 2, 8)
	w := Worker{ServiceConfig: &ServiceConfig{NAS: "/share"}, JobFileRoot: "/share/inputs"}
	dependent := &Job{Spec: cpuSpec(1, 1)}
	if role == "nas" {
		dependent.Spec.Priority = "production"
		dependent.Spec.SharedInputs = map[string]SharedInput{"data": {NAS: "data", SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	}
	return q, clock, baseline, func(mount HostMountHealth) string {
		t.Helper()
		m.UnavailableRoots = nil
		m.HostHealth = &HostHealth{History: []HostHealthSample{{At: clock.Now(), Mounts: []HostMountHealth{mount}}}}
		applyHostMountHealth(w, &m)
		check(t, q, m, nil)
		// This is the same predicate the production deploy health gate reads.
		if status := q.hostMaintenanceStatus(m.HostID); status.Healthy == mountHealthWithholds(mount.Reason) {
			t.Fatal("mount classification did not reach host health", mount, status)
		}
		return storageRootReason(dependent, q.s.Machines[m.ID])
	}
}

// #948 captured these completed NAS outliers latching mount_probe_slow,
// unavailable dependent roots, then healthy:false for a production deploy gate.
func TestHostHygieneMountCompletedOutliersKeepProductionHealth(t *testing.T) {
	q, clock, baseline, publish := mountAdmissionFixture(t, "nas")
	for i := range baseline {
		baseline[i].Mounts[0].LatencySeconds = .1
	}
	returns := make(chan hostMountResult)
	p := &hostMountProbe{root: "nas", clock: clock, last: HostMountHealth{Root: "nas", LatencySeconds: .1}, measure: func(string, func()) hostMountResult { return <-returns }}
	t.Cleanup(func() {
		close(returns)
		await(t, func() bool { return len(p.pending) == 1 })
	})
	p.sample(clock.Now(), baseline)
	for i, latency := range []time.Duration{645 * time.Millisecond, 1608 * time.Millisecond, 457 * time.Millisecond, 38 * time.Millisecond, 69 * time.Millisecond, 15 * time.Millisecond} {
		returns <- hostMountResult{latency: latency}
		await(t, func() bool { return len(p.pending) == 1 })
		clock.Advance(5 * time.Second) // Captured observation spacing, not a production bound.
		got := p.sample(clock.Now(), baseline)
		if reason := publish(got); reason != "" {
			t.Fatal("completed outlier blocked dependent production work", got, reason)
		}
		if i == 2 && got.Reason != "mount_probe_slow" {
			t.Fatal("three completed outliers lost their advisory", got)
		}
	}
	if len(alertsOf(q, "host_mount_degraded")) != 1 || len(alertsOf(q, "host_mount_unavailable")) != 0 {
		t.Fatal("advisory alert was lost or became an availability failure")
	}
}

// Four successful metadata operations can exceed the stall allowance in total.
// Only an individual operation's lack of progress is an admission failure.
func TestHostHygieneMountProgressingOutlierIsAdvisory(t *testing.T) {
	for _, role := range []string{"checkpoint", "nas"} {
		t.Run(role, func(t *testing.T) {
			q, clock, baseline, publish := mountAdmissionFixture(t, role)
			files := fstest.MapFS{"root": &fstest.MapFile{Mode: fs.ModeDir}}
			permit, entered := make(chan struct{}), make(chan int, 8)
			t.Cleanup(func() { close(permit) })
			var operations atomic.Int32
			p := &hostMountProbe{root: role, path: "root", clock: clock, last: HostMountHealth{Root: role, LatencySeconds: .001}}
			p.measure = func(path string, progress func()) hostMountResult {
				start := clock.Now()
				step := func() { entered <- int(operations.Load()); <-permit }
				observed := func() { progress(); operations.Add(1) }
				if role == "nas" {
					err := readHealthMount(path, func(path string) (os.FileInfo, error) {
						step()
						return fs.Stat(files, path)
					}, func(path string) (fs.ReadDirFile, error) {
						step()
						f, err := files.Open(path)
						if err != nil {
							return nil, err
						}
						return steppedMountDirectory{ReadDirFile: f.(fs.ReadDirFile), step: step}, nil
					}, observed)
					return hostMountResult{latency: clock.Now().Sub(start), err: err}
				}
				for stage := 0; stage < 4; stage++ {
					step()
					switch stage {
					case 0:
						if _, err := fs.Stat(files, path); err != nil {
							return hostMountResult{err: err}
						}
					case 1:
						files[path+"/probe"] = &fstest.MapFile{}
					case 3:
						delete(files, path+"/probe")
					}
					observed()
				}
				return hostMountResult{latency: clock.Now().Sub(start)}
			}
			p.sample(clock.Now(), baseline)
			for stage := 0; stage < 4; stage++ {
				<-entered
				// Spend twenty seconds in each operation, matching successful metadata
				// tails that motivated #234. Every operation stays below its 30s bound.
				for i := 0; i < 4; i++ {
					clock.Advance(signalRefresh)
					got := p.sample(clock.Now(), baseline)
					if got.Reason == "mount_probe_stalled" || publish(got) != "" {
						t.Fatal("progressing sequence held admission", stage, got)
					}
				}
				permit <- struct{}{}
				await(t, func() bool { return operations.Load() >= int32(stage+1) })
			}
			await(t, func() bool { return len(p.pending) == 1 })
			got := p.sample(clock.Now(), baseline)
			if got.Reason != "mount_probe_slow" || got.LatencySeconds <= checkpointAvailabilityStallBudget.Seconds() || publish(got) != "" {
				t.Fatal("slow completed success was not advisory", got)
			}
			alerts := alertsOf(q, "host_mount_degraded")
			if len(alerts) != 1 || alerts[0].Reason != "mount_probe_slow" || alerts[0].Mount == nil || alerts[0].Mount.SlowThresholdSeconds != .001 || alerts[0].Mount.OperationAgeSeconds <= 0 || len(alertsOf(q, "host_mount_unavailable")) != 0 {
				t.Fatal("advisory lost measured evidence or duplicated its wake", alerts)
			}
		})
	}
}

func TestHostHygieneMountAdvisoryEscalatesAndNeedsFreshRecovery(t *testing.T) {
	q, clock, baseline, publish := mountAdmissionFixture(t, "checkpoint")
	returns := make(chan error)
	t.Cleanup(func() { close(returns) })
	var calls atomic.Int32
	var latency atomic.Int64
	latency.Store(int64(100 * time.Millisecond))
	p := &hostMountProbe{root: "checkpoint", path: "root", clock: clock, reason: "mount_probe_slow", last: HostMountHealth{Root: "checkpoint", LatencySeconds: .1}}
	p.measure = func(string, func()) hostMountResult {
		calls.Add(1)
		err := <-returns
		return hostMountResult{latency: time.Duration(latency.Load()), err: err}
	}
	initial := p.sample(clock.Now(), baseline)
	if publish(initial) != "" {
		t.Fatal("advisory withheld admission")
	}
	await(t, func() bool { return calls.Load() == 1 })
	clock.Advance(checkpointAvailabilityStallBudget)
	var stalled HostMountHealth
	for i := 0; i < hostHealthConfirmation; i++ {
		stalled = p.sample(clock.Now(), baseline)
	}
	if stalled.Reason != "mount_probe_stalled" || stalled.OperationAgeSeconds != checkpointAvailabilityStallBudget.Seconds() || publish(stalled) == "" || calls.Load() != 1 {
		t.Fatal("no-progress stall did not fence dependent work with one request", stalled, calls.Load())
	}
	alerts := alertsOf(q, "host_mount_unavailable")
	if len(alerts) != 1 || alerts[0].Reason != "mount_probe_stalled" || alerts[0].Mount == nil || alerts[0].Mount.StallAllowanceSeconds != checkpointAvailabilityStallBudget.Seconds() {
		t.Fatal("advisory hid stall escalation evidence", alerts)
	}
	returns <- os.ErrPermission
	await(t, func() bool { return len(p.pending) == 1 })
	failed := p.sample(clock.Now(), baseline)
	if failed.Reason != "mount_probe_failed" || publish(failed) == "" {
		t.Fatal("failed operation admitted work", failed)
	}
	clock.Advance(p.nextProbe.Sub(clock.Now()))
	p.sample(clock.Now(), baseline)
	for i := 0; i < hostHealthConfirmation; i++ {
		// Mixed fast/slow successes establish recovery just as consecutive
		// fast successes do. Their advisory classifications may differ.
		latency.Store(int64(100 * time.Millisecond))
		if i%2 == 1 {
			latency.Store(int64(time.Millisecond))
		}
		returns <- nil
		await(t, func() bool { return len(p.pending) == 1 })
		clock.Advance(signalRefresh)
		recovered := p.sample(clock.Now(), baseline)
		reason := publish(recovered)
		if i < hostHealthConfirmation-1 {
			if reason == "" {
				t.Fatal("single completion cleared failure", recovered)
			}
			// Repeated pending advisory observations cannot count as recovery.
			for j := 0; j < hostHealthConfirmation+1; j++ {
				if publish(p.sample(clock.Now(), baseline)) == "" {
					t.Fatal("pending request proved recovery")
				}
			}
		} else if reason != "" || recovered.Reason != "mount_probe_slow" {
			t.Fatal("fresh successful outliers did not restore admission", recovered, reason)
		}
	}
}

func TestHostHygieneMountCachedPreFailureSuccessCannotRecover(t *testing.T) {
	_, clock, baseline, publish := mountAdmissionFixture(t, "checkpoint")
	release := make(chan struct{})
	p := &hostMountProbe{root: "checkpoint", path: "root", clock: clock, reason: "mount_probe_failed", last: HostMountHealth{Root: "checkpoint", LatencySeconds: .001}, probe: func(string, func()) error { <-release; return nil }}
	t.Cleanup(func() { close(release); await(t, func() bool { return len(p.pending) == 1 }) })
	for i := 0; i < hostHealthConfirmation+2; i++ {
		if got := p.sample(clock.Now(), baseline); publish(got) == "" {
			t.Fatal("cached pre-failure success counted as fresh recovery", got)
		}
	}
}
