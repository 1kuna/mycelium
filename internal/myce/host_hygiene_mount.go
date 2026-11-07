package myce

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync/atomic"
	"time"

	"mycelium/internal/probeguard"
)

// Read one directory entry, including EOF for an empty directory. Metadata alone
// can succeed from cache while a dead SMB connection cannot serve reads.
var workerHealthMountProbe = func(path string, progress func()) error {
	probeguard.Check()
	return readHealthMount(path, os.Stat, func(path string) (fs.ReadDirFile, error) { return os.Open(path) }, progress)
}

// Reuse checkpoint admission's exclusive create/close/remove access check.
// ReadDir(1) can enumerate a large SMB directory while opening its stream on
// macOS; health sampling must not repeatedly scan active checkpoint entries.
var workerHealthCheckpointProbe = func(path string, progress func()) error {
	probeguard.Check()
	return workerCheckpointAvailable(path, progress)
}

func readHealthMount(path string, stat func(string) (os.FileInfo, error), open func(string) (fs.ReadDirFile, error), progress func()) (err error) {
	info, err := stat(path)
	progress()
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("mount root is not a directory")
	}
	f, err := open(path)
	progress()
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
		progress()
	}()
	_, err = f.ReadDir(1)
	progress()
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// Fixed codes only: filesystem errors and mount sources may contain secrets.
type mountRecoveryError string

func (e mountRecoveryError) Error() string { return string(e) }

func mountHumanReason(osname string) mountRecoveryError {
	if osname == "darwin" {
		return "mount_macos_approval_required"
	}
	return "mount_linux_operator_required"
}

// Access is the supported, unprivileged automount trigger on both systems. We
// never invoke mount, sudo, Finder, AppleScript, or a credential helper. A failed
// trigger is retried by the same single-outstanding probe lane.
func probeRecoveryMount(path, required, osname string, probe func(string) error, inventory func() ([]nasMount, error)) error {
	mounts, err := inventory()
	if err != nil {
		return mountRecoveryError("mount_inventory_unavailable")
	}
	for _, m := range mounts {
		if withinNAS(m.root, path) && (required == "" || len(m.root) > len(required)) {
			required = m.root
		}
	}
	err = probe(path)
	if required == "" {
		return err
	} // Explicit local checkpoint roots stay local.
	// Re-read after access: an automount can have become a real network mount.
	mounts, invErr := inventory()
	if invErr != nil {
		return mountRecoveryError("mount_inventory_unavailable")
	}
	mounted, automount := false, false
	for _, m := range mounts {
		if withinNAS(m.root, required) {
			if m.kind == "autofs" {
				automount = true
			} else {
				mounted = true
			}
		}
	}
	if !mounted && !automount || errors.Is(err, os.ErrPermission) {
		return mountHumanReason(osname)
	}
	if !mounted && err == nil {
		return mountRecoveryError("mount_automount_pending")
	}
	return err
}

func healthMountAccess(role string) func(string, func()) error {
	if role == "checkpoint" {
		return workerHealthCheckpointProbe
	}
	return workerHealthMountProbe
}

func recoveryMountProbe(w Worker, role, path string) func(string, func()) error {
	probe := healthMountAccess(role)
	required := ""
	if w.ServiceConfig != nil && withinNAS(w.ServiceConfig.NAS, path) && w.ServiceConfig.NAS != "" {
		required = w.ServiceConfig.NAS
	}
	if w.rootMonitor != nil {
		w.rootMonitor.mu.Lock()
		if expected := w.rootMonitor.expected[role]; expected != "" {
			required = expected
		}
		w.rootMonitor.mu.Unlock()
	}
	return func(_ string, progress func()) error {
		if w.rootMonitor != nil {
			w.rootMonitor.mu.Lock()
			if expected := w.rootMonitor.expected[role]; expected != "" {
				required = expected
			}
			w.rootMonitor.mu.Unlock()
		}
		return probeRecoveryMount(path, required, runtime.GOOS, func(path string) error { return probe(path, progress) }, func() ([]nasMount, error) {
			probeguard.Check()
			defer progress()
			return mountedNASInventory(context.Background(), runtime.GOOS, func(ctx context.Context, _ string, _ ...string) ([]byte, error) {
				if runtime.GOOS == "linux" {
					return os.ReadFile("/proc/mounts")
				}
				return cachedMountInventory(ctx)
			})
		})
	}
}

type hostMountResult struct {
	latency    time.Duration
	err        error
	generation uint64
	dataProof  bool
}
type hostMountProbe struct {
	root, path  string
	clock       Clock
	progressed  atomic.Pointer[time.Time]
	probe       func(string, func()) error
	measure     func(string, func()) hostMountResult
	pending     chan hostMountResult
	started     time.Time
	last        HostMountHealth
	candidate   string
	consecutive int
	reason      string
	nextProbe   time.Time
	retryDelay  time.Duration
	storage     *hostHealthMonitor
	dataProbe   func(string, uint64, func()) error
}

func workerHealthMounts(w Worker) map[string]string {
	roots := map[string]string{}
	if w.CheckpointRoot != "" {
		roots["checkpoint"] = w.CheckpointRoot
	}
	if w.ServiceConfig != nil && w.ServiceConfig.NAS != "" {
		roots["nas"] = w.ServiceConfig.NAS
	}
	return roots
}

// An uninterruptible kernel request remains owned until completion. Sampling
// never waits for it and a userspace timeout never creates another request.
func (p *hostMountProbe) sample(now time.Time, baseline []HostHealthSample) HostMountHealth {
	base := slices.DeleteFunc(slices.Clone(baseline), func(s HostHealthSample) bool {
		return !slices.ContainsFunc(s.Mounts, func(m HostMountHealth) bool {
			return m.Root == p.root && m.LatencySeconds > 0 && m.PendingSeconds == 0 && m.Reason == "" && !m.Adverse
		})
	})
	// This learned whole-sequence band is advisory. Admission instead uses the
	// existing #234 per-operation no-progress allowance, including during warmup.
	limit := signalRefresh
	if len(base) > 0 {
		_, hi := healthBand(base, func(s HostHealthSample) float64 {
			for _, m := range s.Mounts {
				if m.Root == p.root {
					return m.LatencySeconds
				}
			}
			return 0
		})
		limit = max(time.Nanosecond, time.Duration(hi*float64(time.Second)))
	}
	completed := false
	if p.pending != nil {
		select {
		case r := <-p.pending:
			p.pending, completed = nil, true
			if p.storage != nil && !p.storage.storageResult(p, r, now) {
				completed = false
				break
			}
			p.last = HostMountHealth{Root: p.root, LatencySeconds: r.latency.Seconds()}
			if r.err != nil {
				p.last.Reason = "mount_probe_failed"
				var recovery mountRecoveryError
				if errors.As(r.err, &recovery) {
					p.last.Reason = string(recovery)
				}
				// Backoff starts at the existing observation cadence or measured cost.
				// Its ceiling is the existing signal freshness allowance (or a slower
				// host's observed cost), bounding recovery delay without a fleet constant.
				p.retryDelay = min(max(signalMaxAge, limit), max(max(signalRefresh, limit), p.retryDelay*2))
				p.nextProbe = now.Add(p.retryDelay)
			} else {
				p.retryDelay, p.nextProbe = 0, time.Time{}
			}
		default:
		}
	}
	result := p.last
	result.Root = p.root
	result.SlowThresholdSeconds = limit.Seconds()
	result.StallAllowanceSeconds = checkpointAvailabilityStallBudget.Seconds()
	if p.pending != nil {
		result.PendingSeconds = max(0, now.Sub(p.started).Seconds())
		progressed := p.started
		if at := p.progressed.Load(); at != nil {
			progressed = *at
		}
		result.OperationAgeSeconds = max(0, now.Sub(progressed).Seconds())
		if result.OperationAgeSeconds >= result.StallAllowanceSeconds {
			result.Reason = "mount_probe_stalled"
			return p.confirm(result, false)
		}
		// An unfinished read, even with progress, cannot clear a failed/stalled
		// episode. Recovery still requires fresh successful completions.
		if mountHealthWithholds(p.reason) {
			result.Reason, result.Adverse = p.reason, true
			return result
		}
		if result.LatencySeconds == 0 {
			result.Reason = "mount_probe_pending"
			return p.confirm(result, false)
		}
		if len(base) < hostHealthWarmup || result.PendingSeconds <= limit.Seconds() {
			result.Reason = p.reason
			return result
		}
		result.Reason = "mount_probe_slow"
		return p.confirm(result, false)
	}
	if completed && len(base) >= hostHealthWarmup && result.LatencySeconds > limit.Seconds() && result.Reason == "" {
		result.Reason = "mount_probe_slow"
	}
	if !now.Before(p.nextProbe) {
		p.pending, p.started = make(chan hostMountResult, 1), now
		p.progressed.Store(&now)
		done, path, probe, measure := p.pending, p.path, p.probe, p.measure
		generation, data := p.faultGeneration(), false
		if state := p.backingFault(); state != nil && state.fault.Active && state.fault.Data && state.owner == p.root {
			data = true
			path = state.source
			cycle := state.cycle
			state.cycle++
			read := p.dataProbe
			if read == nil {
				read = workerStorageReopen
			}
			probe = func(path string, progress func()) error { return read(path, cycle, progress) }
		}
		clock := p.clock
		if clock == nil {
			clock = RealClock{}
		}
		go func() {
			progress := func() {
				at := clock.Now()
				p.progressed.Store(&at)
			}
			if measure != nil && !data {
				result := measure(path, progress)
				result.generation = generation
				done <- result
				return
			}
			started := time.Now() // OS elapsed I/O measurement, never a scheduling clock.
			err := probe(path, progress)
			done <- hostMountResult{latency: time.Since(started), err: err, generation: generation, dataProof: data}
		}()
	}
	if !completed && result.LatencySeconds == 0 && result.Reason == "" {
		result.Reason = "mount_probe_pending"
	}
	// A cached success is not fresh recovery evidence.
	if !completed && result.Reason == "" {
		result.Reason = p.reason
		return result
	}
	return p.confirm(result, completed)
}

func mountHealthWithholds(reason string) bool {
	return reason != "" && reason != "mount_probe_slow"
}

func (p *hostMountProbe) confirm(result HostMountHealth, fresh bool) HostMountHealth {
	result.Adverse = result.Reason != ""
	if result.Reason != "" && result.Reason != "mount_probe_slow" && result.Reason != "mount_probe_stalled" {
		p.reason, p.candidate, p.consecutive = result.Reason, result.Reason, 0
		return result
	}
	candidate := result.Reason
	if mountHealthWithholds(p.reason) && !mountHealthWithholds(candidate) {
		if !fresh {
			result.Reason = p.reason
			return result
		}
		// Fast and advisory fresh completions both establish access;
		// alternating them must not reset successful recovery evidence.
		candidate = "recovered"
	}
	if candidate != p.candidate {
		p.candidate, p.consecutive = candidate, 0
	}
	p.consecutive++
	if p.consecutive >= hostHealthConfirmation {
		p.reason = result.Reason
	}
	result.Reason = p.reason
	return result
}

// Reuse dependency-specific withholding instead of a global NAS drain.
func applyHostMountHealth(w Worker, m *Machine) {
	if m.HostHealth == nil || len(m.HostHealth.History) == 0 {
		return
	}
	s := m.HostHealth.History[len(m.HostHealth.History)-1]
	mark := func(role, reason string) {
		for i := range m.UnavailableRoots {
			if m.UnavailableRoots[i].Root == role {
				m.UnavailableRoots[i].Reason = reason
				return
			}
		}
		m.UnavailableRoots = append(m.UnavailableRoots, UnavailableRoot{Root: role, Reason: reason})
	}
	for _, probe := range s.Mounts {
		if !mountHealthWithholds(probe.Reason) {
			continue
		}
		if probe.Root != "nas" {
			mark(probe.Root, probe.Reason)
			continue
		}
		if probe.Root == "nas" && w.ServiceConfig != nil {
			roots := map[string]string{"job-files": w.JobFileRoot, "model": w.ModelRoot, "model-cache": w.CacheRoot, "environment-source": w.EnvironmentSourceRoot, "environment": w.EnvironmentRoot, "file-cache": w.FileCacheRoot, "extension": w.ExtensionRoot}
			backings := w.storageRoleBackings(roots)
			for role, path := range roots {
				if path != "" && withinNAS(w.ServiceConfig.NAS, path) && backings[role] == filepath.Clean(w.ServiceConfig.NAS) {
					mark(role, probe.Reason)
				}
			}
		}
	}
}
