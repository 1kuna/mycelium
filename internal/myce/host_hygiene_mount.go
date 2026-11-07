package myce

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"runtime"
	"slices"
	"time"

	"mycelium/internal/probeguard"
)

// Read one directory entry, including EOF for an empty directory. Metadata alone
// can succeed from cache while a dead SMB connection cannot serve reads.
var workerHealthMountProbe = func(path string) error {
	probeguard.Check()
	return readHealthMount(path, os.Stat, func(path string) (fs.ReadDirFile, error) { return os.Open(path) })
}

// Reuse checkpoint admission's exclusive create/close/remove access check.
// ReadDir(1) can enumerate a large SMB directory while opening its stream on
// macOS; health sampling must not repeatedly scan active checkpoint entries.
var workerHealthCheckpointProbe = func(path string) error {
	probeguard.Check()
	return workerCheckpointAvailable(path, func() {})
}

func readHealthMount(path string, stat func(string) (os.FileInfo, error), open func(string) (fs.ReadDirFile, error)) error {
	info, err := stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("mount root is not a directory")
	}
	f, err := open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.ReadDir(1)
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

func healthMountAccess(role string) func(string) error {
	if role == "checkpoint" {
		return workerHealthCheckpointProbe
	}
	return workerHealthMountProbe
}

func recoveryMountProbe(w Worker, role, path string) func(string) error {
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
	return func(string) error {
		if w.rootMonitor != nil {
			w.rootMonitor.mu.Lock()
			if expected := w.rootMonitor.expected[role]; expected != "" {
				required = expected
			}
			w.rootMonitor.mu.Unlock()
		}
		return probeRecoveryMount(path, required, runtime.GOOS, probe, func() ([]nasMount, error) {
			probeguard.Check()
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
	latency time.Duration
	err     error
}
type hostMountProbe struct {
	root, path  string
	probe       func(string) error
	measure     func(string) hostMountResult
	pending     chan hostMountResult
	started     time.Time
	last        HostMountHealth
	candidate   string
	consecutive int
	reason      string
	nextProbe   time.Time
	timing      probeTiming
	retryDelay  time.Duration
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
	// Cold start gets one observation cadence. Measured samples can detect a
	// pending stall immediately, but completed successes train through warmup:
	// one unusually fast read must not freeze all later successful evidence.
	limit := p.timing.controlPeriod()
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
			p.last = HostMountHealth{Root: p.root, LatencySeconds: r.latency.Seconds()}
			if r.err != nil {
				p.last.Reason = "mount_probe_failed"
				var recovery mountRecoveryError
				if errors.As(r.err, &recovery) {
					p.last.Reason = string(recovery)
				}
				// Failed attempts spread retries by their observed cost and repeated
				// failure spacing. No universal mount-recovery deadline is assumed.
				p.retryDelay = max(limit, max(r.latency, p.retryDelay*2))
				p.nextProbe = now.Add(p.retryDelay)
			} else {
				p.retryDelay, p.nextProbe = 0, time.Time{}
			}
		default:
		}
	}
	result := p.last
	result.Root = p.root
	if p.pending != nil {
		result.PendingSeconds = max(0, now.Sub(p.started).Seconds())
		if result.PendingSeconds > limit.Seconds() {
			result.Reason = "mount_probe_stalled"
		} else if result.LatencySeconds == 0 && result.Reason == "" {
			result.Reason = "mount_probe_pending"
		}
		if result.Reason == "" {
			result.Reason = p.reason
			return result
		}
		return p.confirm(result)
	}
	if completed && len(base) >= hostHealthWarmup && result.LatencySeconds > limit.Seconds() && result.Reason == "" {
		result.Reason = "mount_probe_slow"
	}
	if !now.Before(p.nextProbe) {
		p.pending, p.started = make(chan hostMountResult, 1), now
		done, path, probe, measure := p.pending, p.path, p.probe, p.measure
		go func() {
			if measure != nil {
				done <- measure(path)
				return
			}
			started := time.Now() // OS elapsed I/O measurement, never a scheduling clock.
			err := probe(path)
			done <- hostMountResult{time.Since(started), err}
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
	return p.confirm(result)
}

func (p *hostMountProbe) confirm(result HostMountHealth) HostMountHealth {
	result.Adverse = result.Reason != ""
	if result.Reason != "" && result.Reason != "mount_probe_slow" && result.Reason != "mount_probe_stalled" {
		p.reason, p.candidate, p.consecutive = result.Reason, result.Reason, 0
		return result
	}
	if result.Reason != p.candidate {
		p.candidate, p.consecutive = result.Reason, 0
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
		if probe.Reason == "" {
			continue
		}
		if probe.Root == "checkpoint" {
			mark("checkpoint", probe.Reason)
			continue
		}
		if probe.Root == "nas" && w.ServiceConfig != nil {
			for role, path := range map[string]string{"job-files": w.JobFileRoot, "model": w.ModelRoot, "model-cache": w.CacheRoot, "environment-source": w.EnvironmentSourceRoot, "environment": w.EnvironmentRoot, "file-cache": w.FileCacheRoot, "extension": w.ExtensionRoot} {
				if path != "" && withinNAS(w.ServiceConfig.NAS, path) {
					mark(role, probe.Reason)
				}
			}
		}
	}
}
