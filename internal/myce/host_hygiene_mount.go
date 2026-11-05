package myce

import (
	"os"
	"slices"
	"time"

	"mycelium/internal/probeguard"
)

var workerHealthMountProbe = func(path string) error { probeguard.Check(); _, err := os.Stat(path); return err }

type hostMountResult struct {
	latency time.Duration
	err     error
}
type hostMountProbe struct {
	root, path  string
	probe       func(string) error
	pending     chan hostMountResult
	started     time.Time
	last        HostMountHealth
	candidate   string
	consecutive int
	reason      string
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
	if p.pending != nil {
		select {
		case r := <-p.pending:
			p.pending = nil
			p.last = HostMountHealth{Root: p.root, LatencySeconds: r.latency.Seconds()}
			if r.err != nil {
				p.last.Reason = "mount_probe_failed"
			}
		default:
		}
	}
	base := slices.DeleteFunc(slices.Clone(baseline), func(s HostHealthSample) bool {
		return !slices.ContainsFunc(s.Mounts, func(m HostMountHealth) bool {
			return m.Root == p.root && m.LatencySeconds > 0 && m.PendingSeconds == 0 && m.Reason == ""
		})
	})
	limit := 0.0
	if len(base) >= hostHealthWarmup {
		_, limit = healthBand(base, func(s HostHealthSample) float64 {
			for _, m := range s.Mounts {
				if m.Root == p.root {
					return m.LatencySeconds
				}
			}
			return 0
		})
	}
	result := p.last
	result.Root = p.root
	if p.pending != nil {
		result.PendingSeconds = max(0, now.Sub(p.started).Seconds())
		if limit > 0 && result.PendingSeconds > limit {
			result.Reason = "mount_probe_stalled"
		}
		return p.confirm(result)
	}
	if limit > 0 && result.LatencySeconds > limit && result.Reason == "" {
		result.Reason = "mount_probe_slow"
	}
	p.pending, p.started = make(chan hostMountResult, 1), now
	done, path, probe := p.pending, p.path, p.probe
	go func() { started := time.Now(); err := probe(path); done <- hostMountResult{time.Since(started), err} }()
	return p.confirm(result)
}

func (p *hostMountProbe) confirm(result HostMountHealth) HostMountHealth {
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
	for _, probe := range s.Mounts {
		if probe.Reason == "" {
			continue
		}
		if probe.Root == "checkpoint" {
			m.UnavailableRoots = append(m.UnavailableRoots, UnavailableRoot{Root: "checkpoint", Reason: probe.Reason})
			continue
		}
		if probe.Root == "nas" && w.ServiceConfig != nil {
			for role, path := range map[string]string{"job-files": w.JobFileRoot, "model": w.ModelRoot, "model-cache": w.CacheRoot, "environment-source": w.EnvironmentSourceRoot, "environment": w.EnvironmentRoot, "file-cache": w.FileCacheRoot, "extension": w.ExtensionRoot} {
				if path != "" && withinNAS(w.ServiceConfig.NAS, path) {
					m.UnavailableRoots = append(m.UnavailableRoots, UnavailableRoot{Root: role, Reason: probe.Reason})
				}
			}
		}
	}
}
