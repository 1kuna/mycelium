package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"time"
)

// Reuse the process-memory observer. Three observations over two periods
// distinguish sustained growth from a brief spike (such as a fork's shared
// pages before exec) without another polling loop. processMemorySamplePeriod
// is per platform: it follows what the process probe costs to run.
const processMemoryGrace = 2 * processMemorySamplePeriod
const rssLimitExceeded = "rss_limit_exceeded"
const darwinRSSGuardStatus = "sampled RSS guard (native CPU); no kernel memory ceiling"

type RSSLimitEvidence struct {
	ObservedBytes uint64 `json:"observed_bytes"`
	LimitBytes    int64  `json:"limit_bytes"`
	Quality       string `json:"quality"`
}

func safeRSSLimit(e *RSSLimitEvidence) *RSSLimitEvidence {
	if e == nil || e.LimitBytes <= 0 || e.ObservedBytes <= uint64(e.LimitBytes) || e.Quality != "owned_session_rss" && e.Quality != "cgroup_current" {
		return nil
	}
	copy := *e
	return &copy
}

func (e *RSSLimitEvidence) String() string {
	if e.Quality == "cgroup_current" {
		return fmt.Sprintf("sampled cgroup memory %d bytes exceeded resolved limit %d bytes (%s)", e.ObservedBytes, e.LimitBytes, e.Quality)
	}
	return fmt.Sprintf("sampled RSS %d bytes exceeded resolved limit %d bytes (%s)", e.ObservedBytes, e.LimitBytes, e.Quality)
}

type rssGuard struct {
	limit       int64
	roots       map[int]string
	since, last time.Time
	samples     int
	exceeded    bool
}

func (g *rssGuard) observe(now time.Time, roots map[int]string, bytes uint64, members int, err error) *RSSLimitEvidence {
	if g.exceeded || g.limit <= 0 {
		return nil
	}
	valid := len(roots) > 0 && members > 0 && err == nil
	for pid, birth := range roots {
		valid = valid && pid > 0 && birth != ""
	}
	if !valid || bytes <= uint64(g.limit) {
		g.samples = 0
		return nil
	}
	// A replaced supervisor or backward clock starts a new window; a failed or
	// under-limit observation already did. A late sample is not evidence of a
	// dip: sampling slowed by host load, which is when a runaway thrashes the
	// host, must not postpone enforcement indefinitely.
	if g.samples == 0 || !maps.Equal(roots, g.roots) || now.Before(g.last) {
		g.since, g.samples, g.roots = now, 0, maps.Clone(roots)
	}
	g.last = now
	g.samples++
	if g.samples < 3 || now.Sub(g.since) < processMemoryGrace {
		return nil
	}
	g.exceeded = true
	return &RSSLimitEvidence{ObservedBytes: bytes, LimitBytes: g.limit, Quality: "owned_session_rss"}
}

func (p *process) rssRoots() map[int]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rssRootsLocked()
}

func (p *process) rssRootsLocked() map[int]string {
	if p.native != nil && p.native.supervisor != nil {
		pid := p.native.supervisor.Process.Pid
		return map[int]string{pid: p.native.custody.Groups[pid][pid]}
	}
	if p.cmd != nil && p.cmd.Process != nil {
		return map[int]string{p.cmd.Process.Pid: p.birth}
	}
	return nil
}

// Persist the resource fact before using the ordinary exact-attempt stop and
// cleanup path. A completed/replaced execution cannot inherit a late sample.
func (p *process) stopRSS(now time.Time, roots map[int]string, evidence *RSSLimitEvidence) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !maps.Equal(roots, p.rssRootsLocked()) || p.outcome.RSSLimit != nil {
		return false
	}
	observation := &p.exitObservation
	if p.native != nil && p.native.main != nil {
		if p.native.main.exitObservation == nil {
			p.native.main.exitObservation = new(nativeExitObservation)
		}
		observation = p.native.main.exitObservation
	}
	// Receipt commit and native exit observation arbitrate under the same lock.
	// Release it before stopLocked, whose signal path takes this lock itself.
	observation.mu.Lock()
	accepted := func() bool {
		if observation.exit != nil || p.outcome.Exit != nil || p.native != nil && p.native.main != nil && p.native.main.Exit != nil {
			return false
		}
		return p.recordRSSLocked(roots, evidence)
	}()
	observation.mu.Unlock()
	if !accepted {
		return false
	}
	p.stopLocked(now, 0, true)
	return true
}

func (p *process) recordRSSLocked(roots map[int]string, evidence *RSSLimitEvidence) bool {
	for root, saved := range roots {
		birth, err := processBirth(root)
		if err != nil || birth != saved {
			return false
		}
	}
	next := p.outcome
	next.RSSLimit = safeRSSLimit(evidence)
	if next.RSSLimit == nil {
		return false
	}
	if p.native != nil {
		p.outcome = next
		if p.native.saveLocked() != nil {
			p.outcome.RSSLimit = nil
			return false
		}
	} else {
		if p.saveOutcome == nil || p.saveOutcome(next) != nil {
			return false
		}
		p.outcome = next
	}
	return true
}

// Recover the persisted limit fact even if the worker died between delivery
// and the final result. Cleanup/release is still performed by normal recovery.
func (s *workerStore) recoverRSSCause(machine string) error {
	if s.state.Result != nil {
		return nil
	}
	for _, native := range []bool{false, true} {
		name := s.state.Identity + ".outcome.json"
		if native {
			name = "native-" + s.state.Identity + ".json"
		}
		data, err := os.ReadFile(filepath.Join(s.dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		var facts memberOutcome
		if native {
			var record struct {
				Identity string
				Outcome  memberOutcome
			}
			if err := json.Unmarshal(data, &record); err != nil {
				return err
			}
			if record.Identity != s.state.Identity {
				return errors.New("mismatched RSS outcome custody")
			}
			facts = record.Outcome
		} else if err := json.Unmarshal(data, &facts); err != nil {
			return err
		}
		if safeRSSLimit(facts.RSSLimit) == nil {
			continue
		}
		r := classifyAttemptOutcome(Result{Machine: machine, Session: s.state.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, ReleasePending: true}, attemptCause{memberOutcome: facts})
		if err := saveWorkerJSON(s.dir, s.state.Identity+".result.json", r); err != nil {
			return err
		}
		s.state.Result = &r
		return nil
	}
	return nil
}
