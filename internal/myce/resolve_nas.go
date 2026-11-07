package myce

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

// Diagnostic evidence expires with the worker's existing session freshness
// fence, never a universal cache minute. Pending work is bounded by measured
// queue memory. One check per heartbeat matches the worker's one syscall slot;
// sending larger batches cannot increase its physical processing concurrency.

type NASPathCheck struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}
type NASPathReport struct {
	AgeSeconds float64 `json:"age_seconds"`
	ID         string  `json:"id"`
	State      string  `json:"state"` // present, missing, unverified
}
type ResolveNASInput struct {
	Name       string    `json:"name"`
	State      string    `json:"state"`
	ObservedAt time.Time `json:"observed_at,omitempty"` // queue receipt, not worker wall time
}
type resolveNASEntry struct {
	check             NASPathCheck
	machine, session  string
	expires, observed time.Time
	state             string
	lifetime          time.Duration
}
type resolveNASCache struct {
	mu      sync.Mutex
	clock   Clock
	entries map[string]*resolveNASEntry
	budgets *requestBudgets
}

func (q *Queue) resolveNASInspections() *resolveNASCache {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.nasInspections == nil {
		q.nasInspections = newResolveNASCache(q.clock, &q.requestBudgets)
	}
	return q.nasInspections
}

func newResolveNASCache(clock Clock, budgets ...*requestBudgets) *resolveNASCache {
	b := &requestBudgets{}
	if len(budgets) > 0 {
		b = budgets[0]
	}
	return &resolveNASCache{clock: clock, budgets: b, entries: map[string]*resolveNASEntry{}}
}
func (c *resolveNASCache) expire(now time.Time) {
	for key, entry := range c.entries {
		if !now.Before(entry.expires) {
			delete(c.entries, key)
		}
	}
}
func (c *resolveNASCache) resolve(req ResolveRequest, report *ResolveReport) {
	capacity, _ := c.budgets.capacity(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	c.expire(now)
	var used int64
	for key, entry := range c.entries {
		used += int64(len(key)+len(entry.check.ID)+len(entry.check.Path)) + int64(reflect.TypeFor[resolveNASEntry]().Size())
	}
	for i := range report.Targets {
		target := &report.Targets[i]
		for _, name := range sortedPortabilityKeys(req.Spec.SharedInputs) {
			input := req.Spec.SharedInputs[name]
			if input.NAS == "" {
				continue
			}
			evidence := ResolveNASInput{Name: name, State: "unverified"}
			key := identityHash([]string{target.Machine, target.session, input.NAS})
			entry := c.entries[key]
			if req.InspectNAS && target.nasInspection && target.Freshness == "fresh" && entry == nil && used+int64(len(key)+len(input.NAS))+int64(reflect.TypeFor[resolveNASEntry]().Size()) < capacity {
				entry = &resolveNASEntry{check: NASPathCheck{ID: ID(), Path: input.NAS}, machine: target.Machine, session: target.session, expires: target.freshUntil, lifetime: target.freshUntil.Sub(now), state: "pending"}
				c.entries[key] = entry
				used += int64(len(key)+len(entry.check.ID)+len(entry.check.Path)) + int64(reflect.TypeFor[resolveNASEntry]().Size())
			}
			if entry != nil && target.Freshness == "fresh" {
				evidence.State, evidence.ObservedAt = entry.state, entry.observed
			}
			target.NASInputs = append(target.NASInputs, evidence)
			if evidence.State == "missing" {
				report.Findings = append(report.Findings, ResolveFinding{Field: "shared_inputs." + name + ".nas", Side: "worker", Problem: fmt.Sprintf("%s reports the path missing under its checkpoint root (existence only; bytes unverified)", target.Machine), Fix: "Correct the checkpoint-root-relative path, or import with profile local: or myce file put and omit nas."})
				if !target.cachedInputs[input.key()] {
					target.Eligible = false
					target.Reason = "blocked: worker reports missing NAS shared input " + name
				}
			}
		}
	}
	if req.InspectNAS {
		report.Notes = append(report.Notes, "NAS inspection is optional, authenticated, metadata-only and cached until the worker session freshness fence; pending/unverified is unknown. For one path, repeat after two successful worker heartbeats: the first delivers the check and the next returns its observation. More paths or slow metadata can require further heartbeats. Only declared shared_inputs.nas paths are inspected under the worker checkpoint root. No bytes, hashes or runtime readiness are verified; submission and staging revalidate. Workers need the inspection-capable release.")
	}
}
func (c *resolveNASCache) checkin(in Checkin) []NASPathCheck {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	c.expire(now)
	reports := map[string]NASPathReport{}
	for _, r := range in.NASPathReports {
		if !math.IsNaN(r.AgeSeconds) && !math.IsInf(r.AgeSeconds, 0) && r.AgeSeconds >= 0 && r.AgeSeconds <= float64(math.MaxInt64)/float64(time.Second) && (r.State == "present" || r.State == "missing" || r.State == "unverified") {
			reports[r.ID] = r
		}
	}
	checks := []NASPathCheck{}
	for _, key := range sortedPortabilityKeys(c.entries) {
		entry := c.entries[key]
		if entry.machine != in.Machine.ID || entry.session != in.Machine.Session {
			continue
		}
		if entry.state == "pending" {
			if state, ok := reports[entry.check.ID]; ok {
				entry.state, entry.observed = state.State, now.Add(-time.Duration(state.AgeSeconds*float64(time.Second)))
				entry.expires = entry.observed.Add(entry.lifetime)
			} else if len(checks) == 0 {
				checks = append(checks, entry.check)
			}
		}
	}
	return checks
}

type workerNASInspection struct {
	sampled map[string]time.Time
	checks  []NASPathCheck
	reports []NASPathReport
	probe   checkpointIO[string]
	root    string
}

func (w *Worker) inspectNASPaths(checks []NASPathCheck, root string) {
	if w.nasInspection == nil {
		w.nasInspection = &workerNASInspection{sampled: map[string]time.Time{}, probe: checkpointIO[string]{timeout: localObservationBudget}}
	}
	s := w.nasInspection
	// Reports were delivered successfully by this heartbeat; discard old results.
	s.reports = nil
	sampled := map[string]time.Time{}
	if s.root == root {
		for _, check := range checks {
			sampled[check.ID] = s.sampled[check.ID]
		}
	}
	s.sampled = sampled
	s.root = root
	s.checks = slices.Clone(checks)
}
func (w *Worker) nasPathReports() []NASPathReport {
	s := w.nasInspection
	if s == nil {
		return nil
	}
	// One operation each heartbeat costs at most the existing 25ms observation
	// budget. A stuck syscall retains one slot and never stalls stop delivery.
	if len(s.checks) > 0 {
		check := s.checks[0]
		root := s.root
		if s.sampled[check.ID].IsZero() {
			s.sampled[check.ID] = w.Clock.Now()
		}
		state, err := s.probe.run(check.ID, func() (string, error) { return inspectNASPath(root, check.Path), nil })
		if err == nil {
			s.reports = append(s.reports, NASPathReport{ID: check.ID, State: state})
			s.checks = s.checks[1:]
		}
	}
	reports := slices.Clone(s.reports)
	for i := range reports {
		reports[i].AgeSeconds = max(0, w.Clock.Now().Sub(s.sampled[reports[i].ID]).Seconds())
	}
	return reports
}
func inspectNASPath(root, path string) string {
	if root == "" || !safeRelative(path) {
		return "unverified"
	}
	// Lstat each component: never follow a symlink outside the checkpoint root.
	current := root
	parts := strings.Split(filepath.Clean(path), string(filepath.Separator))
	for _, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			return "missing"
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return "unverified"
		}
	}
	return "present"
}
