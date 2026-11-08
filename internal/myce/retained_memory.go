package myce

import (
	"maps"
	"time"
)

// Reduce only immutable execution facts. Owner inventory, qualification, and
// time are checked by every reader; admission answers never cross a pass.
// Equal provenance/time entries combine by max, exactly as the original scan.
// Extension entries retain their own frozen bundle, even for equal digests.
type retainedMemoryProvenance struct {
	key       ObservationKey
	machine   string
	ended     time.Time
	extension *Job
}
type retainedMemoryEntry struct {
	provenance retainedMemoryProvenance
	evidence   memoryConstraintEvidence
}
type retainedMemoryIndex map[memoryHistoryKey]map[memoryWorkKey][]retainedMemoryEntry

func buildRetainedMemoryIndex(jobs map[string]*Job, skip map[string]*Job) retainedMemoryIndex {
	groups := map[memoryLowerKey]map[retainedMemoryProvenance]memoryConstraintEvidence{}
	for id, j := range jobs {
		if skip[id] != nil || j.Timing == nil || j.Timing.Key == nil || len(j.MemoryAttempts) == 0 {
			continue
		}
		ended := time.Time{}
		for _, outcome := range j.AttemptHistory {
			if outcome.Attempt == j.Timing.Execution.Attempt && outcome.Machine == j.Timing.Machine && outcome.Session == j.Timing.Session {
				ended = outcome.EndedAt
			}
		}
		if ended.IsZero() && j.StartedAt != nil && j.FinishedAt != nil {
			ended = *j.FinishedAt
		}
		if ended.IsZero() {
			continue
		}
		evidence := memoryConstraintEvidence{}
		valid := false
		for _, facts := range j.MemoryAttempts {
			p := facts.MemoryPeak
			if facts.Attempt != j.Timing.Execution.Attempt || j.Timing.Execution.ID != j.ID || facts.Machine != j.Timing.Machine || facts.Session != j.Timing.Session || p == nil || !p.constrained() || p.HostGB == nil || !validRate(*p.HostGB) || !memoryConstraintFacts(facts) {
				continue
			}
			valid = true
			peak := hostMemoryPeakGB(p)
			evidence.lower = max(evidence.lower, peak)
			margin := memoryGrowthGB(p)
			if envelope := facts.MemoryEnvelope; envelope != nil && envelope.HostGB > 0 {
				margin = max(margin, peak-envelope.HostGB)
			}
			if margin > 0 && (facts.LearningReason == "oom" || facts.LearningReason == "attempt_failed") {
				evidence.recovery = max(evidence.recovery, peak+margin)
			}
		}
		if !valid {
			continue
		}
		key := *j.Timing.Key
		work := memoryLowerIdentity(key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program))
		provenance := retainedMemoryProvenance{key: key, machine: j.Timing.Machine, ended: ended}
		if key.ExtensionDigest != "" {
			provenance.extension = j
		}
		if groups[work] == nil {
			groups[work] = map[retainedMemoryProvenance]memoryConstraintEvidence{}
		}
		old := groups[work][provenance]
		groups[work][provenance] = memoryConstraintEvidence{max(old.lower, evidence.lower), max(old.recovery, evidence.recovery)}
	}
	index := retainedMemoryIndex{}
	for work, entries := range groups {
		key := memoryHistoryKey{work.work.work, work.os}
		if index[key] == nil {
			index[key] = map[memoryWorkKey][]retainedMemoryEntry{}
		}
		for provenance, evidence := range entries {
			index[key][work.work] = append(index[key][work.work], retainedMemoryEntry{provenance, evidence})
		}
	}
	return index
}

func (c *memoryPlanningCache) retainedMemoryEvidence(q *Queue, key memoryLowerKey) memoryConstraintEvidence {
	history := q.memoryHistory()
	if c.history.index == nil {
		defer q.bodyPhaseFinish("retained_index", q.bodyPhaseStart())
		if history.records != nil && history.records.updating {
			mutable := maps.Clone(history.records.live)
			for id := range history.records.jobs {
				if j := history.s.Jobs[id]; j != nil {
					mutable[id] = j
				}
			}
			if history.retainedMemoryIndex == nil {
				history.memoryHistoryJobs(true)
				history.retainedMemoryIndex = buildRetainedMemoryIndex(history.s.Jobs, mutable)
			}
			// Live and touched cold jobs may acquire new execution facts in any
			// mutating pass. Never lend their derived facts to the next pass.
			c.history.index = buildRetainedMemoryIndex(mutable, nil)
			c.history.mergeRetainedIndex(history.retainedMemoryIndex)
		} else {
			c.history.index = buildRetainedMemoryIndex(history.memoryHistoryJobs(true), nil)
		}
	}
	evidence := memoryConstraintEvidence{}
	now := q.clock.Now()
	for _, entry := range c.history.index[memoryHistoryKey{key.work.work, key.os}][key.work] {
		p := entry.provenance
		if p.ended.Before(key.after) || now.Before(p.ended) {
			continue
		}
		m := c.history.retainedOwner(history, p, key.discrete)
		if m == nil {
			continue
		}
		if hostID(m) != p.key.Host || m.ExtensionFingerprint != p.key.Fingerprint || machineTarget(m) != p.key.Target {
			continue
		}
		if p.key.ExtensionDigest != "" {
			usable := false
			for _, e := range p.extension.Extensions {
				if e.Digest == p.key.ExtensionDigest {
					point := WorkObservation{Key: p.key}
					point.StaleReason = curveStaleReason(point, e, m)
					usable = memoryIdentityUsable(point)
				}
			}
			if !usable {
				continue
			}
		}
		evidence.lower = max(evidence.lower, entry.evidence.lower)
		evidence.recovery = max(evidence.recovery, entry.evidence.recovery)
	}
	return evidence
}
func (c *memoryHistoryCache) mergeRetainedIndex(cold retainedMemoryIndex) {
	for key, works := range cold {
		if c.index[key] == nil {
			c.index[key] = map[memoryWorkKey][]retainedMemoryEntry{}
		}
		for work, entries := range works {
			c.index[key][work] = append(c.index[key][work], entries...)
		}
	}
}

func (q *Queue) invalidateRetainedMemory() {
	q.retainedMemoryRevision++
	q.retainedMemoryIndex = nil
}

// Only execution/configuration fields used by the reduction are copied. Frozen
// copies let hashing and reduction run outside q.mu; cold writes/collection and
// rollback invalidate their generation before they can be installed.
func snapshotRetainedMemoryJobs(q *Queue) map[string]*Job {
	jobs := map[string]*Job{}
	for id, j := range q.s.Jobs {
		if q.records.live[id] != nil || j.Timing == nil || j.Timing.Key == nil || len(j.MemoryAttempts) == 0 {
			continue
		}
		source := Job{ID: j.ID, Spec: Spec{Needs: j.Spec.Needs, Args: j.Spec.Args, Program: j.Spec.Program},
			Timing: j.Timing, AttemptHistory: j.AttemptHistory, MemoryAttempts: j.MemoryAttempts,
			StartedAt: j.StartedAt, FinishedAt: j.FinishedAt, Extensions: j.Extensions}
		copy := cloneJob(&source)
		jobs[id] = &copy
	}
	return jobs
}

// Inventory views are derived once per immutable pass, independently of the
// number of retained executions. No owner view crosses a mutation or clock pass.
type retainedMemoryOwner struct {
	machine, component string
	discrete           bool
}

func (c *memoryHistoryCache) retainedOwner(q *Queue, p retainedMemoryProvenance, discrete bool) *Machine {
	key := retainedMemoryOwner{machine: p.machine}
	if discrete && p.key.Target.Platform != "cpu" {
		key.discrete = true
		key.component = p.key.Component
	}
	if m, ok := c.owners[key]; ok {
		return m
	}
	if c.owners == nil {
		c.owners = map[retainedMemoryOwner]*Machine{}
	}
	owner := q.s.Machines[p.machine]
	var m *Machine
	if owner != nil && !owner.Gone && !q.unreported[owner.ID] {
		m = owner
		if owner.Devices != nil {
			m = cpuMachineView(owner)
			if key.discrete && !owner.UnifiedMemory {
				m = allocationMachine(owner, []string{key.component})
				if m == owner {
					m = nil
				}
			}
		}
	}
	c.owners[key] = m
	return m
}
