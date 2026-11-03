package myce

import (
	"sort"
	"time"
)

type memoryRecovery struct {
	point WorkObservation
}

// Older completions erased accepted aggregates even for interrupted launches.
// Reconstruct only invalidated rows from exact retained accepted samples. This
// is a read projection, not a migration or a write to the learning ledger.
func (q *Queue) recoveredMemoryObservations() map[ObservationKey]memoryRecovery {
	wanted := map[ObservationKey]WorkObservation{}
	wantedWork := map[memoryWorkKey]WorkObservation{}
	for _, p := range q.s.WorkObservations {
		if p.MemoryConfiguration != "" && p.MemoryRecordedAt.IsZero() {
			wanted[p.Key] = p
			wantedWork[memoryWorkIdentity(p.Key, p.MemoryConfiguration)] = p
		}
	}
	recovered := map[ObservationKey]memoryRecovery{}
	if len(wanted) == 0 {
		return recovered
	}
	type failures struct {
		seen          bool
		constrainedAt time.Time
	}
	failed := map[memoryWorkKey]failures{}
	// Match accepted writer order, including corrected attribution replacing
	// legacy peaks rather than merging an inflated predecessor back into them.
	jobs := make([]*Job, 0, len(q.s.Jobs))
	for _, j := range q.s.Jobs {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(i, j int) bool {
		a, b := jobs[i].FinishedAt, jobs[j].FinishedAt
		if a == nil {
			return false
		}
		if b == nil {
			return true
		}
		if a.Equal(*b) {
			return jobs[i].ID < jobs[j].ID
		}
		return a.Before(*b)
	})
	for _, j := range jobs {
		checked := map[memoryWorkKey]bool{}
		vetoRealFailures := func(base WorkObservation) {
			work := memoryWorkIdentity(base.Key, base.MemoryConfiguration)
			if checked[work] {
				return
			}
			checked[work] = true
			// An identical constrained peak can invalidate without changing the
			// aggregate, hence without emitting a learning-mutation entry. These
			// outcomes only veto recovery; they never establish learned peaks.
			for _, outcome := range j.AttemptHistory {
				if outcome.State == "succeeded" {
					continue
				}
				constrained := false
				for _, facts := range j.MemoryAttempts {
					if facts.Attempt == outcome.Attempt && facts.Machine == outcome.Machine && facts.Session == outcome.Session && facts.MemoryPeak.constrained() && facts.MemoryPeak.Source != processGroupMemorySource {
						constrained = true
					}
				}
				r := Result{Interrupted: outcome.Interrupted, OutOfMemory: outcome.OutOfMemory, ExecutionCause: outcome.ExecutionCause}
				if outcome.OutOfMemory || constrained && memoryConstraintResult(true, r) {
					f := failed[work]
					if outcome.EndedAt.After(f.constrainedAt) {
						f.constrainedAt = outcome.EndedAt
					}
					failed[work] = f
				}
			}
		}
		// Invalidation entries keep the rewritten key and execution after retry
		// replaces Timing. Pair only with that execution's retained outcome.
		for _, entry := range j.LedgerEntries {
			if entry.Kind != "work_learning" || entry.Reason != "memory_invalidated" || entry.WorkLearning == nil {
				continue
			}
			base, ok := wanted[entry.WorkLearning.Key]
			if !ok {
				continue
			}
			work := memoryWorkIdentity(base.Key, base.MemoryConfiguration)
			vetoRealFailures(base)
			matched := false
			for _, outcome := range j.AttemptHistory {
				if outcome.Attempt != entry.Attempt || outcome.Machine != entry.Machine || outcome.Session != entry.Session || outcome.State == "succeeded" {
					continue
				}
				f := failed[work]
				matched = true
				f.seen = true
				r := Result{Interrupted: outcome.Interrupted, OutOfMemory: outcome.OutOfMemory, ExecutionCause: outcome.ExecutionCause}
				if (outcome.OutOfMemory || memoryConstraintResult(true, r)) && outcome.EndedAt.After(f.constrainedAt) {
					f.constrainedAt = outcome.EndedAt
				}
				failed[work] = f
			}
			if !matched {
				f := failed[work]
				if entry.At.After(f.constrainedAt) {
					f.constrainedAt = entry.At // An expired outcome cannot prove interruption.
				}
				failed[work] = f
			}
		}
		if j.Timing == nil || j.Timing.Key == nil {
			continue
		}
		k := *j.Timing.Key
		configuration := memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
		if base, ok := wantedWork[memoryWorkIdentity(k, configuration)]; ok {
			vetoRealFailures(base)
		}
		for _, facts := range j.MemoryAttempts {
			if facts.Attempt != j.Timing.Execution.Attempt || facts.Machine != j.Timing.Machine || facts.Session != j.Timing.Session || j.Timing.Execution.ID != j.ID {
				continue
			}
			p := facts.MemoryPeak
			if p == nil {
				continue
			}
			base, ok := wanted[k]
			if !ok || base.MemoryConfiguration != configuration || facts.LearningReason != "eligible" || j.State != "succeeded" || j.FinishedAt == nil || p.constrained() || p.PressureUnavailable || p.HostGB == nil || !validRate(*p.HostGB) {
				continue
			}
			age := q.clock.Now().Sub(*j.FinishedAt)
			if age < 0 || age > workCurveMaxAge {
				continue
			}
			recovery, exists := recovered[k]
			if !exists {
				base.MemoryPeak, base.MemoryRange, base.GPUHostRange = nil, nil, nil
				base.MemorySamples = 0
				recovery.point = base
			}
			old := recovery.point.MemoryPeak
			corrected := p.GPUHost != nil && p.GPUHost.SiblingAdjusted
			oldCorrected := old != nil && old.GPUHost != nil && old.GPUHost.SiblingAdjusted
			if oldCorrected && p.GPUHost != nil && !corrected {
				continue
			}
			if corrected && !oldCorrected {
				recovery.point.MemoryPeak = nil
				recovery.point.MemorySamples = 0
			}
			recovery.point.MemoryPeak = mergeMemoryPeaks(recovery.point.MemoryPeak, p)
			recovery.point.MemorySamples++
			if j.FinishedAt.After(recovery.point.MemoryRecordedAt) {
				recovery.point.MemoryRecordedAt = *j.FinishedAt
			}
			recovered[k] = recovery
		}
	}
	for k, recovery := range recovered {
		f := failed[memoryWorkIdentity(k, recovery.point.MemoryConfiguration)]
		if !f.seen {
			delete(recovered, k) // Unknown invalidation is not proof of interruption.
			continue
		}
		if !f.constrainedAt.IsZero() && !f.constrainedAt.Before(recovery.point.MemoryRecordedAt) {
			delete(recovered, k) // Real constrained/OOM completions retain existing exploration.
			continue
		}
		recovered[k] = recovery
	}
	return recovered
}
