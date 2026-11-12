package myce

import (
	"slices"
	"strings"
)

const memoryEvidenceRecovering = "memory_evidence_recovering"

const measuredOOMRecoverySource = "exploratory OOM recovery from measured demand"

func measuredOOMRecovery(j *Job) (bound, lower float64) {
	bound, lower = minimumHostMemoryGB, minimumHostMemoryGB
	if f := j.MemoryOOMFloor; f != nil {
		bound = max(bound, f.HostMemoryGB)
		lower = max(lower, f.HostMemoryGB)
	}
	for _, facts := range j.MemoryAttempts {
		if facts.LearningReason != "oom" || facts.MemoryPeak == nil || facts.MemoryPeak.HostGB == nil || !validRate(*facts.MemoryPeak.HostGB) {
			continue
		}
		peak := hostMemoryPeakGB(facts.MemoryPeak)
		lower = max(lower, peak)
		margin := max(memoryGrowthGB(facts.MemoryPeak), minimumHostMemoryGB)
		if e := facts.MemoryEnvelope; e != nil && validRate(e.HostGB) {
			margin = max(margin, peak-e.HostGB)
		}
		// The durable floor already retains the previous host demand. The
		// historical envelope is a physical total, potentially including a
		// device share; use it only to measure observed ceiling overshoot.
		bound = max(bound, peak+margin)
	}
	return bound, lower
}

// Qualification may defer uncertain sizing, never the author's requirements,
// member containment or an attempt's durable OOM/device retry floor. This copy
// is only a diagnostic; dispatch still uses the complete resolved envelope.
func memoryIntent(j *Job) *Job {
	copy := *j
	copy.MemoryEnvelope = nil
	copy.Spec.Needs.Memory = ""
	n := &copy.Spec.Needs
	n.HostMemoryGB = max(n.HostMemoryGB, minimumHostMemoryGB, j.Spec.Run.MemoryGB)
	for _, side := range j.Spec.Run.Sidecars {
		n.HostMemoryGB = max(n.HostMemoryGB, side.MemoryGB)
	}
	if floor := j.MemoryOOMFloor; floor != nil {
		n.HostMemoryGB = max(n.HostMemoryGB, floor.HostMemoryGB)
		n.MemoryGB = max(n.MemoryGB, floor.MemoryGB)
	}
	n.MemoryGB = max(n.MemoryGB, j.DeviceMemoryFloorGB)
	return &copy
}

func memoryMayAuto(j *Job) bool {
	if j.Spec.Needs.Memory == "auto" {
		return true
	}
	for _, e := range j.Extensions {
		for _, v := range e.Variants {
			if v.Needs.Memory == "auto" {
				return true
			}
		}
	}
	return false
}

func (q *Queue) memorySizingRecoverable(j *Job) bool {
	if !memoryMayAuto(j) {
		return false
	}
	recoverable := false
	q.evaluateTargetsMemory(j, false, func(fit targetFit, candidate *Job, m *Machine) bool {
		auto := j.Spec.Needs.Memory == "auto"
		if e := qualifiedExtension(candidate, m); e != nil {
			if v := e.variant(machineTarget(m)); v != nil {
				auto = auto || v.Needs.Memory == "auto"
			}
		}
		unknownCacheOnly := m.SharedInputCache == nil && fit.failures == 1 && fit.first == "shared input cache"
		if auto && (fit.failures == 0 || unknownCacheOnly) {
			recoverable = true
			return false
		}
		return true
	})
	return recoverable
}

func (q *Queue) memoryEvidenceWaitingReason(j *Job) string {
	if !memoryMayAuto(j) {
		return ""
	}
	if ok, why := q.qualifiedTargetSized(j); !ok && q.memorySizingRecoverable(j) {
		return memoryEvidenceRecovering + ": waiting for safe capacity or fresh comparable memory evidence; " + why
	}
	return ""
}

func (q *Queue) recoveryMemoryBound(j *Job, m *Machine, estimate memoryEstimate, prior, measuredPrior float64) (float64, string) {
	lower := q.constrainedMemoryLowerFor(j, estimate, !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory)
	recovery := 0.0
	if estimate.key != nil {
		facts := q.memoryConstraintEvidence(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program), !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory)
		recovery = facts.recovery
	}
	room, why := q.recoveryMemoryRoom(j, m)
	if lower == 0 || recovery <= 0 {
		// A censored peak proves a floor. Keep progress finite and independent
		// of current availability when no larger measured error is retained.
		return max(prior, lower, measuredPrior) + minimumHostMemoryGB, why
	}
	return max(min(max(prior, lower, recovery), room), lower, measuredPrior), why
}

func (q *Queue) recoveryMemoryRoom(j *Job, m *Machine) (float64, string) {
	// Reuse physical headroom, allowance, launch drift and release custody.
	// Qualification passes its permanent-capacity view; dispatch passes the
	// live ledger. Neither needs another target's allowance or a fixed cap.
	// Only permanent physical/allowance capacity may bound replaceable
	// uncertainty. Live free RAM determines whether it can launch, not demand.
	physical := q.qualificationCapacity(m)
	room, why := q.exploratoryMemory(j, &physical)
	if m.UnifiedMemory && !j.Spec.Needs.cpuOnly() {
		room -= j.Spec.Needs.MemoryGB
	}
	return max(0, room), why
}

// memorySizingDeadEnd: every target that could otherwise run this auto-memory
// job refuses with the queue's own "retrying unchanged cannot fix capacity"
// judgement. Targets that fail structurally are unrelated; any other failure
// (capacity, preparation, qualification) can clear, so the job keeps waiting.
func (q *Queue) memorySizingDeadEnd(j *Job) string {
	if !memoryMayAuto(j) {
		return ""
	}
	var refusals []string
	live := false
	q.evaluateTargetsMemory(j, true, func(fit targetFit, candidate *Job, m *Machine) bool {
		if fit.excluded {
			return true
		}
		if fit.failures == 0 {
			live = true
			return false
		}
		memory := false
		for _, dimension := range fit.failed {
			switch dimension {
			case "platform", "target", "device", "excluded_machines", "allow-list", "cores", "hardware capacity", "card group":
				return true // this target can never serve the job
			case "memory envelope":
				memory = true
			case "host memory", "GPU memory":
			default:
				live = true
				return false
			}
		}
		refusal := ""
		if candidate.MemoryEnvelope != nil {
			refusal = candidate.MemoryEnvelope.Refusal
		}
		if !memory || !strings.Contains(refusal, memoryCapacityRemedies) {
			live = true
			return false
		}
		refusal = strings.TrimSuffix(strings.TrimSuffix(refusal, memoryCapacityRemedies), "; ")
		if refusal == "" {
			refusal = "no measured comparable demand fits"
		}
		refusals = append(refusals, m.ID+": "+refusal)
		return true
	})
	if live || len(refusals) == 0 {
		return ""
	}
	slices.Sort(refusals)
	return "dead end: no host can size this auto-memory job (" + strings.Join(slices.Compact(refusals), "; ") + "); action: resubmit with an explicit memory figure that fits a host, or reduce the workload's memory use"
}
