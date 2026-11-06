package myce

const memoryEvidenceRecovering = "memory_evidence_recovering"

// Qualification may defer uncertain sizing, never the author's requirements,
// member containment or an attempt's durable OOM/device retry floor. This copy
// is only a diagnostic; dispatch still uses the complete resolved envelope.
func memoryIntent(j *Job) *Job {
	copy := *j
	copy.MemoryEnvelope = nil
	copy.Spec.Needs.Memory = ""
	n := &copy.Spec.Needs
	n.HostMemoryGB = max(n.HostMemoryGB, minimumHostMemoryGB, j.Spec.Run.MemoryGB, j.Spec.Run.DockerMemoryGB)
	for _, side := range j.Spec.Run.Sidecars {
		n.HostMemoryGB = max(n.HostMemoryGB, side.MemoryGB)
	}
	for _, side := range j.Spec.Run.DockerSidecars {
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
	margin := 0.0
	if estimate.key != nil {
		facts := q.memoryConstraintEvidence(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program), !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory)
		margin = facts.margin
	}
	if lower == 0 || margin <= 0 {
		// Without an observed error/growth margin there is no finite upper
		// estimate. Preserve physical probing, including the measured floor.
		room, why := q.exploratoryMemory(j, m)
		return max(prior, room, lower, measuredPrior), why
	}
	bound := max(prior, lower+margin)
	largest := 0.0
	q.memoryHistory().evaluateTargetsMemory(memoryIntent(j), false, func(fit targetFit, candidate *Job, target *Machine) bool {
		if fit.failures == 0 {
			room := target.FreeHostMemoryGB - target.MemoryCeilingSlackGB
			if target.UnifiedMemory && !candidate.Spec.Needs.cpuOnly() {
				room -= candidate.Spec.Needs.MemoryGB
			}
			largest = max(largest, room)
		}
		return true
	})
	if largest > 0 {
		bound = min(bound, largest)
	}
	// Capping uncertainty never authorizes a run below measured need.
	return max(bound, lower, measuredPrior), ""
}
