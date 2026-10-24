package myce

import "slices"

func executionTargetMatches(s Spec, m *Machine) bool {
	if len(s.Targets) == 0 {
		return true
	}
	target := machineTarget(m)
	if s.Needs.cpuOnly() {
		target.Platform = "cpu"
	}
	return slices.Contains(s.Targets, target)
}

// Submission checks qualification and total admitted capacity, not free slots.
// Busy qualified hosts can queue work. Dispatch and launch still check live
// admission through fitsResources; this check grants no capacity reservation.
func (q *Queue) hasQualifiedTarget(j *Job) bool {
	// Resolve envelopes with the planner's code, but without temporary
	// occupancy or release holds. Retain observations for learned needs.
	capacity := Queue{clock: q.clock, s: q.s}
	capacity.s.Jobs, capacity.s.ModelSlots, capacity.s.HostReleases = nil, nil, nil
	for _, m := range q.s.Machines {
		if !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		for _, target := range q.machineTargets(m) {
			copy := cloneMachine(target)
			copy.Gone, copy.Leaving, copy.Recovering, copy.CheckpointUnavailable = false, false, false, false
			copy.MemoryError, copy.HostMemoryError, copy.CPUCapacityReason = "", "", ""
			copy.HostMemoryPolicyMismatch = false
			policy := q.s.HostMaintenance[hostID(m)]
			if policy.CPUBudgetCores != nil {
				copy.CPUBudgetCores = *policy.CPUBudgetCores
			}
			copy.EffectiveCPUCores, _ = cpuCapacity(&copy)
			if availability := machineAvailability(&copy); availability != nil {
				availability.Available = true
			}
			copy.cpuLedger = true
			copy.CPUInUseCores = 0
			copy.FreeCPUCores = copy.EffectiveCPUCores
			if copy.CapabilityProfile != nil {
				copy.FreeHostMemoryGB = float64(copy.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
				allowance := copy.HostMemoryBudgetGB
				if policy.MemoryBudget != nil {
					allowance = policy.MemoryBudget.GiB
				}
				copy.EffectiveHostMemoryBudgetGB = allowance
				if allowance > 0 {
					copy.FreeHostMemoryGB = min(copy.FreeHostMemoryGB, allowance)
				}
				copy.HostMemoryMeasured = true
			}
			copy.FreeMemoryGB = copy.TotalMemoryGB
			// Observation identity must see the same available target copy,
			// including when the live owner is temporarily unavailable.
			capacity.s.Machines = map[string]*Machine{copy.ID: &copy}
			for _, candidate := range extensionJobs(j, &copy, true) {
				candidate = capacity.memoryCandidate(candidate, &copy)
				if hostFits(&copy, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) && fitsResources(candidate, &copy, copy.TotalMemoryGB) {
					return true
				}
			}
		}
	}
	return false
}
