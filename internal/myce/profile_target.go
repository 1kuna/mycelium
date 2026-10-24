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
	for _, m := range q.s.Machines {
		if q.maintenanceBlocked(hostID(m)) || !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		for _, target := range q.machineTargets(m) {
			copy := *target
			copy.FreeCPUCores = copy.EffectiveCPUCores
			copy.FreeHostMemoryGB = copy.EffectiveHostMemoryBudgetGB
			if copy.FreeHostMemoryGB == 0 && copy.CapabilityProfile != nil {
				copy.FreeHostMemoryGB = float64(copy.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
			}
			copy.FreeMemoryGB = copy.TotalMemoryGB
			if fits(j, &copy, copy.TotalMemoryGB) {
				return true
			}
		}
	}
	return false
}
