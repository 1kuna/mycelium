package myce

// A preparation dependency is impossible only when every known compatible
// target lacks a builder or has a deterministic failure. Offline inventory and
// temporary admission pressure are not negative evidence. This uses the same
// frozen variants and environment identities as preparation, without live room.
func (q *Queue) environmentDeadEnd(j *Job) string {
	if j.Spec.Environment.Key() == "" && len(j.EnvironmentRecipes) == 0 && len(j.Extensions) == 0 && j.CommandDiscovery == nil {
		return ""
	}
	for _, id := range j.Spec.AllowedMachines {
		if q.s.Machines[id] == nil {
			return ""
		}
	}
	known, compatible, failed := false, false, false
	for _, owner := range q.s.Machines {
		if owner.Replacement != nil || !q.jobAllowsMachine(j, owner.ID) {
			continue
		}
		if owner.CapabilityProfile == nil {
			return ""
		}
		known = true
		if !j.Spec.Needs.cpuOnly() && inventoryWaitingReason(owner) != "" {
			return ""
		}
		for _, m := range q.placementTargets(owner) {
			for _, candidate := range extensionJobs(j, m, false) {
				probe := *candidate
				if len(probe.Extensions) > 0 {
					// Only the native check may use unqualified frozen files.
					probe.Spec.ExtensionCheck = &ExtensionCheckRequest{Work: j.Spec.workType()}
				}
				if !executionTargetMatches(probe.Spec, m) || !probe.Spec.platformFits(m.GPU) || !deviceAllowed(&probe, m) || probe.Spec.Needs.cpuOnly() != (m.GPU == "cpu") {
					continue
				}
				compatible = true
				if environmentReady(&probe, m) {
					return ""
				}
				if q.unreported[owner.ID] || !q.inventoryFresh(owner) {
					return ""
				}
				if !m.EnvironmentBuilder || !environmentCanRun(&probe, m) {
					continue
				}
				if !q.environmentFailureBlocked(&probe, m) {
					return ""
				}
				for _, report := range m.EnvironmentReports {
					if contains(environmentFailureKeys(&probe, m), report.Key) && environmentReportMatches(report, m) && q.environmentReportBlocked(report) && environmentFailureKind(report) == "transient" {
						return ""
					}
				}
				failed = true
			}
		}
	}
	// Extension coverage already distinguishes absent devices from unsupported
	// targets. Do not turn that unknown-inventory wait into a preparation failure.
	if !known || j.Spec.extensionJob() && !compatible {
		return ""
	}
	if failed {
		return "dead end: environment preparation failed on every compatible builder; action: fix the environment recipe or repair a builder, then resubmit"
	}
	return "dead end: no known host can prepare the required environment; action: provide a compatible environment builder or change the recipe and resubmit"
}
