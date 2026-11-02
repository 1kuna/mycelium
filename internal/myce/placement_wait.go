package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Explain admission on the job's own targets, using the same target checks and
// resolved envelopes as placement. Network and preparation failures are useful
// diagnostics; unrelated platforms, devices and qualifications are not.
func (q *Queue) targetWaitingReason(j *Job) string {
	var reasons []string
	specific := false
	for _, m := range q.plannerMachines() {
		if !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		capacity := q.qualificationCapacity(m)
		for _, candidate := range q.memoryCandidates(j, m) {
			var fit targetFit
			fitsResourcesChecked(candidate, &capacity, capacity.TotalMemoryGB, &fit)
			unrelated := false
			for _, dimension := range fit.failed {
				switch dimension {
				case "native network isolation", "environment", "shared host profile", "memory envelope":
					// These are actionable blockers to explain below. All other
					// failures at permanent capacity exclude this target, including
					// undersized CPUs/devices and unsupported execution runtimes.
				default:
					unrelated = true
				}
			}
			if unrelated {
				continue
			}
			var blocked []string
			if !nativeNetworkFits(candidate, m) {
				blocked = append(blocked, targetFitReason("native network isolation", candidate, m))
			}
			if why := sharedHostRefusal(candidate, m); why != "" {
				blocked = append(blocked, why)
			} else if !environmentCanRun(candidate, m) {
				blocked = append(blocked, targetFitReason("environment", candidate, m))
			}
			if candidate.MemoryEnvelope != nil && candidate.MemoryEnvelope.Refusal != "" {
				blocked = append(blocked, targetFitReason("memory envelope", candidate, m))
			}
			// A reservation cannot explain a job that cannot use this target's
			// software or network. Preserve those actionable reasons instead.
			if len(blocked) == 0 {
				if why := q.reservationReasonOnTarget(j, m); why != "" {
					blocked = append(blocked, why)
				}
			}
			specific = specific || len(blocked) > 0
			if why := hostFitFailure(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB); why != "" {
				n := candidate.Spec.Needs
				detail := fmt.Sprintf("RAM envelope: %.1f GiB needed, %.1f GiB available", hostNeed(m, n.MemoryGB, n.HostMemoryGB), m.FreeHostMemoryGB)
				if candidate.MemoryEnvelope != nil {
					detail += " (" + candidate.MemoryEnvelope.Origin() + ")"
				}
				if why != "host memory" {
					detail += "; " + why
				}
				blocked = append(blocked, detail)
			}
			if len(blocked) == 0 {
				return "" // Another target can serve it; retain ordinary queue ordering.
			}
			reasons = append(reasons, m.ID+": "+strings.Join(blocked, "; "))
		}
	}
	if !specific {
		return ""
	}
	slices.Sort(reasons)
	return strings.Join(slices.Compact(reasons), "; ")
}
