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
	if why := q.memoryEvidenceWaitingReason(j); why != "" {
		return why
	}
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
				case "tools", "native network isolation", "environment", "shared host profile", "memory envelope":
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
			if !toolsFit(candidate, m) {
				blocked = append(blocked, targetFitReason("tools", candidate, m))
			}
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
			if len(blocked) == 0 {
				if why := q.cardWaitingReason(candidate, m); why != "" {
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

// Occupancy is not RAM pressure. Explain the same sharing/release gate used by
// dispatch, including the exact jobs retaining a card, without changing it.
func (q *Queue) cardWaitingReason(j *Job, m *Machine) string {
	if j.Spec.Needs.cpuOnly() || q.batchSlotFree(m, j) && j.Spec.Needs.MemoryGB <= m.FreeMemoryGB {
		return ""
	}
	holders := map[string]string{}
	for _, holder := range q.liveJobs() {
		if holder.ID != j.ID && batchOnTarget(holder, m) && batchReservesCapacity(holder) {
			holders[holder.ID] = holder.ID + " (" + q.priorityMatch(holder.Spec).Tier + ")"
		}
	}
	for _, release := range q.s.HostReleases {
		if !q.batchReleaseOnTarget(release, m) {
			continue
		}
		if _, ok := holders[release.Job]; !ok {
			holders[release.Job] = release.Job + " (awaiting exact release)"
		}
	}
	if len(holders) == 0 {
		return ""
	}
	owners := make([]string, 0, len(holders))
	for _, owner := range holders {
		owners = append(owners, owner)
	}
	slices.Sort(owners)
	device := strings.Join(targetDevices(m), ",")
	if device == "" {
		return reservationWaitReason(m, true, 0) + "; waiting for GPU batch slot held by " + strings.Join(owners, ", ")
	}
	if n, shared := q.deviceCoTenants(m, j, true); shared && n > 0 && q.cardSettled(m) {
		return reservationWaitReason(m, false, n) + "; held by " + strings.Join(owners, ", ")
	}
	return reservationWaitReason(m, true, 0) + fmt.Sprintf("; card busy: waiting for GPU batch slot on device %s held by %s", device, strings.Join(owners, ", "))
}
