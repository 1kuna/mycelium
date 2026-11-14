package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Explain admission on the job's own targets, using the same target checks and
// resolved envelopes as placement. Network and preparation failures are useful
// diagnostics; unrelated platforms, devices and qualifications are not.
func (q *Queue) targetWait(j *Job, scans ...*placementScan) *Hold {
	if why := q.memoryEvidenceWaitingReason(j); why != "" {
		return newHold(memoryEvidenceRecovering, why)
	}
	var reasons []string
	// One kind when every blocked target agrees; mixed causes are capacity.
	kinds := map[string]bool{}
	specific := false
	for _, m := range q.planningMachines(scans...) {
		if !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		// Plain jobs keep their structural requirements through sizing. The
		// final report below excludes these same unrelated routes. Extension
		// variants must still be lowered first by memoryCandidates.
		if !j.Spec.extensionJob() && !memorySizingTargetFits(j, m) {
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
			block := func(kind, why string) {
				blocked = append(blocked, why)
				kinds[kind] = true
			}
			if why := inventoryWaitingReason(m); !candidate.Spec.Needs.cpuOnly() && why != "" {
				block("unknown_observation", why)
			}
			if !toolsFit(candidate, m) {
				block("capability", targetFitReason("tools", candidate, m))
			}
			if !nativeNetworkFits(candidate, m) {
				block("capability", targetFitReason("native network isolation", candidate, m))
			}
			if why := sharedHostRefusal(candidate, m); why != "" {
				block("capability", why)
			} else if !environmentCanRun(candidate, m) {
				block("capability", targetFitReason("environment", candidate, m))
			}
			if candidate.MemoryEnvelope != nil && candidate.MemoryEnvelope.Refusal != "" {
				block(memoryRefusalKind(candidate.MemoryEnvelope, "memory"), targetFitReason("memory envelope", candidate, m))
			}
			// A reservation cannot explain a job that cannot use this target's
			// software or network. Preserve those actionable reasons instead.
			if len(blocked) == 0 {
				if why := q.reservationReasonOnTarget(j, m); why != "" {
					block("reservation", why)
				}
			}
			if len(blocked) == 0 {
				if why := q.cardWaitingReason(candidate, m); why != "" {
					block("capacity", why)
				}
			}
			// Permanent capacity above deliberately ignores current occupancy.
			// Explain occupied cores here, before an otherwise empty target can
			// discard the other hosts' blockers or the RAM-only fallback skips it.
			if !cpuFits(candidate, m) {
				why := m.CPUCapacityReason
				if why == "" {
					_, why = cpuCapacity(m)
				}
				if why == "" {
					why = fmt.Sprintf("%d cores needed; %d cores unreserved of %d admitted (%d reserved; CPU utilization unknown)", candidate.Spec.Needs.cores(), m.FreeCPUCores, m.EffectiveCPUCores, m.CPUInUseCores)
				}
				block("capacity", "CPU capacity: "+why)
			}
			// Permanent qualification also clears temporary availability. Such
			// a host cannot serve this job just because its cores and RAM fit.
			if m.Disabled != "" || m.Gone || m.Leaving || m.Recovering || availabilityBlocked(m) {
				block("host_unavailable", HyphaAvailability(m))
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
				block("memory", detail)
			}
			if len(blocked) == 0 {
				return nil // Another target can serve it; retain ordinary queue ordering.
			}
			reasons = append(reasons, m.ID+": "+strings.Join(blocked, "; "))
		}
	}
	if !specific {
		return nil
	}
	slices.Sort(reasons)
	reason := strings.Join(slices.Compact(reasons), "; ")
	if len(j.Spec.AllowedMachines) > 0 {
		// Preserve authored placement context when a pinned host's temporary
		// blocker is more specific than the ordinary allowed-machine wait.
		reason = "waiting for allowed machines: " + strings.Join(j.Spec.AllowedMachines, ", ") + "; " + reason
	}
	// The cold-probe margin is its own kind only when it alone holds the job.
	if kinds["cold_probe_margin"] && len(kinds) > 1 {
		delete(kinds, "cold_probe_margin")
		kinds["memory"] = true
	}
	kind := "capacity"
	if len(kinds) == 1 {
		for k := range kinds {
			kind = k
		}
	}
	return newHold(kind, reason)
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

// A dead end is a wait the queue itself judges waiting cannot clear. It fails
// the job at once with one sentence and one action instead of leaving it
// queued for days. Capacity, preparation, qualification the queue runs itself,
// dependencies and holds all stay waits. Runs once per queue tick.
func (q *Queue) maintainDeadEndWaits() {
	q.releaseUnusedMemoryExplorations()
	now := q.clock.Now()
	for _, j := range q.liveJobs() {
		// A fan-out parent is the placement unit until its first piece exists.
		started := j.Spec.FanOut != nil && j.FanOut != nil && len(j.FanOut.Children) > 0
		if j.State != "queued" || cancelled(j) || j.Spec.ExtensionCheck != nil || started || j.Batch != nil || !dependencyReady(j) || j.Attempt > 0 && !j.ReleaseConfirmed {
			continue
		}
		reason := q.deadEndReason(j)
		if reason == "" {
			continue
		}
		q.touchJob(j.ID)
		q.closeAdmissionWait(j, "failed")
		j.State, j.Reason, j.DiagnosticCode, j.FinishedAt = "failed", reason, deadEndWait, &now
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
	}
}

const deadEndWait = "dead_end_wait"

func (q *Queue) deadEndReason(j *Job) string {
	if why := q.oomCapacityDeadEnd(j); why != "" {
		return why
	}
	if why := q.extensionDeadEnd(j); why != "" {
		return why
	}
	return q.memorySizingDeadEnd(j)
}
