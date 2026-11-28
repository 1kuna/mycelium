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
	if j.Spec.childFanOut() && j.FanOut != nil && j.FanOut.Next < len(j.Spec.FanOut.Pieces) {
		// Dispatch adds the piece arguments before looking up its exact estimate.
		piece := *j
		piece.Spec.Args = append(slices.Clone(j.Spec.Args), j.Spec.FanOut.Pieces[j.FanOut.Next].Args...)
		j = &piece
	}
	var reasons []string
	var memoryCapacity []string
	var ephemeralRefusals []string
	targets := 0
	// One kind when every blocked target agrees. Mixed causes are capacity,
	// unless some target is refused only for memory: more memory would run the
	// job there, so the wait is a memory wait (#2184).
	kinds := map[string]bool{}
	memoryOnly := false
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
			poolFailure := ""
			if pooledMemory(candidate) {
				poolFailure = q.poolCapacityFailure(candidate, m)
			}
			var fit targetFit
			fitsResourcesChecked(candidate, &capacity, capacity.TotalMemoryGB, &fit)
			unrelated := false
			for _, dimension := range fit.failed {
				switch dimension {
				case "tools", "native network isolation", "environment", "shared host profile", "memory envelope":
					// These are actionable blockers to explain below. All other
					// failures at permanent capacity exclude this target, including
					// undersized CPUs/devices and unsupported execution runtimes.
				case "GPU memory", "hardware capacity", "memory_capacity":
					// A pool that cannot fit even when empty is actionable
					// evidence. Keep only otherwise compatible targets.
					if poolFailure == "" || candidate.OOMCapacity != nil {
						unrelated = true
					}
				default:
					unrelated = true
				}
			}
			if unrelated {
				continue
			}
			targets++
			var blocked []string
			targetMemoryOnly := true
			block := func(kind, why string) {
				blocked = append(blocked, why)
				kinds[kind] = true
				targetMemoryOnly = targetMemoryOnly && kind == memoryWaitReason
			}
			if why := inventoryWaitingReason(m); !candidate.Spec.Needs.cpuOnly() && why != "" {
				block("unknown_observation", why)
			}
			if !toolsFit(candidate, m) {
				block("capability", targetFitReason("tools", candidate, m))
			}
			if !nativeNetworkFits(candidate, m) {
				reason := targetFitReason("native network isolation", candidate, m)
				block("capability", reason)
				if candidate.Spec.ephemeral() {
					ephemeralRefusals = append(ephemeralRefusals, reason)
				}
			}
			if why := q.sharedHostAdmissionRefusal(candidate, m); why != "" {
				block("capability", why)
			} else if !environmentCanRun(candidate, m) {
				block("capability", targetFitReason("environment", candidate, m))
			}
			if candidate.MemoryEnvelope != nil && candidate.MemoryEnvelope.Refusal != "" {
				block("memory", targetFitReason("memory envelope", candidate, m))
			}
			if len(blocked) == 0 && poolFailure != "" {
				targets--
				memoryCapacity = append(memoryCapacity, m.ID+": "+poolFailure)
				continue
			}
			if hold := workerMemoryHold(m); hold != nil {
				block(hold.Kind, hold.Reason)
			}
			// A reservation cannot explain a job that cannot use this target's
			// software or network. Preserve those actionable reasons instead.
			if len(blocked) == 0 {
				if why := q.reservationReasonOnTarget(j, m); why != "" {
					block("reservation", why)
				}
			}
			hostFailure := hostFitFailure(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB)
			if err := q.workerRAMGate(candidate, m); err != nil {
				block(memoryWaitReason, err.Error())
			}
			if len(blocked) == 0 {
				if why := q.cardWaitingReason(candidate, m); why != "" {
					block("capacity", why)
				} else if pooledMemory(candidate) {
					if why := q.poolCustodyWaitingReason(candidate, m); why != "" {
						kind := "capacity"
						if candidate.Spec.Needs.cpuOnly() && hostFailure != "" {
							// CPU RAM custody and a live RAM shortage describe the same wait.
							kind = memoryWaitReason
						}
						block(kind, why)
					} else if need := candidate.Spec.Needs.MemoryGB; memoryPools(candidate).Card != nil && need > m.FreeMemoryGB {
						block(memoryWaitReason, fmt.Sprintf("card needs %.3f GiB, available %.3f GiB (device %s)", need, m.FreeMemoryGB, m.Device))
					}
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
			if hostFailure != "" {
				n := candidate.Spec.Needs
				available := "unknown"
				if m.HostMemoryMeasured && m.HostMemoryError == "" {
					available = fmt.Sprintf("%.3f GiB", m.HostAvailableMemoryGB)
				}
				detail := fmt.Sprintf("RAM needs %.3f GiB, available %s, keep %.3f GiB, reserved %.3f GiB, admission room %.3f GiB",
					hostNeed(m, n.MemoryGB, n.HostMemoryGB), available, q.pooledKeep(m), m.HostMemoryInUseGB, m.FreeHostMemoryGB)
				if ledger := q.hostMemoryLedgers[m.ID]; ledger != nil {
					detail += fmt.Sprintf(", outstanding reservations %.3f GiB", ledger.GrowthGB)
				}
				if candidate.MemoryEnvelope != nil {
					detail += " (" + candidate.MemoryEnvelope.Origin() + ")"
				}
				if hostFailure != "host memory" {
					detail += "; " + hostFailure
				}
				block(memoryWaitReason, detail)
			}
			specific = specific || len(blocked) > 0
			if len(blocked) == 0 {
				return nil // Another target can serve it; retain ordinary queue ordering.
			}
			memoryOnly = memoryOnly || targetMemoryOnly
			reasons = append(reasons, m.ID+": "+strings.Join(blocked, "; "))
		}
	}
	if targets == 0 && len(memoryCapacity) > 0 {
		slices.Sort(memoryCapacity)
		return newHold("memory_capacity", strings.Join(slices.Compact(memoryCapacity), "; "))
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
	if len(ephemeralRefusals) > 0 && len(ephemeralRefusals) == targets {
		h := newHold(ephemeralIsolationHold, reason)
		slices.Sort(ephemeralRefusals)
		h.Reason = strings.Join(slices.Compact(ephemeralRefusals), "; ")
		return h
	}
	kind := "capacity"
	if len(kinds) == 1 {
		for k := range kinds {
			kind = k
		}
	} else if memoryOnly {
		kind = memoryWaitReason
	}
	return newHold(kind, reason)
}

// A fully dispatched parent is an aggregate. Its pieces own any remaining
// placement waits, including when one of them holds an accelerator card.
func fanOutDispatchedHold(j *Job) *Hold {
	if !j.Spec.childFanOut() || j.FanOut == nil || len(j.FanOut.Children) == 0 || j.FanOut.Next < len(j.Spec.FanOut.Pieces) {
		return nil
	}
	detail := fmt.Sprintf("%d/%d pieces accepted", len(j.FanOut.Results), len(j.Spec.FanOut.Pieces))
	if len(j.FanOut.Children) == 1 {
		detail += "; dispatched job " + j.FanOut.Children[0]
	}
	return newHold("fan_out_dispatched", detail)
}

// Occupancy is not RAM pressure. Explain the same sharing/release gate used by
// dispatch, including the exact jobs retaining a card, without changing it.
func (q *Queue) cardWaitingReason(j *Job, m *Machine) string {
	if j.Spec.Needs.cpuOnly() || q.batchSlotFree(m, j) && j.Spec.Needs.MemoryGB <= m.FreeMemoryGB {
		return ""
	}
	holders := map[string]string{}
	restriction := q.gpuSharingRestriction(j, m)
	var sharingBlockers []string
	for _, holder := range q.liveJobs() {
		if holder.ID != j.ID && batchOnTarget(holder, m) && batchReservesCapacity(holder) {
			holders[holder.ID] = holder.ID + " (" + q.priorityMatch(holder.Spec).Tier + ")"
			if m.GPU == "xpu" && !holder.Spec.ExclusiveDevice && holder.Memory != nil && unmeasuredCard(holder.Memory.Card) {
				holders[holder.ID] += " (exclusive while measuring card memory)"
			}
			if restriction.conflicts(holder, m) {
				sharingBlockers = append(sharingBlockers, holders[holder.ID])
			}
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
	if len(sharingBlockers) > 0 {
		slices.Sort(sharingBlockers)
		return reservationWaitReason(m, true, 0) + fmt.Sprintf("; measured same-family throughput is lower when sharing device %s; waiting for release from %s", device, strings.Join(sharingBlockers, ", "))
	}
	if n, shared := q.deviceCoTenants(m, j, true); shared && n > 0 && q.cardSettled(m) {
		return reservationWaitReason(m, false, n) + "; held by " + strings.Join(owners, ", ")
	}
	reason := reservationWaitReason(m, true, 0) + fmt.Sprintf("; card busy: waiting for GPU batch slot on device %s held by %s", device, strings.Join(owners, ", "))
	if m.GPU == "xpu" && !j.Spec.ExclusiveDevice && j.Memory != nil && unmeasuredCard(j.Memory.Card) {
		reason += "; this job requires an exclusive first run to measure card memory"
	}
	return reason
}

func unmeasuredCard(card *memoryPoolEstimate) bool {
	return card != nil && card.Alone && card.Evidence.Level == "unknown"
}

// A dead end is a wait the queue itself judges waiting cannot clear. It fails
// the job at once with one sentence and one action instead of leaving it
// queued for days. Capacity, preparation, qualification the queue runs itself,
// dependencies and holds all stay waits. Runs once per queue tick.
func (q *Queue) maintainDeadEndWaits() {
	now := q.clock.Now()
	for _, j := range q.liveJobs() {
		// A fan-out parent is the placement unit until its first piece exists.
		started := j.Spec.childFanOut() && j.FanOut != nil && len(j.FanOut.Children) > 0
		if j.State != "queued" || cancelled(j) || started || j.Batch != nil || !dependencyReady(j) || j.Attempt > 0 && !j.ReleaseConfirmed {
			continue
		}
		reason, failures := q.deadEndReason(j)
		if reason == "" {
			continue
		}
		q.touchJob(j.ID)
		q.closeAdmissionWait(j, "failed")
		j.State, j.Reason, j.DiagnosticCode, j.FinishedAt = "failed", reason, deadEndWait, &now
		j.NativeCheckFailures = failures
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
	}
}

const deadEndWait = "dead_end_wait"

func (q *Queue) deadEndReason(j *Job) (string, []NativeCheckFailure) {
	if why, failures := q.extensionDeadEnd(j); why != "" {
		return why, failures
	}
	if why := q.environmentDeadEnd(j); why != "" {
		return why, nil
	}
	return "", nil
}
