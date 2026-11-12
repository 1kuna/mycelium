package myce

import "fmt"

// CPU and accelerator targets share one physical host ledger. A ready job
// must not disappear from CPU admission just because existing CPU reservations
// currently consume its RAM or cores (#787). This is an admission projection,
// never a stop plan: running work finishes under its existing contract.
type hostBackfillDemand struct {
	job     *Job
	machine *Machine
	order   *candidateSet
}

// Piece executions finish too. Parents describe demand; they never own
// execution capacity.
func backfillFiniteExecution(j *Job) bool {
	return j.Spec.Needs.Model == "" && j.Spec.FanOut == nil && j.Spec.ExtensionCheck == nil
}

func (q *Queue) backfillCandidates(j *Job, m, projected *Machine, inventory []*Machine) []*Job {
	if j.Spec.FanOut != nil {
		if candidate := q.fanOutCandidateWithResources(j, projected, nil, inventory); candidate != nil {
			return []*Job{candidate}
		}
		return nil
	}
	return q.memoryCandidates(j, m)
}

// Select one currently usable target per demand, preferring immediate capacity
// and then ordinary placement value. Busy cards, cold environments, missing
// qualification, pressure and stale workers cannot withhold backfill. Only
// host capacity released by finite CPU work is projected; device capacity and
// all other current readiness checks remain unchanged.
func (q *Queue) hostBackfillFences(scan *placementScan) map[string]*hostBackfillDemand {
	if scan.hostBackfill != nil {
		return scan.hostBackfill
	}
	scan.hostBackfill = map[string]*hostBackfillDemand{}
	demands := []*Job{}
	for _, j := range q.liveJobs() {
		if j.State == "queued" && (backfillFiniteExecution(j) || j.Spec.FanOut != nil) {
			demands = append(demands, j)
		}
	}
	if len(demands) == 0 {
		return scan.hostBackfill
	}
	live := q.placementLiveness(scan)
	projections := map[string]*Machine{}
	for _, m := range scan.machines {
		if !live[m.ID] || q.preparationActive(m) || q.preparationStopping(m) || m.HostMemoryError != "" || hostHasUnifiedCUDA(m) && !validHostMemoryObservation(m.MemoryObservation) {
			continue
		}
		jobs := []*Job{}
		for _, running := range q.liveJobs() {
			owner := q.s.Machines[running.Machine]
			if owner != nil && !owner.Gone && hostID(owner) == hostID(m) && running.Spec.Needs.cpuOnly() && backfillFiniteExecution(running) && batchReservesCapacity(running) {
				jobs = append(jobs, running)
			}
		}
		projected := q.afterReleases(m, jobs, nil)
		capacity := q.qualificationCapacity(m)
		projected.FreeHostMemoryGB = min(capacity.FreeHostMemoryGB, projected.FreeHostMemoryGB)
		projections[targetKey(m)] = &projected
	}
	// Keep dispatch's target order and global wave planner. Passing the entire
	// projected inventory selects one wave/target rather than reserving every
	// host independently for a fan-out parent's unscaled per-piece template.
	inventory := make([]*Machine, 0, len(projections))
	for _, m := range scan.machines {
		if projected := projections[targetKey(m)]; projected != nil {
			inventory = append(inventory, projected)
		}
	}
	choices := []*hostBackfillDemand{}
	orders := map[string]*candidateSet{}
	for _, j := range demands {
		var best *hostBackfillDemand
		demandInventory := inventory
		if j.Spec.FanOut != nil && j.FanOut != nil {
			for _, target := range scan.machines {
				if projections[targetKey(target)] == nil || !q.workerIdle(target, j) {
					continue
				}
				if candidate := q.fanOutCandidateWithResources(j, target, nil, scan.machines); q.immediateCandidate(candidate, target) != nil {
					// Dispatch can use capacity now, including serial fallback on
					// a qualified target without curves. Preserve that destination.
					demandInventory = scan.machines
					break
				}
			}
		}
		for _, m := range scan.machines {
			projected := projections[targetKey(m)]
			if projected == nil || !q.placementAllowed(j, m) || !q.preemptionPlacementAllows(j, m) || q.preemptionReleasesPending(j) || !q.checkpointReady(j, m) || q.environmentFailureBlocked(j, m) {
				continue
			}
			for _, candidate := range q.backfillCandidates(j, m, projected, demandInventory) {
				if !environmentReady(candidate, m) || q.capReasonFor(candidate, m) != "" || !q.batchSlotFree(m, candidate) || !fits(candidate, projected, m.FreeMemoryGB) {
					continue
				}
				key := targetKey(m)
				if orders[key] == nil {
					orders[key] = &candidateSet{q: q, scores: map[string]float64{}, actives: map[string]float64{}}
				}
				orders[key].jobs = append(orders[key].jobs, candidate)
				ready := fits(candidate, m, m.FreeMemoryGB)
				bestReady := best != nil && fits(best.job, best.machine, best.machine.FreeMemoryGB)
				if best == nil || ready && !bestReady || ready == bestReady && q.targetPlacementBetter(candidate, m, best.machine, scan) {
					best = &hostBackfillDemand{job: candidate, machine: m}
				}
			}
		}
		if best != nil {
			choices = append(choices, best)
		}
	}
	// Use the existing tier/fair-turn comparator without recursively constructing
	// candidate sets (these demands are precisely the ones ordinary fitting can
	// omit). Only projects eligible on that target compete for its fair share;
	// demand restricted to another host cannot change a local fair turn.
	for _, choice := range choices {
		set := orders[targetKey(choice.machine)]
		choice.order = set
		host := hostID(choice.machine)
		previous := scan.hostBackfill[host]
		if previous == nil || q.beforeWithFair(choice.job, previous.job, q.s.Machines[choice.machine.ID], set.score, set.active) {
			scan.hostBackfill[host] = choice
		}
	}
	return scan.hostBackfill
}

func (q *Queue) hostBackfillAllows(j *Job, m *Machine, fence *hostBackfillDemand) bool {
	if fence == nil || j.ID == fence.job.ID {
		return true
	}
	// Keep the existing tier and fair-turn order, including accelerator-first
	// ordering across CPU/device views of the same worker. A demand protects
	// capacity only against work behind it, never against its own admission.
	if q.beforeWithFair(j, fence.job, q.s.Machines[m.ID], fence.order.score, fence.order.active) {
		return true
	}
	d := fence.job.Spec.Needs
	// Backfill can use only capacity in excess of the demand. While a dimension
	// is short, new work cannot deepen that deficit; other hosts remain free.
	return hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) <= max(0, m.FreeHostMemoryGB-m.hostReservationDebtGB-hostNeed(fence.machine, d.MemoryGB, d.HostMemoryGB)) &&
		j.Spec.Needs.cores() <= max(0, m.FreeCPUCores-d.cores())
}

func (q *Queue) hostBackfillWaitingReason(j *Job, scan *placementScan) string {
	if !j.Spec.Needs.cpuOnly() {
		return ""
	}
	var demand *Job
	for _, m := range scan.machines {
		if !q.placementLiveness(scan)[m.ID] || !q.placementAllowed(j, m) || !q.checkpointReady(j, m) || !environmentReady(j, m) || q.preparationActive(m) || q.preparationStopping(m) {
			continue
		}
		for _, candidate := range q.memoryCandidates(j, m) {
			if !q.batchSlotFree(m, candidate) || !fits(candidate, m, m.FreeMemoryGB) {
				continue
			}
			// No fitting target means capacity accumulation cannot be this job's
			// blocker. Avoid sizing the entire backlog just to discover that.
			fence := q.hostBackfillFences(scan)[hostID(m)]
			if q.hostBackfillAllows(candidate, m, fence) {
				return "" // Another currently fitting host is not withheld.
			}
			if demand == nil || fence.job.ID < demand.ID {
				demand = fence.job
			}
		}
	}
	if demand != nil {
		return fmt.Sprintf("host_capacity_accumulation: waiting for job %s to acquire host RAM/cores; running attempts finish normally", demand.ID)
	}
	return ""
}
