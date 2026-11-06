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
		if j.State == "queued" && finiteYieldJob(j) {
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
		room := m.FreeHostMemoryGB - m.hostReservationDebtGB
		for _, running := range q.liveJobs() {
			owner := q.s.Machines[running.Machine]
			if owner != nil && !owner.Gone && hostID(owner) == hostID(m) && running.Spec.Needs.cpuOnly() && finiteYieldJob(running) && batchReservesCapacity(running) {
				jobs = append(jobs, running)
				room += hostNeed(owner, running.Spec.Needs.MemoryGB, running.Spec.Needs.HostMemoryGB)
			}
		}
		projected := q.cpuAfterReleases(m, jobs, nil, 0)
		capacity := q.qualificationCapacity(m)
		projected.FreeHostMemoryGB = min(capacity.FreeHostMemoryGB, max(0, room))
		projections[targetKey(m)] = &projected
	}
	choices := []*hostBackfillDemand{}
	for _, j := range demands {
		var best *hostBackfillDemand
		for _, m := range scan.machines {
			projected := projections[targetKey(m)]
			if projected == nil || !q.placementAllowed(j, m) || !q.preemptionPlacementAllows(j, m) || q.preemptionReleasesPending(j) || !q.checkpointReady(j, m) || q.environmentFailureBlocked(j, m) {
				continue
			}
			for _, candidate := range q.memoryCandidates(j, m) {
				if !environmentReady(candidate, m) || q.capReasonFor(candidate, m) != "" || !q.batchSlotFree(m, candidate) || !fits(candidate, projected, m.FreeMemoryGB) {
					continue
				}
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
	// omit). All otherwise ready demands participate in the fair-share scores.
	set := &candidateSet{q: q, scores: map[string]float64{}, actives: map[string]float64{}}
	for _, choice := range choices {
		set.jobs = append(set.jobs, choice.job)
	}
	for _, choice := range choices {
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
	fences := q.hostBackfillFences(scan)
	var demand *Job
	for _, m := range scan.machines {
		if !q.placementLiveness(scan)[m.ID] || !q.placementAllowed(j, m) || !q.checkpointReady(j, m) || !environmentReady(j, m) || q.preparationActive(m) || q.preparationStopping(m) {
			continue
		}
		for _, candidate := range q.memoryCandidates(j, m) {
			if !q.batchSlotFree(m, candidate) || !fits(candidate, m, m.FreeMemoryGB) {
				continue
			}
			fence := fences[hostID(m)]
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
