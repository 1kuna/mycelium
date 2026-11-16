package myce

import (
	"fmt"
	"time"
)

// CPU and accelerator targets share one physical host ledger. A ready job
// must not disappear from host admission just because existing reservations
// currently consume its RAM or cores (#787). This is an admission projection,
// never a stop plan: running work finishes under its existing contract.
type hostBackfillDemand struct {
	job     *Job
	machine *Machine
	order   *candidateSet
	// drain marks a maintenance resume's fence; start is when it is expected
	// to fit anyway, or nil when unknown.
	drain bool
	start *time.Time
	// Device demand can be next after a finite GPU execution. CPU backfill
	// and lower-tier GPU work observe production's fence;
	// the projection grants no accelerator admission.
	devicePending bool
}

// Piece executions finish too. Parents describe demand; they never own
// execution capacity.
func backfillFiniteExecution(j *Job) bool {
	return j.Spec.Needs.Model == "" && j.Spec.FanOut == nil && j.Spec.ExtensionCheck == nil
}

func (q *Queue) backfillCandidates(j *Job, m, projected *Machine, resources map[batchResourceKey]batchResourceRoom) []*Job {
	if j.Spec.FanOut != nil {
		if candidate := q.fanOutCandidateWithResources(j, projected, resources); candidate != nil {
			return []*Job{candidate}
		}
		return nil
	}
	return q.memoryCandidates(j, m)
}

// Select one currently usable target per demand, preferring immediate capacity
// and then ordinary placement value. Cold environments, missing qualification,
// pressure and stale workers cannot withhold backfill. Finite host releases and
// queued GPU demand behind finite device executions project host capacity only;
// actual device admission still requires the ordinary exact-release checks.
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
	deviceProjections := map[string]*Machine{}
	for _, m := range scan.machines {
		if !live[m.ID] || q.preparationActive(m) || q.preparationStopping(m) || m.HostMemoryError != "" || hostHasUnifiedCUDA(m) && !validHostMemoryObservation(m.MemoryObservation) {
			continue
		}
		jobs := []*Job{}
		for _, running := range q.liveJobs() {
			owner := q.s.Machines[running.Machine]
			if owner != nil && !owner.Gone && hostID(owner) == hostID(m) && backfillFiniteExecution(running) && batchReservesCapacity(running) {
				jobs = append(jobs, running)
			}
		}
		projected := q.afterReleases(m, jobs, nil)
		capacity := q.qualificationCapacity(m)
		projected.FreeHostMemoryGB = min(capacity.FreeHostMemoryGB, projected.FreeHostMemoryGB)
		projections[targetKey(m)] = &projected
		if m.GPU != "cpu" {
			deviceJobs := []*Job{}
			for _, running := range q.liveJobs() {
				if !running.Spec.Needs.cpuOnly() && backfillFiniteExecution(running) && batchReservesCapacity(running) && batchOnTarget(running, m) {
					deviceJobs = append(deviceJobs, running)
				}
			}
			if len(deviceJobs) > 0 {
				future := q.afterReleases(m, append(jobs, deviceJobs...), nil)
				for _, running := range deviceJobs {
					future.FreeMemoryGB += deviceShareGB(running)
				}
				future.FreeMemoryGB = min(m.TotalMemoryGB, future.FreeMemoryGB)
				future.FreeHostMemoryGB = min(capacity.FreeHostMemoryGB, future.FreeHostMemoryGB)
				deviceProjections[targetKey(m)] = &future
			}
		}
	}
	choices := []*hostBackfillDemand{}
	orders := map[string]*candidateSet{}
	drains := map[string]*Machine{}
	resources := map[*Machine]map[batchResourceKey]batchResourceRoom{}
	resourceRoom := func(m *Machine) map[batchResourceKey]batchResourceRoom {
		if resources[m] == nil {
			resources[m] = map[batchResourceKey]batchResourceRoom{}
		}
		return resources[m]
	}
	for _, j := range demands {
		var best *hostBackfillDemand
		drain := maintenanceResume(j) && j.Spec.FanOut == nil
		for _, m := range scan.machines {
			projected := projections[targetKey(m)]
			if projected == nil || !q.placementAllowed(j, m) || !q.preemptionPlacementAllows(j, m) || q.preemptionReleasesPending(j) || !q.checkpointReady(j, m) || q.environmentFailureBlocked(j, m) {
				continue
			}
			device := m.FreeMemoryGB
			devicePending := false
			if !j.Spec.Needs.cpuOnly() && !drain && deviceProjections[targetKey(m)] != nil {
				projected = deviceProjections[targetKey(m)]
				device, devicePending = projected.FreeMemoryGB, true
			}
			if drain {
				// The whole host drains toward a maintenance resume.
				if drains[targetKey(m)] == nil {
					p := q.drainProjection(m, q.drainingAttempts(m))
					drains[targetKey(m)] = &p
				}
				projected, device = drains[targetKey(m)], drains[targetKey(m)].FreeMemoryGB
			}
			for _, candidate := range q.backfillCandidates(j, m, projected, resourceRoom(projected)) {
				if !environmentReady(candidate, m) || q.capReasonFor(candidate, m) != "" || !drain && !devicePending && !q.batchSlotFree(m, candidate) || !fits(candidate, projected, device) {
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
					best = &hostBackfillDemand{job: candidate, machine: m, drain: drain, devicePending: devicePending}
				}
			}
		}
		if best != nil {
			if best.drain {
				best.start = q.drainStart(best.job, best.machine, q.drainingAttempts(best.machine))
			}
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
	if fence.devicePending && !j.Spec.Needs.cpuOnly() && !(fence.job.Spec.Priority == "production" && q.comparePriority(j.Spec, fence.job.Spec) > 0) {
		return true // Other GPU demand keeps ordinary device admission order.
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
	room := hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) <= max(0, m.FreeHostMemoryGB-m.hostReservationDebtGB-hostNeed(fence.machine, d.MemoryGB, d.HostMemoryGB)) &&
		j.Spec.Needs.cores() <= max(0, m.FreeCPUCores-d.cores())
	if !fence.drain {
		return room
	}
	// A maintenance resume also keeps its accelerator: later work does not
	// take that card. Either way, work expected to finish before the resume
	// could start anyway still backfills.
	if !d.cpuOnly() && !j.Spec.Needs.cpuOnly() && targetKey(m) == targetKey(fence.machine) {
		room = false
	}
	return room || q.finishesBeforeDrain(j, m, fence.start)
}

func (q *Queue) hostBackfillWaitingReason(j *Job, scan *placementScan) string {
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
