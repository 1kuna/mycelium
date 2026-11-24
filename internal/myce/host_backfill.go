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
	pooled  bool
	// drain marks a maintenance resume's fence; start is when it is expected
	// to fit anyway, or nil when unknown.
	drain bool
	start *time.Time
	// A fleet startup probe has first use of non-production backfill.
	// When production owns the main fence, its vector is protected first.
	fleetProbe bool
	probeReady bool
	startProbe *hostBackfillDemand
}

// Piece executions and native extension checks finish too. Checks must protect
// their resolved host vector while queued, including guarded first runs that
// cannot fit beside CPU holders yet (#1717). Parents describe demand; they
// never own execution capacity.
func backfillFiniteExecution(j *Job) bool {
	return j.Spec.Needs.Model == "" && j.Spec.FanOut == nil
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
	choices := []*hostBackfillDemand{}
	probes := map[string]*hostBackfillDemand{}
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
		start := q.fleetStartReservation(j)

		if !drain && start == nil && (q.s.FleetUpgrade == nil || q.s.FleetUpgrade.State != "running" || jobClass(j.Spec) != "production") {
			continue
		}
		for _, m := range scan.machines {
			if !live[m.ID] || q.preparationActive(m) || q.preparationStopping(m) ||
				start != nil && !q.fleetStartReserves(start, m) || !q.placementAllowed(j, m) ||
				!q.preemptionPlacementAllows(j, m) || q.preemptionReleasesPending(j) ||
				!q.checkpointReady(j, m) || q.environmentFailureBlocked(j, m) {
				continue
			}
			key := targetKey(m)
			if drains[key] == nil {
				room := q.drainProjection(m, q.drainingAttempts(m))
				drains[key] = &room
			}
			projected := drains[key]
			for _, candidate := range q.backfillCandidates(j, m, projected, resourceRoom(projected)) {
				if !environmentReady(candidate, m) || q.capReasonFor(candidate, m) != "" || !fits(candidate, projected, projected.FreeMemoryGB) {
					continue
				}
				ready := fits(candidate, m, m.FreeMemoryGB)
				if orders[key] == nil {
					orders[key] = &candidateSet{q: q, scores: map[string]float64{}, actives: map[string]float64{}}
				}
				orders[key].jobs = append(orders[key].jobs, candidate)
				bestReady := best != nil && fits(best.job, best.machine, best.machine.FreeMemoryGB)
				if best == nil || ready && !bestReady || ready == bestReady && q.targetPlacementBetter(candidate, m, best.machine, scan) {
					best = &hostBackfillDemand{job: candidate, machine: m, drain: drain}
				}
			}
		}
		if best != nil {
			if start != nil {
				best.fleetProbe = true
				best.probeReady = q.fleetProofFresh(best.machine.Fleet) && fits(best.job, best.machine, best.machine.FreeMemoryGB)
				probes[hostID(best.machine)] = best
				continue
			}
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
		if probe := probes[host]; probe != nil {
			if jobClass(choice.job.Spec) != "production" {
				continue
			}
			choice.startProbe = probe
		} else if !choice.drain {
			continue // Ordinary production reservations use the pooled rule below.
		}
		previous := scan.hostBackfill[host]
		if previous == nil || q.hostBackfillBefore(choice.job, previous.job, choice.machine, set) {
			scan.hostBackfill[host] = choice
		}
	}
	for host, probe := range probes {
		if scan.hostBackfill[host] == nil {
			scan.hostBackfill[host] = probe
		}
	}
	q.addPoolBackfillHeads(scan, demands, live)
	return scan.hostBackfill
}

// Host RAM is shared by CPU and device views. Apply accelerator-first backfill
// ordering consistently on both, including CPU maintenance resumes. Resume
// promotion must not refill the RAM a queued accelerator needs. Tiers and an
// already selected preemption release retain their existing precedence.
func (q *Queue) hostBackfillBefore(a, b *Job, m *Machine, order *candidateSet) bool {
	if q.comparePriority(a.Spec, b.Spec) == 0 && (a.PreemptionPlacement != nil) == (b.PreemptionPlacement != nil) && a.Spec.Needs.cpuOnly() != b.Spec.Needs.cpuOnly() {
		return !a.Spec.Needs.cpuOnly()
	}
	return q.beforeWithFair(a, b, m, order.score, order.active)
}

func (q *Queue) hostBackfillAllows(j *Job, m *Machine, fence *hostBackfillDemand) bool {
	return q.hostBackfillBlocker(j, m, fence) == nil
}

// Cold model loads and new preparation can claim host capacity before the
// next batch offer. Apply the same reservation to those new claims only.
func (q *Queue) fleetProbeBackfillAllows(j *Job, m *Machine, scans ...*placementScan) bool {
	u := q.s.FleetUpgrade
	if u == nil || u.State != "running" || jobClass(j.Spec) == "production" {
		return true
	}
	for _, h := range u.Hosts {
		if h.Host != hostID(m) || h.Outcome != FleetHostDeferred || !fleetProbePending(q.s.Jobs[h.StartProbe]) || !q.clock.Now().Before(h.startCapacityDeadline()) {
			continue
		}
		fence := q.hostBackfillFences(q.placementScan(scans...))[h.Host]
		return fence == nil || !fence.fleetProbe && fence.startProbe == nil || q.hostBackfillAllows(j, m, fence)
	}
	return true
}

func (q *Queue) hostBackfillBlocker(j *Job, m *Machine, fence *hostBackfillDemand) *Job {
	if fence == nil || j.ID == fence.job.ID {
		return nil
	}
	if fence.pooled {
		if q.beforeWithFair(j, fence.job, fence.machine, fence.order.score, fence.order.active) || q.poolBackfillFits(j, m, fence) {
			return nil
		}
		return fence.job
	}
	production := jobClass(j.Spec) == "production"
	switch {
	case fence.fleetProbe:
		if !production && !fleetProbeBackfillFits(j, m, fence, nil) {
			return fence.job
		}
		return nil
	case fence.startProbe != nil && !production:
		// The probe and lower work can use only surplus above production's
		// projected demand, even under a custom policy that puts them first.
		if !q.hostBackfillFits(j, m, fence) {
			return fence.job
		}
		if j.ID != fence.startProbe.job.ID && !fleetProbeBackfillFits(j, m, fence.startProbe, fence) {
			return fence.startProbe.job
		}
		return nil
	}
	// Keep the existing tier and fair-turn order, including accelerator-first
	// ordering across CPU/device views of the same worker. A demand protects
	// capacity only against work behind it, never against its own admission.
	if q.hostBackfillBefore(j, fence.job, m, fence.order) || q.hostBackfillFits(j, m, fence) {
		return nil
	}
	return fence.job
}

func fleetProbeBackfillFits(j *Job, m *Machine, probe, production *hostBackfillDemand) bool {
	// A worker only receives one offer per pull. Take that turn when the
	// probe is ready, even if the experiment would fit alongside it.
	return !(probe.probeReady && m.ID == probe.machine.ID) && hostBackfillRoom(j, m, m, probe, production)
}

func hostBackfillRoom(j *Job, m, room *Machine, fence, extra *hostBackfillDemand) bool {
	need := fence.job.Spec.Needs
	memory, cores := hostNeed(fence.machine, need.MemoryGB, need.HostMemoryGB), need.cores()
	if extra != nil {
		need = extra.job.Spec.Needs
		memory += hostNeed(extra.machine, need.MemoryGB, need.HostMemoryGB)
		cores += need.cores()
	}
	// Backfill can use only capacity in excess of the demand. While a dimension
	// is short, new work cannot deepen that deficit; other hosts remain free.
	return hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) <= max(0, room.FreeHostMemoryGB-room.hostReservationDebtGB-memory) &&
		j.Spec.Needs.cores() <= max(0, room.FreeCPUCores-cores)
}

func (q *Queue) hostBackfillFits(j *Job, m *Machine, fence *hostBackfillDemand) bool {
	room := hostBackfillRoom(j, m, m, fence, nil)
	if !fence.drain {
		return room
	}
	// A maintenance resume also keeps its accelerator: later work does not
	// take that card. Either way, work expected to finish before the resume
	// could start anyway still backfills.
	if !fence.job.Spec.Needs.cpuOnly() && !j.Spec.Needs.cpuOnly() && targetKey(m) == targetKey(fence.machine) {
		room = false
	}
	return room || q.finishesBeforeDrain(j, m, fence.start)
}

func (q *Queue) hostBackfillWaitingReason(j *Job, scan *placementScan) string {
	var demand *Job
	var poolFence bool
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
			blocker := q.hostBackfillBlocker(candidate, m, fence)
			if blocker == nil {
				return "" // Another currently fitting host is not withheld.
			}
			if demand == nil || blocker.ID < demand.ID {
				demand = blocker
				poolFence = fence.pooled
			}
		}
	}
	if demand != nil {
		if poolFence {
			return fmt.Sprintf("memory_wait: waiting for job %s to acquire its reserved pool capacity", demand.ID)
		}
		return fmt.Sprintf("host_capacity_accumulation: waiting for job %s to acquire host RAM/cores; running attempts finish normally", demand.ID)
	}
	return ""
}
