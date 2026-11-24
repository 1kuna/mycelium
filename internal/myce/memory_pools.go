package myce

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

const pooledMemoryCapability = "pooled_memory_v1"
const sampledMemoryCapability = "sampled_memory_pool_v1"

// MemoryPools is the resolved memory contract for one assignment. A unified
// machine accounts for accelerator allocations in RAM and has no Card pool.
type MemoryPools struct {
	RAM     memoryPoolEstimate  `json:"ram"`
	Card    *memoryPoolEstimate `json:"card,omitempty"`
	KeepGB  float64             `json:"keep_gb"`
	LimitGB float64             `json:"limit_gb,omitempty"`
}

func pooledMemory(j *Job) bool {
	return j.Memory != nil || j.MemoryEnvelope != nil && j.MemoryEnvelope.Basis == pooledMemoryCapability
}

func memoryPools(j *Job) MemoryPools {
	if j.Memory != nil {
		return *j.Memory
	}
	// The adjacent queue preserves the envelope if it rewrites a job carrying
	// new fields. A new worker can still honor that attempt's containment.
	return MemoryPools{RAM: memoryPoolEstimate{TypicalGB: j.MemoryEnvelope.HostGB, ChargeGB: j.MemoryEnvelope.HostGB}, LimitGB: j.MemoryEnvelope.LimitHostGB}
}

func (j *Job) restorePoolCustody(copy Copy) {
	if copy.Memory == nil {
		return
	}
	j.Memory = copy.Memory
	host := max(j.Memory.RAM.ChargeGB, minimumHostMemoryGB)
	device := 0.0
	if j.Memory.Card != nil {
		device = j.Memory.Card.ChargeGB * float64(max(1, len(copy.AllocatedDevices)))
	}
	j.Spec.Needs.HostMemoryGB, j.Spec.Needs.MemoryGB = host, device
	j.MemoryEnvelope = &JobMemoryEnvelope{Machine: copy.Machine, Source: "memory pool estimate", Basis: pooledMemoryCapability,
		HostGB: j.Memory.RAM.ChargeGB, DeviceGB: device, LimitHostGB: j.Memory.LimitGB}
}

const memoryWaitReason = "memory_wait"

func workerUsesMemoryPools(m *Machine) bool {
	return m != nil && slices.Contains(m.Capabilities, pooledMemoryCapability)
}

const (
	workerMemoryReleaseReason     = "worker_release_floor"
	workerMemoryContainmentReason = "memory_containment_unavailable"
)

func workerMemoryHold(m *Machine) *Hold {
	// In-process planner fixtures and synthetic targets lack a release report.
	if m.Release == nil {
		return nil
	}
	if !workerUsesMemoryPools(m) {
		return newHold(workerMemoryReleaseReason, "worker must upgrade before receiving new work")
	}
	if !slices.Contains(m.Capabilities, sampledMemoryCapability) &&
		(m.MemoryContainment == nil || m.MemoryContainment.State != "available" || m.HostMemoryBudgetGB <= 0) {
		return newHold(workerMemoryContainmentReason, "worker must confirm its memory slice ceiling before receiving new work")
	}
	return nil
}

// The r6.35 queue supplies an envelope. Keep its figures as estimates for
// newly offered work. Existing worker stores keep the cgroups they started in.
func (w Worker) acceptMemoryOffer(j *Job) {
	if j.Memory != nil {
		return
	}
	if pooledMemory(j) {
		pools := memoryPools(j)
		j.Memory = &pools
		return
	}
	m := localJobMachine(w, *j)
	n := j.Spec.Needs
	host, device := hostNeed(&m, n.MemoryGB, n.HostMemoryGB), n.MemoryGB
	if e := j.MemoryEnvelope; e != nil {
		host, device = max(host, e.HostGB), max(device, e.DeviceGB)
	}
	pools := MemoryPools{RAM: memoryPoolEstimate{TypicalGB: host, ChargeGB: host}, KeepGB: w.HostMemoryReserveGB}
	if j.Spec.MemoryLimitGB != nil {
		pools.LimitGB = *j.Spec.MemoryLimitGB
	}
	if !n.cpuOnly() && !m.UnifiedMemory {
		perCard := device / float64(max(1, len(j.AllocatedDevices)))
		card := memoryPoolEstimate{TypicalGB: perCard, ChargeGB: perCard}
		pools.Card = &card
	}
	j.Memory = &pools
}

func pooledEvidenceKey(j *Job, m *Machine) memoryEvidenceKey {
	target := machineTarget(m)
	if j.Spec.Needs.cpuOnly() {
		target.Platform = "cpu"
	}
	configuration := memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
	key := ObservationKey{Concurrency: 1, BatchSize: 1}
	identity := *j
	if j.Spec.extensionJob() {
		if e := qualifiedExtension(j, m); e != nil {
			identity.Extension = &ExtensionSelection{Digest: e.Digest}
			key.ExtensionDigest = e.Digest
			if variant := e.variant(target); variant != nil {
				key.Reference = variant.References[j.Spec.workType()]
			}
		}
		key.Work = j.Spec.workType()
		key.Check = j.Spec.ExtensionCheck != nil
	} else {
		key.Work = profileMemoryIdentity(j)
	}
	if len(m.groupMembers) > 1 || len(j.AllocatedDevices) > 1 {
		key.Concurrency = max(len(m.groupMembers), len(j.AllocatedDevices))
	}
	return memoryEvidenceKey{Platform: memoryPlatform(target, m.UnifiedMemory), Pool: "ram", Family: memoryFamily(&identity), Exact: memoryEvidenceExact(key, configuration)}
}

func (q *Queue) pooledMemoryCandidate(j *Job, m *Machine) *Job {
	n := j.Spec.Needs
	key := pooledEvidenceKey(j, m)
	hostHint, deviceHint := n.HostMemoryGB, n.MemoryGB
	hostHint = max(hostHint, j.Spec.SizeHint.HostMemoryGB)
	deviceHint = max(deviceHint, j.Spec.SizeHint.MemoryGB)
	if !n.cpuOnly() && m.UnifiedMemory {
		hostHint += deviceHint
	}
	if j.MemoryFallback == nil {
		j.MemoryFallback = &SizeHint{HostMemoryGB: n.HostMemoryGB, MemoryGB: n.MemoryGB}
	}
	evidence := q.memoryEvidence
	if evidence == nil {
		evidence = &memoryEvidence{}
	}
	confidence := evidence.confidence()
	pools := MemoryPools{RAM: evidence.estimate(key, hostHint, confidence), KeepGB: q.pooledKeep(m)}
	if !n.cpuOnly() && pools.RAM.Alone {
		// A cold accelerator job shares RAM. Missing device evidence can
		// require a discrete card to itself, but never the entire host.
		if pools.RAM.TypicalGB == 0 {
			cpu := memoryEvidenceKey{Platform: memoryPlatform(ExecutionTarget{OS: machineTarget(m).OS, Platform: "cpu"}, false), Pool: "ram"}
			prior := evidence.first[key.platformKey()]
			if len(prior) == 0 {
				prior = evidence.first[cpu]
			}
			if len(prior) > 0 {
				pools.RAM = memoryPoolEstimate{TypicalGB: memoryMedian(prior), ChargeGB: slices.Max(prior),
					Evidence: memoryEstimateEvidence{Level: "platform", Runs: len(prior)}}
			}
		}
		pools.RAM.Alone = false
		pools.RAM.TypicalGB = max(pools.RAM.TypicalGB, minimumHostMemoryGB)
		pools.RAM.ChargeGB = max(pools.RAM.ChargeGB, pools.RAM.TypicalGB)
	}
	if j.Spec.MemoryLimitGB != nil {
		pools.LimitGB = *j.Spec.MemoryLimitGB
		pools.RAM.TypicalGB = min(pools.RAM.TypicalGB, pools.LimitGB)
		pools.RAM.ChargeGB = min(pools.RAM.ChargeGB, pools.LimitGB)
	}
	fitPoolCharge(&pools.RAM, q.poolAllowance(m), evidence.exact[key].Lower)
	n.MemoryGB = 0
	if !n.cpuOnly() && !m.UnifiedMemory {
		key.Pool = "card"
		count := max(1, len(m.groupMembers))
		card := evidence.estimate(key, deviceHint/float64(count), confidence)
		fitPoolCharge(&card, m.TotalMemoryGB/float64(count), evidence.exact[key].Lower)
		pools.Card, n.MemoryGB = &card, card.ChargeGB*float64(count)
	}
	// The adjacent planner carries a positive scalar RAM declaration. Pool
	// accounting and the worker's cgroup use the resolved contract above.
	n.HostMemoryGB = max(pools.RAM.ChargeGB, minimumHostMemoryGB)
	j.Memory, j.Spec.Needs = &pools, n
	j.MemoryEnvelope = &JobMemoryEnvelope{Machine: m.ID, Device: m.Device, Basis: pooledMemoryCapability,
		Source: "memory pool estimate", HostGB: pools.RAM.ChargeGB, DeviceGB: n.MemoryGB, LimitHostGB: pools.LimitGB}
	q.observeMemoryAdmission(j, m)
	return j
}

// A cautious estimate above the entire pool would strand work even after all
// holders release. Use the typical estimate on that target, preserving the
// retry floor from measured shortages and the ordinary live-room check.
func fitPoolCharge(estimate *memoryPoolEstimate, allowance, lower float64) {
	floor := max(estimate.TypicalGB, 2*lower)
	if allowance > 0 && estimate.ChargeGB > allowance && floor <= allowance {
		estimate.ChargeGB = floor
	}
}

func (q *Queue) pooledKeep(m *Machine) float64 {
	keep := 0.0
	if o := m.MemoryObservation; o != nil {
		keep = o.ReserveGB
	}
	if d := m.HostMemoryDerivation; d != nil {
		keep = max(keep, d.ReserveGB)
	}
	if p := q.s.HostMaintenance[hostID(m)].SharedHost; p != nil {
		keep = max(keep, p.InteractiveReserveGB)
	}
	return keep
}

func (q *Queue) poolAllowance(m *Machine) float64 {
	if m.HostMemoryDerivation != nil || m.EffectiveHostMemoryBudgetGB > 0 {
		return m.EffectiveHostMemoryBudgetGB
	}
	if m.HostMemoryBudgetGB > 0 {
		return m.HostMemoryBudgetGB
	}
	if m.CapabilityProfile == nil {
		return 0
	}
	return max(0, float64(m.CapabilityProfile.SystemMemoryBytes)/(1<<30)-q.pooledKeep(m))
}

func (q *Queue) addPoolBackfillHeads(scan *placementScan, demands []*Job, live map[string]bool) {
	byHost := map[string][]*hostBackfillDemand{}
	ready := map[string]bool{}
	for _, m := range scan.machines {
		host := hostID(m)
		if workerMemoryHold(m) != nil || !live[m.ID] || scan.hostBackfill[host] != nil || q.preparationActive(m) || q.preparationStopping(m) {
			continue
		}
		capacity := q.qualificationCapacity(m)
		// Project room after holders finish, while keeping today's device eligibility.
		capacity.InventoryProbe, capacity.MemoryError = m.InventoryProbe, m.MemoryError
		capacity.FreeHostMemoryGB = q.sharedHostRoom(&capacity, q.poolAllowance(m), 0)
		if terms := q.hostMemoryTerms(m); terms != nil {
			// The ledger already credits the measured part of current charges.
			// A larger configured allowance cannot free other physical RAM.
			capacity.FreeHostMemoryGB = min(capacity.FreeHostMemoryGB, terms.CapacityGB)
		}
		for _, j := range demands {
			if !q.placementAllowed(j, m) || !q.preemptionPlacementAllows(j, m) || q.preemptionReleasesPending(j) || !q.checkpointReady(j, m) || q.environmentFailureBlocked(j, m) {
				continue
			}
			for _, candidate := range q.memoryCandidates(j, m) {
				if !pooledMemory(candidate) || !environmentReady(candidate, m) || q.capReasonFor(candidate, m) != "" || !fits(candidate, &capacity, capacity.FreeMemoryGB) {
					continue
				}
				if fits(candidate, m, m.FreeMemoryGB) && q.batchSlotFree(m, candidate) && q.poolCustodyAllows(candidate, m) {
					ready[j.ID] = true
					continue
				}
				byHost[host] = append(byHost[host], &hostBackfillDemand{job: candidate, machine: m, pooled: true})
			}
		}
	}
	for host, choices := range byHost {
		set := &candidateSet{q: q, scores: map[string]float64{}, actives: map[string]float64{}}
		var stages []*hostBackfillDemand
		for _, c := range choices {
			if !ready[c.job.ID] {
				set.jobs = append(set.jobs, c.job)
				if c.job.Spec.After != nil {
					stages = append(stages, c)
				}
			}
		}
		var head *hostBackfillDemand
		for _, c := range choices {
			if ready[c.job.ID] {
				continue
			}
			if head == nil || q.beforeWithFair(c.job, head.job, c.machine, set.score, set.active) {
				head = c
			}
		}
		if head == nil {
			continue
		}
		root := head
		if freshPipelineRoot(root.job) {
			for _, c := range stages {
				if q.pipelineStagePrecedes(c.job, root.job) && (head == root || q.beforeWithFair(c.job, head.job, c.machine, set.score, set.active)) {
					head = c
				}
			}
		}
		if head != root {
			head.fairHead = root
		}
		head.order = set
		scan.hostBackfill[host] = head
	}
}

func (q *Queue) poolBackfillFits(j *Job, m *Machine, head *hostBackfillDemand) bool {
	pool := memoryPools(head.job).RAM
	if pool.Alone || jobCPUCharge(j, m) > max(0, m.FreeCPUCores-jobCPUCharge(head.job, head.machine)) {
		return false
	}
	capacity := *m
	budget := q.sharedHostRoom(&capacity, q.poolAllowance(m), 0) - pool.ChargeGB
	ledger := q.hostMemoryLedgers[m.ID]
	if ledger == nil {
		return false
	}
	for _, holder := range ledger.Owners {
		if holder.Execution == nil {
			budget -= holder.ChargeGB
			continue
		}
		running := q.s.Jobs[holder.Execution.ID]
		if running == nil || running.AssignedAt == nil || running.Attempt != holder.Execution.Attempt || !running.AssignedAt.Before(head.job.WaitingSince) {
			budget -= holder.ChargeGB
		}
	}
	if pooledMemory(j) {
		return !memoryPools(j).RAM.Alone && memoryPools(j).RAM.ChargeGB <= budget
	}
	return hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) <= budget
}

func (q *Queue) poolCapacityFailure(j *Job, m *Machine) string {
	pools := memoryPools(j)
	allowance := q.poolAllowance(m)
	if (allowance > 0 || m.HostMemoryBudgetMeasured) && j.Spec.Needs.HostMemoryGB > allowance {
		return fmt.Sprintf("RAM needs %.3f GiB (typical %.3f GiB); host allowance %.3f GiB", j.Spec.Needs.HostMemoryGB, pools.RAM.TypicalGB, allowance)
	}
	if pools.Card != nil && m.TotalMemoryGB > 0 && j.Spec.Needs.MemoryGB > m.TotalMemoryGB {
		count := float64(max(1, len(m.groupMembers)))
		return fmt.Sprintf("card needs %.3f GiB (typical %.3f GiB); card allowance %.3f GiB", pools.Card.ChargeGB, pools.Card.TypicalGB, m.TotalMemoryGB/count)
	}
	return ""
}

func (q *Queue) poolCustodyWaitingReason(j *Job, m *Machine) string {
	var holders []string
	for _, o := range q.hostObligations(hostID(m)) {
		if !q.poolCustodyConflicts(j, m, o) {
			continue
		}
		switch {
		case o.Engine != nil:
			holders = append(holders, "model "+o.Engine.Model)
		case o.Preparation:
			holders = append(holders, "worker preparation")
		default:
			holders = append(holders, fmt.Sprintf("job %s attempt %d", o.Result.ID, o.Result.Attempt))
		}
	}
	if len(holders) == 0 {
		return ""
	}
	slices.Sort(holders)
	return "memory pool custody held by " + strings.Join(slices.Compact(holders), ", ")
}

func (q *Queue) poolCustodyAllows(j *Job, m *Machine) bool {
	if !pooledMemory(j) {
		return true
	}
	for _, obligation := range q.hostObligations(hostID(m)) {
		if q.poolCustodyConflicts(j, m, obligation) {
			return false
		}
	}
	return true
}

func (q *Queue) poolCustodyConflicts(j *Job, m *Machine, obligation HostObligation) bool {
	if obligation.Result.ID == j.ID || obligation.Call != nil {
		return false
	}
	if obligation.Launch != nil && (!obligation.Launch.Started || obligation.Launch.Kind == "copy") {
		return false // A preparation offer or background copy holds no execution pool.
	}
	pools := memoryPools(j)
	if e := obligation.Engine; e != nil {
		if slot := q.s.ModelSlots[slotKey(e.Machine, e.Model, e.Device)]; slot != nil && e.Machine == m.ID && ownsResidentSlot(j.Spec, slot) {
			return false
		}
		return pools.RAM.Alone || pools.Card != nil && pools.Card.Alone && e.Machine == m.ID && (e.Device == "" || slices.Contains(targetDevices(m), e.Device))
	}
	peer := q.s.Jobs[obligation.Result.ID]
	return pools.RAM.Alone || peer != nil && (pooledMemory(peer) && memoryPools(peer).RAM.Alone ||
		pools.Card != nil && pools.Card.Alone && batchOnTarget(peer, m))
}

// Exclusivity is part of the ordinary release plan. The final offer still
// checks real custody and waits for each selected release to be confirmed.
func (q *Queue) planPoolCustody(j *Job, m *Machine, plan *placementPlan) bool {
	for _, obligation := range q.hostObligations(hostID(m)) {
		// The ordinary planner handles preparation releases and the final
		// admission gate still checks their custody.
		if obligation.Preparation || !q.poolCustodyConflicts(j, m, obligation) {
			continue
		}
		if e := obligation.Engine; e != nil {
			slot := q.s.ModelSlots[slotKey(e.Machine, e.Model, e.Device)]
			if slot == nil || slot.Attempt != e.Attempt || slot.Session != e.Session || !q.slotReclaimableFor(slot, j.Spec) {
				return false
			}
			plan.addSlot(slot)
			continue
		}
		r := obligation.Result
		peer := q.s.Jobs[r.ID]
		if peer == nil || peer.Attempt != r.Attempt || peer.Session != r.Session || peer.Machine != r.Machine ||
			!batchReservesCapacity(peer) || !q.batchPreemptible(peer, j.Spec) && plan.yields[peer.ID] == nil {
			return false
		}
		plan.addJob(peer)
	}
	return true
}

func pooledFootprint(sample hostMemorySample, unified bool) float64 {
	if sample.footprintGB > 0 {
		return sample.footprintGB
	}
	if unified {
		return sample.gb + sample.deviceGB
	}
	return sample.gb
}

func pooledHostCharge(j *Job, m *Machine, sample hostMemorySample, now time.Time, charge float64) (held, pending float64) {
	if pooledMemory(j) {
		charge = memoryPools(j).RAM.ChargeGB
	} else if j.MemoryEnvelope != nil {
		charge = j.MemoryEnvelope.HostGB
	}
	current := pooledFootprint(sample, unifiedCUDA(m))
	held = max(charge, sample.peakGB, current, retainedHostCharge(j))
	if j.Memory != nil {
		held = max(held, j.Memory.RAM.PeakGB)
	}
	if !sample.freshAt(now) {
		current = 0
	}
	return held, max(0, held-current)
}

func retainPoolPeaks(j *Job, host, device float64) {
	if j.Memory == nil {
		return
	}
	p := *j.Memory
	update := func(pool memoryPoolEstimate, peak float64) memoryPoolEstimate {
		pool.PeakGB = max(pool.PeakGB, peak)
		pool.HeldGB = max(pool.PeakGB, pool.ChargeGB)
		return pool
	}
	p.RAM = update(p.RAM, host)
	if p.Card != nil {
		card := update(*p.Card, device)
		if p.RAM == j.Memory.RAM && card == *p.Card {
			return
		}
		p.Card = &card
	} else if p.RAM == j.Memory.RAM {
		return
	}
	j.Memory = &p
}

func (q *Queue) recordPoolEvidence(j *Job, r Result) {
	pressure := j.Reason == memoryPressureCause || r.ExecutionCause != nil && r.ExecutionCause.Class == memoryPressureCause
	if q.memoryEvidence == nil || r.ReleaseOnly || r.MemoryPeak == nil || j.StartedAt == nil || !r.Success && !r.OutOfMemory && !pressure {
		return
	}
	m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if m == nil {
		return
	}
	key := pooledEvidenceKey(j, m)
	limited := r.OutOfMemory || pressure || pooledSampleLimited(r.MemoryPeak)
	points := appendProjectedMemoryPeaks(nil, key, memoryEvidencePoint{At: q.clock.Now(), Limited: limited}, r.MemoryPeak)
	if j.Memory != nil {
		for _, point := range points {
			if point.Key.Pool == "ram" {
				retainPoolPeaks(j, point.Peak, 0)
			} else {
				retainPoolPeaks(j, 0, point.Peak)
			}
		}
	}
	if len(points) == 0 {
		return
	}
	evidence := q.memoryEvidence
	if !q.simulation {
		if q.memoryEvidencePending == nil {
			q.memoryEvidencePending = q.memoryEvidence.stage()
		}
		evidence = q.memoryEvidencePending
	}
	for _, point := range points {
		if pooledMemory(j) {
			pools := memoryPools(j)
			estimate := pools.RAM
			if point.Key.Pool == "card" && pools.Card != nil {
				estimate = *pools.Card
			}
			if point.Key.Pool == "ram" {
				evidence.observeFeedback(estimate.ChargeGB, point.Peak, pressure || r.OutOfMemory, r.Success && !limited)
			}
			if pressure && point.Peak <= estimate.ChargeGB {
				continue // A bystander stop provides no new estimate.
			}
		}
		evidence.observe(point)
	}
}

func pooledSampleLimited(p *AttemptMemoryPeak) bool {
	e := p.Events
	return memoryValue(p.SwapGB) > 0 || p.Pressure != nil && p.Pressure.CgroupSwapOutPages > 0 || e != nil && (e.Max > 0 || e.OOM > 0 || e.OOMKill > 0 || e.OOMGroupKill > 0)
}

func (q *Queue) requeueMemoryPressure(j *Job) {
	e := q.memoryEvidencePending
	if e == nil {
		e = q.memoryEvidence
	}
	var routes int
	var largestLowerBound float64
	if e != nil {
		for _, m := range q.plannerMachines() {
			if !q.jobAllowsMachine(j, m.ID) || !memorySizingTargetFits(j, m) || !toolsFit(j, m) || !nativeNetworkFits(j, m) {
				continue
			}
			key := pooledEvidenceKey(j, m)
			lower := e.exact[key].Lower
			allowance := q.poolAllowance(m)
			if workerMemoryHold(m) != nil {
				continue
			}
			card := key
			card.Pool = "card"
			cardLower := 0.0
			if !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory {
				cardLower = e.exact[card].Lower
			}
			if (allowance <= 0 || 2*lower <= allowance) && (cardLower == 0 || 2*cardLower <= m.TotalMemoryGB) {
				q.requeue(j, memoryPressureCause, false)
				return
			}
			routes++
			largestLowerBound = max(largestLowerBound, lower, cardLower)
		}
	}
	if routes == 0 {
		q.requeue(j, memoryPressureCause, false)
		return
	}
	j.State, j.DiagnosticCode = "failed", "memory_capacity"
	j.Reason = fmt.Sprintf("memory_capacity: %.3f GiB measured under memory pressure, and the retry charge exceeds every eligible memory pool", largestLowerBound)
}

func memoryWithinAllowance(m *Machine, free, keep, held, pending float64) float64 {
	if budget := m.HostMemoryBudgetGB; budget > 0 || m.HostMemoryBudgetMeasured {
		return min(free, budget-max(0, held-pending)+keep)
	}
	return free
}

func checkMemoryPool(typical, available, keep, pending float64, known bool) error {
	if known && typical <= available-keep-pending {
		return nil
	}
	detail := AdmissionDetail{Subcheck: memoryWaitReason, Unit: "GiB", Required: typical, Unavailable: !known, KeepGB: keep, HeldGB: pending}
	if known {
		detail.Available = &available
	}
	return &admissionError{detail}
}

func localPooledAdmission(w Worker, jobs []Job, next Job, models []ModelReport, preparationCores []int) error {
	pools := memoryPools(&next)
	residentModel := ""
	if r := next.Spec.Resident; r != nil && r.Engine != nil {
		residentModel = r.Engine.Model.Name
	}
	cores := next.Spec.Needs.cores()
	for _, n := range preparationCores {
		cores += n
	}
	var held, pending float64
	for _, j := range jobs {
		if j.ReleaseConfirmed {
			continue
		}
		cores += j.Spec.Needs.cores()
		owner := allocationMachine(&w.Machine, j.AllocatedDevices)
		charge, growth := pooledHostCharge(&j, owner, w.hostMemorySamples[jobKey(j)], w.Clock.Now(), localJobHostNeed(w, j))
		held += charge
		pending += growth
		if !next.Spec.Needs.cpuOnly() && !j.Spec.Needs.cpuOnly() && (!sharesDevice(&next) || !sharesDevice(&j)) &&
			slices.ContainsFunc(attemptCards(next, w.Machine), func(id string) bool { return slices.Contains(attemptCards(j, w.Machine), id) }) {
			return refuseAdmission("local_device_reservation", "slots", 1, 0, false)
		}
		if pools.RAM.Alone || pooledMemory(&j) && memoryPools(&j).RAM.Alone {
			return checkMemoryPool(pools.RAM.TypicalGB, 0, 0, pending, false)
		}
	}
	for _, model := range models {
		if model.State == "ready" || model.State == "loading" || model.State == "draining" {
			cores++
			if budget, ok := w.localModelBudget(model); ok {
				charge := hostNeed(allocationMachine(&w.Machine, []string{model.Device}), budget.MemoryGB, budget.HostMemoryGB)
				held, pending = held+charge, pending+charge
			}
			if pools.RAM.Alone && model.Name != residentModel {
				return checkMemoryPool(pools.RAM.TypicalGB, 0, 0, pending, false)
			}
		}
	}
	capacity, reason := cpuCapacity(&w.Machine)
	if reason != "" || cores > capacity {
		return refuseAdmission("aggregate_cpu_allowance", "cores", float64(cores), float64(capacity), reason != "")
	}
	if w.Machine.MaxJobs > 0 && len(jobs) >= w.Machine.MaxJobs {
		return refuseAdmission("local_max_jobs", "jobs", float64(len(jobs)+1), float64(w.Machine.MaxJobs), false)
	}
	if err := w.checkJobRAMWithReservations(context.Background(), next, held, pending); err != nil {
		return err
	}
	if pools.Card == nil {
		return nil
	}
	for _, id := range attemptCards(next, w.Machine) {
		var used float64
		for _, j := range jobs {
			if !j.ReleaseConfirmed && !j.Spec.Needs.cpuOnly() && slices.Contains(attemptCards(j, w.Machine), id) {
				used += max(deviceShareGB(&j), w.hostMemorySamples[jobKey(j)].devicePeakGB)
			}
		}
		for _, model := range models {
			if model.Device == id && (model.State == "ready" || model.State == "loading" || model.State == "draining") {
				if pools.Card.Alone && model.Name != residentModel {
					return checkMemoryPool(pools.Card.TypicalGB, 0, 0, used, false)
				}
				if budget, ok := w.localModelBudget(model); ok {
					used += budget.MemoryGB
				}
			}
		}
		index := slices.IndexFunc(w.Machine.Devices, func(d MachineDevice) bool { return d.ID == id })
		if index < 0 {
			return checkMemoryPool(pools.Card.TypicalGB, 0, 0, used, false)
		}
		card := w.Machine.Devices[index]
		free := max(0, card.BudgetGB-used)
		if card.MemoryMeasured {
			free = min(free, card.FreeMemoryGB)
		}
		if err := checkMemoryPool(pools.Card.TypicalGB, free, 0, 0, card.MemoryMeasured || card.MemoryMode == "fixed" && card.BudgetGB > 0); err != nil {
			return err
		}
	}
	return nil
}

func memoryIntent(j *Job) *Job {
	copy := *j
	copy.MemoryEnvelope = nil
	copy.Spec.Needs.Memory = ""
	n := &copy.Spec.Needs
	n.HostMemoryGB = max(n.HostMemoryGB, minimumHostMemoryGB, j.Spec.Run.MemoryGB)
	for _, side := range j.Spec.Run.Sidecars {
		n.HostMemoryGB = max(n.HostMemoryGB, side.MemoryGB)
	}
	n.MemoryGB = max(n.MemoryGB, j.DeviceMemoryFloorGB)
	return &copy
}

const declareMemory = "declare positive memory_gb (host RAM for CPU work, GPU memory otherwise)"

// A plain job that states no usable memory figure, judged on normalized needs
// as Validate does. Partial figures stay hints, as with an explicit auto.
func (s Spec) memoryOmitted() bool {
	if normalizeResources(&s) != nil {
		return false // Submission reports the invalid field itself.
	}
	n := s.Needs
	return !s.extensionJob() && n.Memory == "" && (!n.cpuOnly() && n.MemoryGB <= 0 || n.cpuOnly() && n.HostMemoryGB <= 0)
}

// Omitted memory means auto: agents state needs, Mycelium learns footprints.
func (q *Queue) defaultMemory(s *Spec) error {
	if !s.memoryOmitted() {
		return nil
	}

	s.Needs.Memory = "auto"
	return nil
}

// Plain jobs share memory evidence by project and name, extension jobs by
// extension and work type, across configurations. It only bounds a first run.
func memoryFamily(j *Job) string {
	if j.Spec.extensionJob() {
		digest := ""
		if j.Extension != nil {
			digest = j.Extension.Digest
		}
		work := j.Spec.workType()
		if j.Spec.ExtensionCheck != nil {
			work = "check:" + work // checks and workloads learn separately (#618)
		}
		return identityHash(struct{ Extension, Work string }{digest, work})
	}
	return identityHash(struct{ Project, Name string }{j.Spec.Project, j.Spec.Name})
}
