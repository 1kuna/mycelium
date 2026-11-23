package myce

import (
	"context"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"time"
)

const pooledMemoryCapability = "pooled_memory_v1"

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

const memoryWaitReason = "memory_wait"

func workerUsesMemoryPools(m *Machine) bool {
	return m != nil && slices.Contains(m.Capabilities, pooledMemoryCapability)
}

func (w Worker) supportsMemoryPools() bool {
	if w.HostMemorySource == nil && w.hostRawMemorySource == nil {
		return false
	}
	switch runtime.GOOS {
	case "linux":
		return w.memoryCeilings != nil && w.Machine.MemoryContainment != nil && w.Machine.MemoryContainment.State == "available"
	case "darwin":
		return w.processMemory != nil && w.processMemory.OwnedFootprint != nil
	default:
		return false
	}
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
	confidence := q.memoryEvidence.confidence()
	pools := MemoryPools{RAM: q.memoryEvidence.estimate(key, hostHint, confidence), KeepGB: q.pooledKeep(m)}
	if j.Spec.MemoryLimitGB != nil {
		pools.LimitGB = *j.Spec.MemoryLimitGB
		pools.RAM.TypicalGB = min(pools.RAM.TypicalGB, pools.LimitGB)
		pools.RAM.ChargeGB = min(pools.RAM.ChargeGB, pools.LimitGB)
	}
	n.MemoryGB = 0
	if !n.cpuOnly() && !m.UnifiedMemory {
		key.Pool = "card"
		count := max(1, len(m.groupMembers))
		card := q.memoryEvidence.estimate(key, deviceHint/float64(count), confidence)
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
	if m.HostMemoryDerivation != nil {
		return m.HostMemoryDerivation.AllowanceGB
	}
	if m.EffectiveHostMemoryBudgetGB > 0 {
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
	for _, m := range scan.machines {
		host := hostID(m)
		if !workerUsesMemoryPools(m) || !live[m.ID] || scan.hostBackfill[host] != nil || q.preparationActive(m) || q.preparationStopping(m) {
			continue
		}
		capacity := q.qualificationCapacity(m)
		capacity.FreeHostMemoryGB = q.poolAllowance(m)
		for _, j := range demands {
			if !q.placementAllowed(j, m) || !q.preemptionPlacementAllows(j, m) || q.preemptionReleasesPending(j) || !q.checkpointReady(j, m) || q.environmentFailureBlocked(j, m) {
				continue
			}
			for _, candidate := range q.memoryCandidates(j, m) {
				if !pooledMemory(candidate) || !environmentReady(candidate, m) || q.capReasonFor(candidate, m) != "" || !fits(candidate, &capacity, capacity.FreeMemoryGB) {
					continue
				}
				if !fits(candidate, m, m.FreeMemoryGB) || !q.batchSlotFree(m, candidate) || !q.poolCustodyAllows(candidate, m) {
					byHost[host] = append(byHost[host], &hostBackfillDemand{job: candidate, machine: m, pooled: true})
				}
			}
		}
	}
	for host, choices := range byHost {
		set := &candidateSet{q: q, scores: map[string]float64{}, actives: map[string]float64{}}
		for _, c := range choices {
			set.jobs = append(set.jobs, c.job)
		}
		for _, c := range choices {
			old := scan.hostBackfill[host]
			if old == nil || q.beforeWithFair(c.job, old.job, c.machine, set.score, set.active) {
				c.order = set
				scan.hostBackfill[host] = c
			}
		}
	}
}

func (q *Queue) poolBackfillFits(j *Job, m *Machine, head *hostBackfillDemand) bool {
	pool := memoryPools(head.job).RAM
	if pool.Alone {
		return false
	}
	budget := q.poolAllowance(m) - pool.ChargeGB
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

func (q *Queue) memoryPoolHold(j *Job, machines map[string]*Machine) *Hold {
	var blocked *Hold
	for _, key := range slices.Sorted(maps.Keys(machines)) {
		m := machines[key]
		if !workerUsesMemoryPools(m) || !q.placementAllowed(j, m) {
			continue
		}
		capacity := q.qualificationCapacity(m)
		for _, c := range q.memoryCandidates(j, m) {
			if !pooledMemory(c) || !fitsResources(c, &capacity, capacity.TotalMemoryGB) {
				continue
			}
			pools := memoryPools(c)
			if fits(c, m, m.FreeMemoryGB) && q.poolCustodyAllows(c, m) && q.batchSlotFree(m, c) {
				return nil
			}
			ledger := q.hostMemoryLedgers[m.ID]
			if ledger != nil && (pools.RAM.ChargeGB > ledger.SignedRoomGB || !ledger.AvailableKnown) {
				available := "unknown"
				if ledger.AvailableKnown {
					available = fmt.Sprintf("%.3f GiB", ledger.AvailableGB)
				}
				blocked = newHold(memoryWaitReason, fmt.Sprintf("RAM needs %.3f GiB, available %s, keep %.3f GiB, outstanding reservations %.3f GiB", pools.RAM.ChargeGB, available, pools.KeepGB, ledger.GrowthGB))
			} else if !q.poolCustodyAllows(c, m) {
				blocked = newHold(memoryWaitReason, "waiting for exclusive access to the pool")
			} else if pools.Card != nil && m.FreeMemoryGB < c.Spec.Needs.MemoryGB {
				blocked = newHold(memoryWaitReason, fmt.Sprintf("card needs %.3f GiB, available %.3f GiB", c.Spec.Needs.MemoryGB, m.FreeMemoryGB))
			}
		}
	}
	return blocked
}

func (q *Queue) poolCustodyAllows(j *Job, m *Machine) bool {
	if !pooledMemory(j) {
		return true
	}
	pools := memoryPools(j)
	for _, obligation := range q.hostObligations(hostID(m)) {
		if obligation.Result.ID == j.ID {
			continue
		}
		if pools.RAM.Alone {
			return false
		}
		if peer := q.s.Jobs[obligation.Result.ID]; peer != nil && pooledMemory(peer) && memoryPools(peer).RAM.Alone {
			return false
		}
		if pools.Card != nil && pools.Card.Alone {
			if peer := q.s.Jobs[obligation.Result.ID]; peer != nil && batchOnTarget(peer, m) {
				return false
			}
			if e := obligation.Engine; e != nil && e.Machine == m.ID && (e.Device == "" || slices.Contains(targetDevices(m), e.Device)) {
				return false
			}
		}
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
		retainPoolPeaks(j, memoryValue(r.MemoryPeak.HostGB), memoryValue(r.MemoryPeak.DeviceGB))
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
			if allowance <= 0 || !workerUsesMemoryPools(m) || 2*lower <= allowance {
				q.requeue(j, memoryPressureCause, false)
				return
			}
			routes++
			largestLowerBound = max(largestLowerBound, lower)
		}
	}
	if routes == 0 {
		q.requeue(j, memoryPressureCause, false)
		return
	}
	j.State, j.DiagnosticCode = "failed", "memory_capacity"
	j.Reason = fmt.Sprintf("memory_capacity: %.3f GiB measured under memory pressure, and the retry charge exceeds every eligible RAM pool", largestLowerBound)
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
	cores := next.Spec.Needs.cores()
	for _, n := range preparationCores {
		cores += n
	}
	var pending float64
	for _, j := range jobs {
		if j.ReleaseConfirmed {
			continue
		}
		cores += j.Spec.Needs.cores()
		owner := allocationMachine(&w.Machine, j.AllocatedDevices)
		_, growth := memoryCharge(&j, owner, localJobHostNeed(w, j), w.hostMemorySamples[jobKey(j)], w.Clock.Now())
		pending += growth
		if pools.RAM.Alone || pooledMemory(&j) && memoryPools(&j).RAM.Alone {
			return checkMemoryPool(pools.RAM.TypicalGB, 0, 0, pending, false)
		}
	}
	for _, model := range models {
		if model.State == "ready" || model.State == "loading" || model.State == "draining" {
			cores++
			if budget, ok := w.localModelBudget(model); ok {
				pending += hostNeed(allocationMachine(&w.Machine, []string{model.Device}), budget.MemoryGB, budget.HostMemoryGB)
			}
			if pools.RAM.Alone {
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
	if err := w.checkJobRAMWithPending(context.Background(), next, pending); err != nil {
		return err
	}
	if pools.Card == nil {
		return nil
	}
	for _, id := range attemptCards(next, w.Machine) {
		var used float64
		for _, j := range jobs {
			if !j.ReleaseConfirmed && !j.Spec.Needs.cpuOnly() && slices.Contains(attemptCards(j, w.Machine), id) {
				if !sharesDevice(&next) || !sharesDevice(&j) {
					return checkMemoryPool(pools.Card.TypicalGB, 0, 0, 0, false)
				}
				used += max(deviceShareGB(&j), w.hostMemorySamples[jobKey(j)].devicePeakGB)
			}
		}
		for _, model := range models {
			if model.Device == id && (model.State == "ready" || model.State == "loading" || model.State == "draining") {
				if pools.Card.Alone {
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
		if err := checkMemoryPool(pools.Card.TypicalGB, max(0, min(card.BudgetGB-used, card.FreeMemoryGB)), 0, 0, card.MemoryMeasured); err != nil {
			return err
		}
	}
	return nil
}
