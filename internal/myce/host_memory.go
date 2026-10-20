package myce

import (
	"maps"
	"slices"
	"sort"
	"strings"
)

func hostID(m *Machine) string {
	if m.HostID != "" {
		return m.HostID
	}
	return m.ID
}

// Reserve at least 64 KiB for declared host memory: a sub-page cgroup limit
// needs one page. Use the largest supported worker page (4/64 KiB), rather
// than the queue host's page size, so enforced <= reserved across hosts.
const minimumHostMemoryGB = 65536.0 / (1 << 30)

// CPU allocations are additional to GPU allocations on unified machines.
// Unenforced laptop workers support explicit admission-only operation.
func hostNeed(m *Machine, gpu, host float64) float64 {
	need := host
	if m.UnifiedMemory && m.HostMemoryMeasured {
		need += gpu
	}
	if host > 0 {
		return max(need+m.MemoryCeilingSlackGB, minimumHostMemoryGB)
	}
	return need
}
func hostFits(m *Machine, gpu, host float64) bool {
	if m.HostMemoryPolicyMismatch {
		return false
	}
	if host == 0 {
		return !strings.HasPrefix(m.MemoryEnforcement, "memory ceilings enforced") && m.HostMemoryBudgetGB == 0 && m.hostBudgetGB == 0
	}
	return m.HostMemoryMeasured && m.HostMemoryError == "" && m.FreeHostMemoryGB >= hostNeed(m, gpu, host)
}
func (q *Queue) refreshHostMemory() {
	q.refreshHostCPU()
	for _, m := range q.s.Machines {
		available := m.HostAvailableMemoryGB
		for _, other := range q.s.Machines {
			if hostID(other) == hostID(m) && !other.Gone {
				if !other.HostMemoryMeasured || other.HostMemoryError != "" {
					available = 0
				} else {
					available = min(available, other.HostAvailableMemoryGB)
				}
			}
		}

		m.hostBudgetGB = 0
		m.HostMemoryPolicyMismatch = false
		for _, other := range q.s.Machines {
			if hostID(other) != hostID(m) {
				continue
			}
			if other.HostMemoryBudgetGB != m.HostMemoryBudgetGB {
				m.HostMemoryPolicyMismatch = true
			}
			if other.HostMemoryBudgetGB > 0 && (m.hostBudgetGB == 0 || other.HostMemoryBudgetGB < m.hostBudgetGB) {
				m.hostBudgetGB = other.HostMemoryBudgetGB
			}
		}
		if policy, ok := q.s.HostMaintenance[hostID(m)]; ok && policy.MemoryBudget != nil {
			m.hostBudgetGB = policy.MemoryBudget.GiB
			for _, other := range q.s.Machines {
				if hostID(other) == hostID(m) && other.HostMemoryBudgetGB != m.hostBudgetGB {
					m.HostMemoryPolicyMismatch = true
				}
			}
		}
		allowance, _ := hostMemoryAllowance(m.hostBudgetGB)
		m.HostMemoryAllowanceBytes = allowance.Bytes
		m.HostMemoryQualification = ""
		if allowance.Bytes > 0 {
			m.HostMemoryQualification = "UNQUALIFIED"
		}
		if m.hostBudgetGB > 0 {
			available = min(available, m.hostBudgetGB)
		}
		m.EffectiveHostMemoryBudgetGB = m.hostBudgetGB
		reserved := 0.0
		for _, j := range q.s.Jobs {
			owner := q.s.Machines[j.Machine]
			if !j.ReleaseConfirmed && owner != nil && hostID(owner) == hostID(m) && (j.State == "running" || j.State == "stopping") {
				reserved += hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
			}
		}
		for _, r := range q.s.HostReleases {
			if r.Host != hostID(m) {
				continue
			}
			j := q.s.Jobs[r.Job]
			if j != nil && j.Machine == r.Machine && j.Session == r.Session && j.Attempt == r.Attempt && (j.State == "running" || j.State == "stopping") {
				continue
			}
			reserved += r.HostChargeGB
		}
		for _, s := range q.s.ModelSlots {
			owner := q.s.Machines[s.Machine]
			if owner != nil && slotReservesMemory(s) && hostID(owner) == hostID(m) {
				model, _ := q.model(s.Model)
				reserved += hostNeed(owner, model.MemoryGB, model.HostMemoryGB)
			}
		}
		m.HostMemoryInUseGB = reserved
		m.hostReservationDebtGB = max(0, reserved-available)
		m.FreeHostMemoryGB = max(0, available-reserved)
	}
}

// Plan releases first. Never credit stopping/draining work as free until its
// acknowledgement arrives. GPU releases are local; RAM releases may be on any
// card on the same physical host. Pins and failed unloads remain protected.
func (q *Queue) hostReleases(m *Machine, gpu, host float64, priority string, identity ...Spec) ([]*Job, []*ModelSlot, bool) {
	return q.hostReleasesExcept(m, gpu, host, priorityDemand(priority, identity), nil, nil)
}
func (q *Queue) hostReleasesExcept(m *Machine, gpu, host float64, demand Spec, releasedJobs map[*Job]bool, releasedSlots map[*ModelSlot]bool, releasePreparations ...bool) ([]*Job, []*ModelSlot, bool) {
	if m.HostMemoryBudgetStatus() != "" {
		return nil, nil, false
	}
	if host == 0 && (strings.HasPrefix(m.MemoryEnforcement, "memory ceilings enforced") || m.HostMemoryBudgetGB > 0 || m.hostBudgetGB > 0) {
		return nil, nil, false
	}
	cores := demand.Needs.cores()
	releasePreparation := len(releasePreparations) > 0 && releasePreparations[0]
	initialCPU := q.hostCPUInUse(m, releasedJobs, releasedSlots, releasePreparation)
	freedCPU := func(jobs []*Job, slots []*ModelSlot) int {
		jobSet, slotSet := map[*Job]bool{}, map[*ModelSlot]bool{}
		maps.Copy(jobSet, releasedJobs)
		maps.Copy(slotSet, releasedSlots)
		for _, j := range jobs {
			jobSet[j] = true
		}
		for _, s := range slots {
			slotSet[s] = true
		}
		return initialCPU - q.hostCPUInUse(m, jobSet, slotSet, releasePreparation)
	}
	cpuOK := func(jobs []*Job, slots []*ModelSlot) bool {
		copy := *m
		copy.CPUInUseCores = max(0, m.CPUInUseCores-freedCPU(jobs, slots))
		copy.FreeCPUCores = max(0, copy.EffectiveCPUCores-copy.CPUInUseCores)
		return cores == 0 || cpuAllocationFits(&copy, cores)
	}
	if (host == 0 || hostFits(m, gpu, host)) && cpuOK(nil, nil) {
		return nil, nil, true
	}
	if host > 0 && (!m.HostMemoryMeasured || m.HostMemoryError != "") {
		return nil, nil, false
	}
	room := m.FreeHostMemoryGB
	need := hostNeed(m, gpu, host)
	if host == 0 {
		need = 0
	}
	var jobs []*Job
	var slots []*ModelSlot
	for _, j := range q.s.Jobs {
		owner := q.s.Machines[j.Machine]
		if releasedJobs[j] || j.ReleaseConfirmed || owner == nil || owner.Gone || hostID(owner) != hostID(m) || !q.batchPreemptible(j, demand) {
			continue
		}
		if j.State == "running" || j.State == "stopping" {
			jobs = append(jobs, j)
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if q.comparePriority(jobs[i].Spec, jobs[j].Spec) != 0 {
			return q.comparePriority(jobs[i].Spec, jobs[j].Spec) > 0
		}
		return jobs[i].ID < jobs[j].ID
	})
	for _, s := range q.s.ModelSlots {
		owner := q.s.Machines[s.Machine]
		if !releasedSlots[s] && slotReservesMemory(s) && owner != nil && !owner.Gone && hostID(owner) == hostID(m) && q.slotReclaimableFor(s, demand) {
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].InFlight != slots[j].InFlight {
			return slots[i].InFlight < slots[j].InFlight
		}
		return slotKey(slots[i].Machine, slots[i].Model) < slotKey(slots[j].Machine, slots[j].Model)
	})
	// Even releasing every eligible victim must fit. In particular, do not
	// search subsets for a core demand larger than the physical host allowance.
	if !cpuOK(jobs, slots) {
		return nil, nil, false
	}
	allRAM := room
	for _, j := range jobs {
		allRAM += hostNeed(q.s.Machines[j.Machine], j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	}
	for _, s := range slots {
		model, _ := q.model(s.Model)
		allRAM += hostNeed(q.s.Machines[s.Machine], model.MemoryGB, model.HostMemoryGB)
	}
	if allRAM < need {
		return nil, nil, false
	}
	if len(jobs)+len(slots) <= 12 {
		// Search only the existing eligible victims. Above the bound, retain
		// the greedy-plus-prune path below to bound scheduling work per host.
		type symmetryKey struct {
			tier, stops, cores, inFlight int
			ram                          float64
			slot, resumable, retry       bool
			state, preparationWorker     string
		}
		type victim struct {
			job   *Job
			slot  *ModelSlot
			tier  int
			stops int
			ram   float64
			cores int
			id    string
			key   symmetryKey
		}
		preparationWorkers := map[string]bool{}
		for _, p := range q.s.PreparationPlans {
			if owner := q.s.Machines[p.Machine]; p.Started && !(releasePreparation && p.Machine == m.ID) && owner != nil && hostID(owner) == hostID(m) {
				preparationWorkers[p.Machine] = true
			}
		}
		victims := make([]victim, 0, len(jobs)+len(slots))
		for _, j := range jobs {
			v := victim{job: j, tier: q.priorityMatch(j.Spec).Index, ram: hostNeed(q.s.Machines[j.Machine], j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), id: "job/" + j.ID}
			if j.State == "running" {
				v.stops = 1
			}
			v.cores = j.Spec.Needs.cores()
			v.key = symmetryKey{tier: v.tier, stops: v.stops, cores: v.cores, ram: v.ram, state: j.State, resumable: j.Spec.Resumable}
			if preparationWorkers[j.Machine] {
				v.key.preparationWorker = j.Machine
			}
			victims = append(victims, v)
		}
		for _, s := range slots {
			model, _ := q.model(s.Model)
			v := victim{slot: s, tier: q.priorityMatch(q.slotSpec(s)).Index, ram: hostNeed(q.s.Machines[s.Machine], model.MemoryGB, model.HostMemoryGB), id: "slot/" + slotKey(s.Machine, s.Model)}
			if s.State != "draining" && s.State != "unloading" {
				v.stops = 1
			}
			if slotReservesMemory(s) {
				v.cores = 1
			}
			v.key = symmetryKey{tier: v.tier, stops: v.stops, cores: v.cores, ram: v.ram, state: s.State, slot: true, inFlight: s.InFlight, retry: s.RetryReserved}
			if preparationWorkers[s.Machine] {
				v.key.preparationWorker = s.Machine
			}
			victims = append(victims, v)
		}
		// Stable addition order also keeps floating-point RAM sums deterministic.
		sort.Slice(victims, func(i, j int) bool { return victims[i].id < victims[j].id })
		// Interchangeable victims need only their ID-ordered prefixes. Keeping
		// the preceding equal victim selected satisfies the ID tie-break without
		// visiting every tied subset. Build-dependent releases stay worker-local.
		previousEqual := make([]int, len(victims))
		selected := make([]bool, len(victims))
		for i, v := range victims {
			previousEqual[i] = -1
			for k := i - 1; k >= 0; k-- {
				if victims[k].key == v.key {
					previousEqual[i] = k
					break
				}
			}
		}
		remainingRAM := make([]float64, len(victims)+1)
		remainingCores := make([]int, len(victims)+1)
		for i := len(victims) - 1; i >= 0; i-- {
			remainingRAM[i] = remainingRAM[i+1] + victims[i].ram
			remainingCores[i] = remainingCores[i+1] + victims[i].cores
		}
		tiers := make([]int, len(q.effectivePriorityPolicy().Tiers))
		var bestTiers []int
		var chosenJobs, bestJobs []*Job
		var chosenSlots, bestSlots []*ModelSlot
		var ids, bestIDs []string
		stops, bestStops, bestExcess := 0, 0, 0.0
		ramDeficit := max(0, need-room)
		coreDeficit := max(0, m.CPUInUseCores+cores-m.EffectiveCPUCores)
		requiredCoreReleases := coreDeficit
		if !m.cpuLedger {
			// Legacy non-ledger capacity is independent of released executions;
			// cpuOK above has already checked its capability report.
			requiredCoreReleases = 0
		}
		ramCapacity := m.HostAvailableMemoryGB
		if m.hostBudgetGB > 0 {
			ramCapacity = min(ramCapacity, m.hostBudgetGB)
		}
		var search func(int, float64, int)
		search = func(i int, freedRAM float64, freedCores int) {
			if len(preparationWorkers) > 0 {
				freedCores = freedCPU(chosenJobs, chosenSlots)
			}
			if room+freedRAM+remainingRAM[i] < need || freedCores+remainingCores[i] < requiredCoreReleases {
				return
			}
			// Adding victims cannot improve the tier counts or number of stops.
			order := slices.Compare(tiers, bestTiers)
			if bestTiers != nil && (order > 0 || order == 0 && stops > bestStops) {
				return
			}
			// Unused capacity is also monotonic as the selected set grows.
			excess := max(0, freedRAM-ramDeficit)/max(1, ramCapacity) + float64(max(0, freedCores-coreDeficit))/float64(max(1, m.EffectiveCPUCores))
			if bestTiers != nil && order == 0 && stops == bestStops && excess > bestExcess {
				return
			}
			if i == len(victims) {
				if room+freedRAM < need || freedCores < requiredCoreReleases {
					return
				}
				// Sum unused fractions of host RAM and cores, so neither unit
				// (GiB versus cores) implicitly determines the capacity tie-break.
				if bestTiers == nil || order < 0 || order == 0 && (stops < bestStops || stops == bestStops && (excess < bestExcess || excess == bestExcess && slices.Compare(ids, bestIDs) < 0)) {
					bestTiers = slices.Clone(tiers)
					bestStops, bestExcess = stops, excess
					bestJobs, bestSlots, bestIDs = slices.Clone(chosenJobs), slices.Clone(chosenSlots), slices.Clone(ids)
				}
				return
			}
			search(i+1, freedRAM, freedCores)
			if previousEqual[i] >= 0 && !selected[previousEqual[i]] {
				return
			}
			v := victims[i]
			selected[i] = true
			tiers[v.tier] += v.stops
			stops += v.stops
			ids = append(ids, v.id)
			if v.job != nil {
				chosenJobs = append(chosenJobs, v.job)
			} else {
				chosenSlots = append(chosenSlots, v.slot)
			}
			search(i+1, freedRAM+v.ram, freedCores+v.cores)
			if v.job != nil {
				chosenJobs = chosenJobs[:len(chosenJobs)-1]
			} else {
				chosenSlots = chosenSlots[:len(chosenSlots)-1]
			}
			ids = ids[:len(ids)-1]
			tiers[v.tier] -= v.stops
			stops -= v.stops
			selected[i] = false
		}
		search(0, 0, 0)
		return bestJobs, bestSlots, bestTiers != nil
	}
	// Prefer releasing model residency before interrupting commands.
	chosenSlots := []*ModelSlot{}
	for _, s := range slots {
		if room >= need && cpuOK(nil, chosenSlots) {
			break
		}
		model, _ := q.model(s.Model)
		n := hostNeed(q.s.Machines[s.Machine], model.MemoryGB, model.HostMemoryGB)
		if room < need && n > 0 || !cpuOK(nil, chosenSlots) && slotReservesMemory(s) {
			room += n
			chosenSlots = append(chosenSlots, s)
		}
	}
	chosenJobs := []*Job{}
	for _, j := range jobs {
		if room >= need && cpuOK(chosenJobs, chosenSlots) {
			break
		}
		n := hostNeed(q.s.Machines[j.Machine], j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
		if room < need && n > 0 || !cpuOK(chosenJobs, chosenSlots) && !j.ReleaseConfirmed {
			room += n
			chosenJobs = append(chosenJobs, j)
		}
	}
	// A later RAM release can also cover cores freed by an earlier pick.
	// Prune in reverse preference order, retaining the cheaper victims first.
	for i := len(chosenJobs) - 1; i >= 0; i-- {
		j := chosenJobs[i]
		n := hostNeed(q.s.Machines[j.Machine], j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
		remaining := slices.Delete(slices.Clone(chosenJobs), i, i+1)
		if room-n >= need && cpuOK(remaining, chosenSlots) {
			room -= n
			chosenJobs = remaining
		}
	}
	for i := len(chosenSlots) - 1; i >= 0; i-- {
		s := chosenSlots[i]
		model, _ := q.model(s.Model)
		n := hostNeed(q.s.Machines[s.Machine], model.MemoryGB, model.HostMemoryGB)
		remaining := slices.Delete(slices.Clone(chosenSlots), i, i+1)
		if room-n >= need && cpuOK(chosenJobs, remaining) {
			room -= n
			chosenSlots = remaining
		}
	}
	return chosenJobs, chosenSlots, room >= need && cpuOK(chosenJobs, chosenSlots)
}
func (q *Queue) reclaimableGPUMemory(m *Machine, priority string, identity ...Spec) float64 {
	demand := priorityDemand(priority, identity)
	room := m.FreeMemoryGB
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && q.slotReclaimableFor(s, demand) {
			model, _ := q.model(s.Model)
			room += model.MemoryGB
		}
	}
	for _, j := range q.s.Jobs {
		if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") && q.batchPreemptible(j, demand) {
			room += j.Spec.Needs.MemoryGB
		}
	}
	return room
}
