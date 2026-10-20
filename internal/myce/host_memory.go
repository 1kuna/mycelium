package myce

import (
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
			owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
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
			owner := slotMachine(q.s.Machines[s.Machine], s)
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
	// Count the ledger's remaining executions and started builds once. Candidate
	// sets only hand one core back when they remove a worker's last execution.
	executions, builds := map[string]int{}, map[string]int{}
	for _, p := range q.s.PreparationPlans {
		if owner := q.s.Machines[p.Machine]; p.Started && !(releasePreparation && p.Machine == m.ID) && owner != nil && hostID(owner) == hostID(m) {
			builds[p.Machine]++
		}
	}
	selected := map[string]int{}
	freedCPU := func(jobs []*Job, slots []*ModelSlot) int {
		clear(selected)
		freed := 0
		for _, j := range jobs {
			freed += j.Spec.Needs.cores()
			if builds[j.Machine] > 0 {
				selected[j.Machine]++
			}
		}
		for _, s := range slots {
			freed++
			if builds[s.Machine] > 0 {
				selected[s.Machine]++
			}
		}
		for worker, count := range selected {
			if count == executions[worker] {
				freed--
			}
		}
		return freed
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
	// Free RAM is clamped in the live ledger. Repay any hidden reservation
	// debt before crediting candidate releases (#104).
	room := m.FreeHostMemoryGB - m.hostReservationDebtGB
	need := hostNeed(m, gpu, host)
	if host == 0 {
		need = 0
	}
	var jobs []*Job
	var slots []*ModelSlot
	for _, j := range q.s.Jobs {
		owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
		if !releasedJobs[j] && !j.ReleaseConfirmed && owner != nil && hostID(owner) == hostID(m) && (j.State == "running" || j.State == "stopping") {
			executions[j.Machine]++
		}
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
		owner := slotMachine(q.s.Machines[s.Machine], s)
		if !releasedSlots[s] && slotReservesMemory(s) && owner != nil && hostID(owner) == hostID(m) {
			executions[s.Machine]++
		}
		if !releasedSlots[s] && slotReservesMemory(s) && owner != nil && !owner.Gone && hostID(owner) == hostID(m) && q.slotReclaimableFor(s, demand) {
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].InFlight != slots[j].InFlight {
			return slots[i].InFlight < slots[j].InFlight
		}
		return slotKey(slots[i].Machine, slots[i].Model, slots[i].Device) < slotKey(slots[j].Machine, slots[j].Model, slots[j].Device)
	})
	// Exclude victims that free neither RAM nor cores, including build hand-back.
	jobs = slices.DeleteFunc(jobs, func(j *Job) bool {
		return hostNeed(allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices), j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) == 0 && freedCPU([]*Job{j}, nil) == 0
	})
	slots = slices.DeleteFunc(slots, func(s *ModelSlot) bool {
		model, _ := q.model(s.Model)
		return hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB) == 0 && freedCPU(nil, []*ModelSlot{s}) == 0
	})
	// Reject impossible demands without constructing a partial release plan.
	allRAM := room
	for _, j := range jobs {
		allRAM += hostNeed(allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices), j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	}
	for _, s := range slots {
		model, _ := q.model(s.Model)
		allRAM += hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB)
	}
	if allRAM < need || !cpuOK(jobs, slots) {
		return nil, nil, false
	}
	// This single-victim check plus greedy/reverse prune is a heuristic, not
	// always optimal; #100 accepts this tradeoff to keep check-ins inexpensive.
	ramCapacity := m.HostAvailableMemoryGB
	if m.hostBudgetGB > 0 {
		ramCapacity = min(ramCapacity, m.hostBudgetGB)
	}
	ramDeficit := max(0, need-room)
	coreDeficit := max(0, m.CPUInUseCores+cores-m.EffectiveCPUCores)
	var bestJobs []*Job
	var bestSlots []*ModelSlot
	var bestTiers []int
	bestStops, bestExcess, bestID := 0, 0.0, ""
	consider := func(js []*Job, ss []*ModelSlot, spec Spec, stops int, ram float64, id string) {
		if room+ram < need || !cpuOK(js, ss) {
			return
		}
		tiers := make([]int, len(q.effectivePriorityPolicy().Tiers))
		tiers[q.priorityMatch(spec).Index] = stops
		excess := max(0, ram-ramDeficit)/max(1, ramCapacity) + float64(max(0, freedCPU(js, ss)-coreDeficit))/float64(max(1, m.EffectiveCPUCores))
		order := slices.Compare(tiers, bestTiers)
		if bestTiers == nil || order < 0 || order == 0 && (stops < bestStops || stops == bestStops && (excess < bestExcess || excess == bestExcess && id < bestID)) {
			bestJobs, bestSlots, bestTiers = js, ss, tiers
			bestStops, bestExcess, bestID = stops, excess, id
		}
	}
	for _, j := range jobs {
		stops := 0
		if j.State == "running" {
			stops = 1
		}
		consider([]*Job{j}, nil, j.Spec, stops, hostNeed(allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices), j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), "job/"+j.ID)
	}
	for _, s := range slots {
		stops := 0
		if s.State != "draining" && s.State != "unloading" {
			stops = 1
		}
		model, _ := q.model(s.Model)
		consider(nil, []*ModelSlot{s}, q.slotSpec(s), stops, hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB), "slot/"+slotKey(s.Machine, s.Model, s.Device))
	}
	if bestTiers != nil {
		return bestJobs, bestSlots, true
	}
	// Prefer releasing model residency before interrupting commands.
	chosenSlots := []*ModelSlot{}
	for _, s := range slots {
		if room >= need && cpuOK(nil, chosenSlots) {
			break
		}
		model, _ := q.model(s.Model)
		n := hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB)
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
		n := hostNeed(allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices), j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
		if room < need && n > 0 || !cpuOK(chosenJobs, chosenSlots) && !j.ReleaseConfirmed {
			room += n
			chosenJobs = append(chosenJobs, j)
		}
	}
	// A later RAM release can also cover cores freed by an earlier pick.
	// Prune in reverse preference order, retaining the cheaper victims first.
	for i := len(chosenJobs) - 1; i >= 0; i-- {
		j := chosenJobs[i]
		n := hostNeed(allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices), j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
		remaining := slices.Delete(slices.Clone(chosenJobs), i, i+1)
		if room-n >= need && cpuOK(remaining, chosenSlots) {
			room -= n
			chosenJobs = remaining
		}
	}
	for i := len(chosenSlots) - 1; i >= 0; i-- {
		s := chosenSlots[i]
		model, _ := q.model(s.Model)
		n := hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB)
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
		if slotOnTarget(s, m) && q.slotReclaimableFor(s, demand) {
			model, _ := q.model(s.Model)
			room += model.MemoryGB
		}
	}
	for _, j := range q.s.Jobs {
		if batchOnTarget(j, m) && (j.State == "running" || j.State == "stopping") && q.batchPreemptible(j, demand) {
			room += j.Spec.Needs.MemoryGB
		}
	}
	return room
}
