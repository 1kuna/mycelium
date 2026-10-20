package myce

import (
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
func (q *Queue) hostReleasesExcept(m *Machine, gpu, host float64, demand Spec, releasedJobs map[*Job]bool, releasedSlots map[*ModelSlot]bool) ([]*Job, []*ModelSlot, bool) {
	if m.HostMemoryBudgetStatus() != "" {
		return nil, nil, false
	}
	if host == 0 && (strings.HasPrefix(m.MemoryEnforcement, "memory ceilings enforced") || m.HostMemoryBudgetGB > 0 || m.hostBudgetGB > 0) {
		return nil, nil, false
	}
	cores := demand.Needs.cores()
	cpuOK := func(jobs []*Job, slots []*ModelSlot) bool {
		copy := cpuAfterReleases(m, jobs, slots, 0)
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
		if releasedJobs[j] || owner == nil || owner.Gone || hostID(owner) != hostID(m) || !q.batchPreemptible(j, demand) {
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
		if !releasedSlots[s] && owner != nil && !owner.Gone && hostID(owner) == hostID(m) && q.slotReclaimableFor(s, demand) {
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].InFlight != slots[j].InFlight {
			return slots[i].InFlight < slots[j].InFlight
		}
		return slotKey(slots[i].Machine, slots[i].Model) < slotKey(slots[j].Machine, slots[j].Model)
	})
	// Prefer releasing model residency before interrupting commands.
	chosenSlots := []*ModelSlot{}
	for _, s := range slots {
		if room >= need && cpuOK(nil, chosenSlots) {
			break
		}
		model, _ := q.model(s.Model)
		n := hostNeed(q.s.Machines[s.Machine], model.MemoryGB, model.HostMemoryGB)
		if n > 0 || cores > 0 && slotReservesMemory(s) {
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
		if n > 0 || cores > 0 && !j.ReleaseConfirmed {
			room += n
			chosenJobs = append(chosenJobs, j)
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
