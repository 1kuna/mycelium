package myce

import "time"

// One host memory ledger. Capacity is the host's live physical availability
// minus one margin; each owner is charged jobHostMemoryCharge. Room is that
// capacity less the owners' outstanding growth, capped by the host's one
// allowance less their charges, because workers enforce that allowance at
// launch and as the parent memory ceiling. Every protection is a margin on
// the same physical reading, applied once: never a second subtraction.

// resolveHostAllowance picks the host's one allowance source: the queue's host
// policy (measured from idle history, else its authored fallback), else the
// workers' own flags. Workers enforce it once they acknowledge it.
func (q *Queue) resolveHostAllowance(m *Machine) {
	m.hostBudgetGB = 0
	m.HostMemoryDerivation = nil
	m.HostMemoryPolicyMismatch = false
	for _, other := range q.s.Machines {
		if hostID(other) != hostID(m) {
			continue
		}
		if policy := q.s.HostMaintenance[hostID(m)].MemoryBudget; policy != nil && policy.Measured && (other.Gone || other.Replacement != nil) {
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
		budget, derivation := q.measuredHostMemoryBudget(m, *policy.MemoryBudget)
		m.hostBudgetGB, m.HostMemoryDerivation = budget.GiB, derivation
		for _, other := range q.s.Machines {
			if policy.MemoryBudget.Measured && (other.Gone || other.Replacement != nil) {
				continue
			}
			if hostID(other) == hostID(m) && (other.HostMemoryBudgetGB != m.hostBudgetGB || other.HostMemoryBudgetMeasured != policy.MemoryBudget.Measured) {
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
	m.EffectiveHostMemoryBudgetGB = m.hostBudgetGB
}

// hostMemoryMargin is the one RAM margin kept free on worker's physical
// reading for target m: the largest named protection, never stacked. The
// worker's explicit reserve; the measured idle burst (MY-45, which already
// takes the explicit reserve as its floor); a unified CUDA worker's physical
// floor (its GPU reserve, or physical RAM minus an authored or interactive
// allowance, which cgroups cannot enforce for device allocations); and the
// interactive host's owner reserve.
func (q *Queue) hostMemoryMargin(m, worker *Machine) float64 {
	margin := 0.0
	if o := worker.MemoryObservation; o != nil {
		margin = o.ReserveGB
	}
	if d := m.HostMemoryDerivation; d != nil && d.Source == "quiescent_v1" {
		margin = max(margin, d.ReserveGB)
	}
	if floor, ok := unifiedCUDAFloor(worker); ok && hostHasUnifiedCUDA(m) {
		margin = max(margin, floor)
	}
	if p := q.s.HostMaintenance[hostID(m)].SharedHost; p != nil {
		margin = max(margin, p.InteractiveReserveGB)
	}
	return margin
}

// hostLiveCapacity reads the host's live RAM for target m. Capacity is the
// smallest worker reading, each less its margin; available is the smallest
// admission reading as reported. Unknown readings count as zero.
func (q *Queue) hostLiveCapacity(m *Machine) (capacity, available float64, known bool, observedAt time.Time) {
	capacity, available, known, observedAt = m.HostAvailableMemoryGB, m.HostAvailableMemoryGB, true, m.LastSeen
	for _, w := range q.s.Machines {
		if hostID(w) != hostID(m) || w.Gone && w != m {
			continue
		}
		reported := w.HostAvailableMemoryGB
		if !w.HostMemoryMeasured || w.HostMemoryError != "" {
			reported, known = 0, false
		}
		available, observedAt = min(available, reported), minTime(observedAt, w.LastSeen)
		capacity = min(capacity, q.workerLiveMemory(m, w))
	}
	return capacity, available, known, observedAt
}

// workerLiveMemory is one worker's live reading less its margin. Admission
// availability already has the explicit reserve removed; the raw reading
// carries the rest of the margin and any debt below zero. Unified CUDA
// allocations are visible only in the physical reading, so a unified host
// also needs that reading fresh from a live worker session.
func (q *Queue) workerLiveMemory(m, w *Machine) float64 {
	live := w.HostAvailableMemoryGB
	if !w.HostMemoryMeasured || w.HostMemoryError != "" {
		live = 0
	}
	o := w.MemoryObservation
	if validHostMemoryObservation(o) {
		live = min(live, o.TotalGB-o.UsedGB-q.hostMemoryMargin(m, w))
	}
	if hostHasUnifiedCUDA(m) {
		now := q.clock.Now()
		if !validHostMemoryObservation(o) || w.CapabilityProfile == nil || w.CapabilityProfile.SystemMemoryBytes == 0 || !q.workerFresh(w, now) || !q.hostHealthEvidenceValid(w, now) {
			live = min(live, 0)
		}
	}
	return live
}

// hostMemoryRoom is the one admission formula: live capacity less outstanding
// growth, within the enforced allowance less charges, then the interactive
// host's structural checks and measured owner peak.
func (q *Queue) hostMemoryRoom(m *Machine, capacity, reserved, growth float64) float64 {
	room := capacity - growth
	if m.hostBudgetGB > 0 || m.HostMemoryDerivation != nil {
		room = min(room, m.hostBudgetGB-reserved)
	}
	return q.sharedHostRoom(m, room, reserved)
}
