package myce

import (
	"time"
)

// resolveHostAllowance picks the host's one allowance: the queue's host policy
// (measured from idle history, else its authored fallback), lowered to any live
// worker's own acknowledged allowance. Workers adopt the policy from every
// check-in reply and enforce what they acknowledged, so until they acknowledge
// a change the queue admits within the lower value: a fall applies at once, a
// rise after acknowledgement, and admission never pauses. A pending
// acknowledgement of the authored value or of the measured mode (a measured
// value moves with history) is HostMemoryPolicyMismatch: host readiness only.
func (q *Queue) resolveHostAllowance(m *Machine) {
	m.EffectiveHostMemoryBudgetGB, m.HostMemoryDerivation, m.HostMemoryPolicyMismatch = 0, nil, false
	policy := q.s.HostMaintenance[hostID(m)].MemoryBudget
	if policy != nil {
		budget, derivation := q.measuredHostMemoryBudget(m, *policy)
		m.EffectiveHostMemoryBudgetGB, m.HostMemoryDerivation = budget.GiB, derivation
	}
	for _, w := range q.s.Machines {
		if hostID(w) != hostID(m) || w.Gone || w.Replacement != nil {
			continue
		}
		if policy != nil && (w.HostMemoryBudgetMeasured != policy.Measured || !policy.Measured && w.HostMemoryBudgetGB != policy.GiB) {
			m.HostMemoryPolicyMismatch = true
		}
		if (w.HostMemoryBudgetGB > 0 || w.HostMemoryBudgetMeasured) && (m.EffectiveHostMemoryBudgetGB <= 0 && m.HostMemoryDerivation == nil || w.HostMemoryBudgetGB < m.EffectiveHostMemoryBudgetGB) {
			m.EffectiveHostMemoryBudgetGB = w.HostMemoryBudgetGB
		}
	}
	allowance, _ := hostMemoryAllowance(m.EffectiveHostMemoryBudgetGB)
	m.HostMemoryAllowanceBytes = allowance.Bytes
	m.HostMemoryQualification = ""
	if allowance.Bytes > 0 {
		m.HostMemoryQualification = "UNQUALIFIED"
	}
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
	live, _ := q.workerLiveMemoryReading(m, w)
	return live
}

// Unknown capacity is zero room for admission, but is not measured pressure
// that authorizes interrupting an execution.
func (q *Queue) workerLiveMemoryReading(m, w *Machine) (live float64, known bool) {
	live, known = w.HostAvailableMemoryGB, w.HostMemoryMeasured && w.HostMemoryError == ""
	if !known {
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
			known = false
		}
	}
	return live, known
}

func known(gb float64) float64 {
	if validMemoryValue(gb) {
		return gb
	}
	return 0
}

func (s hostMemorySample) freshAt(now time.Time) bool {
	_, fresh := s.hostGrowth(now)
	return fresh
}

// Room.

// hostMemoryRoom is the rule itself for target m: live capacity (less its one
// margin) less outstanding growth, within the enforced arm, which is the
// allowance workers enforce at launch less every charge, lowered to the
// interactive host's measured owner cap.

func (q *Queue) hostMemoryRoom(m *Machine, capacity, reserved, growth float64) float64 {
	if allowance := q.poolAllowance(m); allowance > 0 {
		capacity = min(capacity, allowance-max(0, reserved-growth))
	}
	return q.sharedHostRoom(m, capacity-growth, reserved)
}

// hostMemoryTerms is target m's rule as status reports it: capacity is room
// plus charges, so the three terms always add up. Nil when the live reading
// is unknown.
func (q *Queue) hostMemoryTerms(m *Machine) *HostMemoryTerms {
	l := q.hostMemoryLedgers[m.ID]
	if l == nil || !m.HostMemoryMeasured || m.HostMemoryError != "" {
		return nil
	}
	return &HostMemoryTerms{CapacityGB: l.SignedRoomGB + l.ReservedGB, ChargedGB: l.ReservedGB, RoomGB: l.SignedRoomGB}
}

// hostRoomAfter is target m's signed room once jobs and slots release.
func (q *Queue) hostRoomAfter(m *Machine, jobs []*Job, slots []*ModelSlot) float64 {
	room, released := m.FreeHostMemoryGB-m.hostReservationDebtGB, map[any]bool{}
	for _, j := range jobs {
		if !released[j] {
			released[j], room = true, room+q.jobRelease(m, j)
		}
	}
	for _, s := range slots {
		if !released[s] {
			released[s], room = true, room+q.slotRelease(m, s)
		}
	}
	return room
}

func (q *Queue) jobRelease(m *Machine, j *Job) float64 {
	owner := q.planningAllocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if owner == nil || hostID(owner) != hostID(m) || !batchReservesCapacity(j) {
		return 0
	}
	return q.hostJobCharge(j)
}

func (q *Queue) slotRelease(m *Machine, s *ModelSlot) float64 {
	owner := slotMachine(q.s.Machines[s.Machine], s)
	if owner == nil || hostID(owner) != hostID(m) || !slotReservesMemory(s) {
		return 0
	}
	model, _ := q.model(s.Model)
	return hostNeed(owner, model.MemoryGB, model.HostMemoryGB)
}

// afterReleases projects target m once jobs and slots release: CPU and RAM.
func (q *Queue) afterReleases(m *Machine, jobs []*Job, slots []*ModelSlot) Machine {
	projected := q.cpuAfterReleases(m, jobs, slots, 0)
	room := q.hostRoomAfter(m, jobs, slots)
	projected.FreeHostMemoryGB, projected.hostReservationDebtGB = max(0, room), max(0, -room)
	return projected
}
