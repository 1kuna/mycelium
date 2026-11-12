package myce

import (
	"math"
	"time"
)

// The one memory admission rule (#990): room(host) = capacity − Σ charge.
//
// Capacity is the host's live reading less one margin, lowered to the
// allowance workers enforce. A charge is measured use plus forecast growth,
// bounded by the enforced ceiling, or the declared or learned envelope until
// use is measured. Live availability already lacks resident memory, so each
// owner's resident part is credited once. Room is the smaller of two arms:
//
//	physical: live − margin − Σ(charge − credit)
//	enforced: allowance − Σ charge, and the interactive host's history cap
//
// A release returns its charge to both arms, and to the physical arm also any
// use that earned no credit (unified CUDA device memory, #424). Placement,
// release planning, exploratory sizing, fairness and backfill all read
// hostRoomAfter.

// Capacity.

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

// Charge.

// memoryCharge is the charge of one live attempt on target m whose enforced
// envelope (declared or assigned sizes plus slack) is envelope. growth is the
// part not yet resident, which live availability does not already exclude.
func memoryCharge(j *Job, m *Machine, envelope float64, s hostMemorySample, now time.Time) (charge, growth float64) {
	if j == nil || m == nil {
		return envelope, envelope
	}
	drift := exploratoryDriftMarginGB(m)
	fresh := s.freshAt(now)
	device := unifiedDeviceWork(j, m)
	floor, lend := lendingFloor(j, m, s, now)
	retained, observed := retainedCharge(j, s, now)
	// Actual high water is real occupancy: never discarded or bounded.
	peak := max(known(s.peakGB), known(s.gb), known(s.footprintGB), observed)
	forecast := sampleForecast(s, now)
	if lend == lendByUse && device {
		// Device allocations escape the cgroup but are measured per process.
		forecast = max(forecast, learned(j).PeakHostGB, peak) + max(learned(j).PeakDeviceGB, s.devicePeakGB, s.deviceGB, deviceForecast(s, now))
		peak += max(s.devicePeakGB, s.deviceGB)
	}
	rss := lend == lendByUse && m.MemoryEnforcement == darwinRSSGuardStatus
	overshoot := 0.0
	if rss {
		// RSS can pass the limit before the sampled guard stops it.
		overshoot = s.rateGB * darwinGuardReaction.Seconds()
	}
	margin := drift + overshoot
	adaptive := adaptiveMemoryCeilingGB(j, m)
	ceiling := enforcedCeiling(j, m, envelope, adaptive, lend == lendByUse) + overshoot
	// Measured use carries the snapshot-to-launch margin wherever the charge
	// can sit below what the attempt reaches: lent memory and adaptive
	// estimates, whose execution ceiling is wider. A declared envelope and a
	// probe floor are not measurements and carry none.
	measured := max(peak, forecast)
	if measured > 0 && (lend != lendNone || adaptive > 0) {
		measured += margin
	}
	if lend == lendByUse {
		floor += margin
	}
	if lend != lendNone {
		charge = max(peak, min(max(floor, measured), ceiling))
	} else {
		charge = max(envelope, retained, peak, min(measured, ceiling))
	}
	credit := 0.0
	if fresh {
		credit = s.gb
		if rss {
			// Darwin RSS has no resident field, but this charge is built from
			// the same RSS, so it is credited once.
			credit = s.footprintGB
		}
	}
	growth = max(0, charge-credit)
	if lend == lendByUse {
		growth = max(growth, min(margin, charge))
	} else if device && lend == lendNone {
		// Without a measured device share, the declared device size is still
		// to come: no device overlap with the cgroup sample is proved (#424).
		growth = max(growth, j.Spec.Needs.MemoryGB)
	}
	return charge, growth
}

func learned(j *Job) *LearnedMemory { return j.MemoryEnvelope.Learned }

// known is a reading, or zero when it is invalid: never a poisoned charge.
func known(gb float64) float64 {
	if validMemoryValue(gb) {
		return gb
	}
	return 0
}

// sampleForecast is the attempt's forecast use: through the sample's age while
// it is fresh. A stale sample keeps its measured increment but stops
// extrapolating its rate, and an accepted forecast survives missing reports.
func sampleForecast(s hostMemorySample, now time.Time) float64 {
	forecast := s.retainedGB
	if s.freshAt(now) {
		forecast = max(forecast, s.hostForecast(now))
	} else if !s.at.IsZero() && !now.Before(s.at) && validMemoryValue(s.gb) {
		forecast = max(forecast, s.hostForecast(s.at))
	}
	return forecast
}

type lending int

const (
	lendNone  lending = iota
	lendByUse         // Guarded work with accepted history (#1044, MY-34).
	lendProbe         // A cold probe with complete Started evidence.
)

// lendingFloor says whether an attempt may be charged below its envelope, and
// the least it is charged then. Charging by use lends the rest to neighbours,
// which is safe only where all of these hold, each for a failure it prevents:
//   - the attempt is started, on a host other than the shared
//     interactive one (MY-45 keeps its envelope);
//   - a guard contains it at its ceiling: Linux cgroup v2, the Darwin RSS
//     guard, or the Metal footprint guard; unified CUDA device memory escapes
//     the cgroup but is measured per process (#1044);
//   - its host sample is fresh and complete, so growth is visible;
//   - history says what it reaches (MY-34): the accepted peak of this work on
//     this target.
//
// yieldMemoryOvercommit stops a lender before the kernel would. A cold probe
// lends its unused offer once complete evidence arrives, down to its floor.
func lendingFloor(j *Job, m *Machine, s hostMemorySample, now time.Time) (float64, lending) {
	if floor, ok := byUseFloor(j, m, s, now); ok {
		return floor, lendByUse
	}
	e, n := j.MemoryEnvelope, j.Spec.Needs
	if e == nil || e.ProbeFloorGB <= 0 || n.Memory != "auto" || !n.cpuOnly() || j.State != "running" || !s.growthObserved || !s.freshAt(now) || !validMemoryValue(s.gb) || !validMemoryValue(s.growthGB) {
		return 0, lendNone
	}
	return max(e.ProbeFloorGB, retainedHostCharge(j)), lendProbe
}

// byUseFloor is the by-use lending check and its floor: the accepted peaks.
// Unified CUDA work needs one card's fresh per-process device counter (RSS
// and host-delta fallbacks are estimates); Metal work needs the footprint
// guard and footprint samples; CPU work a declared size under a guard.
func byUseFloor(j *Job, m *Machine, s hostMemorySample, now time.Time) (float64, bool) {
	if j == nil || m == nil || j.StartedAt == nil || j.State != "running" && j.State != "stopping" {
		return 0, false
	}
	n, e := j.Spec.Needs, j.MemoryEnvelope
	if m.SharedHost != nil || e == nil || e.Learned == nil || e.Learned.Evidence < 1 || !s.freshAt(now) || !validMemoryValue(s.footprintGB) || !validMemoryValue(s.rateGB) {
		return 0, false
	}
	l, cgroup := e.Learned, m.MemoryEnforcement == "memory ceilings enforced (cgroup v2)"
	peaks := l.PeakDeviceGB > 0 && validMemoryValue(l.PeakDeviceGB) && validMemoryValue(l.PeakHostGB) && len(j.AllocatedDevices) <= 1 && m.HostMemoryMeasured && n.HostMemoryGB > 0
	switch {
	case unifiedDeviceWork(j, m):
		device := s.source == "" && sharedDeviceSampleReady(s, now) && validMemoryValue(s.deviceGB) && validMemoryValue(s.deviceGrowthGB) && validMemoryValue(s.deviceRateGB)
		return 0, cgroup && peaks && n.MemoryGB > 0 && device // Both peaks are in the forecast.
	case metalFootprintWork(j, m):
		return l.PeakHostGB + l.PeakDeviceGB, m.MemoryEnforcement == darwinRSSGuardStatus && m.UnifiedMemory && peaks && n.Memory != "auto" && s.footprintGB > 0
	}
	guarded := cgroup || m.MemoryEnforcement == darwinRSSGuardStatus
	return hostNeed(m, 0, l.HostGB), guarded && n.cpuOnly() && n.Memory != "auto" && n.HostMemoryGB > 0 && validMemoryValue(l.HostGB) && l.HostGB > 0 && max(s.gb, s.footprintGB) > 0
}

// enforcedCeiling bounds a forecast: the ceiling that contains the attempt
// where one does (adaptive guard, cgroup limit, or the by-use guard),
// else inventoried physical RAM, never the operator's smaller allowance.
func enforcedCeiling(j *Job, m *Machine, envelope, adaptive float64, guarded bool) float64 {
	if adaptive > 0 {
		return max(envelope, adaptive)
	}
	if guarded || envelope > 0 && j.Spec.Needs.cpuOnly() && m.MemoryEnforcement == "memory ceilings enforced (cgroup v2)" {
		return envelope
	}
	if p := m.CapabilityProfile; p != nil && p.SystemMemoryBytes > 0 {
		return max(envelope, float64(p.SystemMemoryBytes)/(1<<30))
	}
	return math.Inf(1)
}

// retainedCharge is this exact attempt's durable charge and observed high
// water, which survive a queue restart. A genuinely newer identified
// observation replaces the charge's forecast part, never the high water; an
// opaque pre-provenance charge stays whole.
func retainedCharge(j *Job, s hostMemorySample, now time.Time) (charge, observed float64) {
	charge = retainedHostCharge(j)
	if c := j.HostMemoryCharge; charge > 0 && c != nil && c.ObservedGB != nil {
		if _, fresh := s.identifiedHostForecast(now); fresh && s.id > c.SampleID {
			return *c.ObservedGB, *c.ObservedGB
		}
		return charge, *c.ObservedGB
	}
	return charge, 0
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
func (q *Queue) hostMemoryRoom(m *Machine, capacity, reserved, growth float64) (room, enforced float64) {
	enforced = math.Inf(1)
	if m.EffectiveHostMemoryBudgetGB > 0 || m.HostMemoryDerivation != nil {
		enforced = m.EffectiveHostMemoryBudgetGB - reserved
	}
	enforced = min(enforced, q.sharedHostRoom(m, math.Inf(1), reserved))
	return min(capacity-growth, enforced), enforced
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
	room, released := q.hostRoomArms(m), map[any]bool{}
	for _, j := range jobs {
		if !released[j] {
			released[j], room = true, room.plus(q.jobRelease(m, j))
		}
	}
	for _, s := range slots {
		if !released[s] {
			released[s], room = true, room.plus(q.slotRelease(m, s))
		}
	}
	return room.gb()
}

// roomArms is a signed room as its physical and enforced arms; a release
// returns a roomArms to each.
type roomArms struct{ live, enforced float64 }

func (r roomArms) gb() float64 { return min(r.live, r.enforced) }
func (r roomArms) plus(o roomArms) roomArms {
	return roomArms{r.live + o.live, r.enforced + o.enforced}
}

// hostRoomArms is target m's room as arms. A machine whose room a caller has
// already projected has no arms: its projected room is the physical arm.
func (q *Queue) hostRoomArms(m *Machine) roomArms {
	room := roomArms{m.FreeHostMemoryGB - m.hostReservationDebtGB, math.Inf(1)}
	if l := q.hostMemoryLedgers[m.ID]; l != nil && math.Abs(l.SignedRoomGB-room.live) < 1e-9 {
		room = roomArms{l.CapacityGB - l.GrowthGB, l.enforcedGB}
	}
	return room
}

// jobRelease is what releasing a ledger attempt on m's host returns: its
// charge to both arms, plus uncredited use to the physical arm.
func (q *Queue) jobRelease(m *Machine, j *Job) roomArms {
	owner := q.planningAllocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if owner == nil || hostID(owner) != hostID(m) || !batchReservesCapacity(j) {
		return roomArms{}
	}
	charge := q.hostJobCharge(j)
	return roomArms{charge + uncreditedGB(j, owner, q.hostMemorySamples[j.Machine][jobKey(*j)], q.clock.Now()), charge}
}

func (q *Queue) slotRelease(m *Machine, s *ModelSlot) roomArms {
	owner := slotMachine(q.s.Machines[s.Machine], s)
	if owner == nil || hostID(owner) != hostID(m) || !slotReservesMemory(s) {
		return roomArms{}
	}
	model, _ := q.model(s.Model)
	need := hostNeed(owner, model.MemoryGB, model.HostMemoryGB)
	return roomArms{need, need}
}

// afterReleases projects target m once jobs and slots release: CPU and RAM.
func (q *Queue) afterReleases(m *Machine, jobs []*Job, slots []*ModelSlot) Machine {
	projected := q.cpuAfterReleases(m, jobs, slots, 0)
	room := q.hostRoomAfter(m, jobs, slots)
	projected.FreeHostMemoryGB, projected.hostReservationDebtGB = max(0, room), max(0, -room)
	return projected
}

// uncreditedGB is use a release frees beyond its charge on the physical arm: a
// started unified CUDA attempt's device memory, which live availability lacks
// but no cgroup sample proves (#424). Its declared size is taken as held
// (#598) and a fresh larger measurement counts (#596). Planning only:
// admission still waits for the confirmed release and rechecks live memory.
func uncreditedGB(j *Job, owner *Machine, s hostMemorySample, now time.Time) float64 {
	if !unifiedCUDA(owner) || j.StartedAt == nil {
		return 0
	}
	device := j.Spec.Needs.MemoryGB
	if !s.deviceAt.IsZero() && !now.Before(s.deviceAt) && now.Sub(s.deviceAt) < deviceMemorySampleFreshFor {
		device = max(device, s.deviceGB)
	}
	return device
}
