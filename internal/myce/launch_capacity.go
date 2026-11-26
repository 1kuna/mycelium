package myce

import (
	"fmt"
	"slices"
)

// LaunchCapacityWait remembers the evidence behind a refused offer. It is
// admission policy only; Copies/HostReleases remain the exact custody authority.
// No timer clears it: an unchanged heartbeat is not new capacity.
type LaunchCapacityWait struct {
	Machine   string           `json:"machine"`
	Session   string           `json:"session"`
	Attempt   int              `json:"attempt"`
	Evidence  string           `json:"evidence"`
	Admission *AdmissionDetail `json:"admission,omitempty"`
	// Required surplus above the candidate's charge in the queue's physical
	// RAM ledger. It can be signed because queue room can already be short.
	LiveGrowthSurplusGB *float64 `json:"live_growth_surplus_gb,omitempty"`
}

const launchCapacityWaitReason = "waiting for launch capacity"

func launchCapacityRefusal(r Result) bool {
	e := safeExecutionEvidence(r.ExecutionEvidence, r.Attempt)
	if e != nil && e.Main != executionNotStarted {
		return false
	}
	c := r.ExecutionCause
	return !r.Success && !r.OutOfMemory && r.Interrupted && c != nil &&
		c.Operation == "launch" && c.Predicate == "memory_capacity" &&
		(c.Class == "resource_limit" || c.Class == "io_error" && e != nil && e.Main == executionNotStarted)
}

// Only capacity values and exact competing custody identities participate.
// Exclude this job: releasing its refused helper cannot by itself prove that
// repeating the same admission will succeed. Timestamps/contact are not room.
func (q *Queue) launchCapacityEvidence(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	parts := []string{fmt.Sprintf("demand/%g/%g/%d", j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB, j.Spec.Needs.cores()), fmt.Sprintf("%s/%s/%t/%g/%t/%g/%g/%g/%d/%d/%g", m.ID, m.Session,
		m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryError != "", m.HostMemoryBudgetGB,
		m.MemoryUsedGB, m.TotalMemoryGB, m.CPUBudgetCores, m.CPUReserveCores, m.ReservedMemoryGB)}
	if m.CapabilityProfile != nil {
		parts = append(parts, fmt.Sprint(m.CapabilityProfile.LogicalCPUs))
	}
	if gate := m.HostRAMGate; gate != nil {
		parts = append(parts, fmt.Sprintf("ram-gate/%g/%g/%g", gate.RawGB, gate.HeldGB, gate.PendingGB))
	}
	for _, o := range q.hostObligations(hostID(m)) {
		if o.Result.ID == j.ID {
			continue
		}
		part := obligationKey(o)
		if other := q.s.Jobs[o.Result.ID]; other != nil && other.Machine == o.Result.Machine && other.Session == o.Result.Session && other.Attempt == o.Result.Attempt {
			part += fmt.Sprintf("/%g/%d", q.hostJobCharge(other), other.Spec.Needs.cores())
		}
		parts = append(parts, part)
	}
	slices.Sort(parts)
	return identityHash(parts)
}

func (q *Queue) recordLaunchCapacityWait(j *Job, r Result) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if m != nil && j.Spec.Needs.cpuOnly() {
		m = cpuMachineView(m)
	}
	if j.LaunchCapacityWait == nil {
		j.LaunchCapacityWait = map[string]LaunchCapacityWait{}
	}
	if m != nil {
		key := targetKey(m)
		wait := LaunchCapacityWait{Machine: j.Machine, Session: j.Session, Attempt: j.Attempt,
			Admission: safeAdmissionDetail(r.ExecutionCause.Admission)}

		if prior := j.LaunchCapacityWait[key]; prior.Session == wait.Session && prior.Attempt == wait.Attempt {
			return // A repeated result must not move the baseline to a newer reading.
		}
		if !q.unreported[m.ID] {
			wait.Evidence = q.launchCapacityEvidence(j, m)
		}
		j.LaunchCapacityWait[key] = wait
	}
}

func (q *Queue) waitLaunchCapacity(j *Job, r Result) {
	q.recordLaunchCapacityWait(j, r)
	q.requeueLaunchCapacity(j)
	if j.State == "queued" && r.ExecutionCause.Admission != nil && r.ExecutionCause.Admission.Subcheck == memoryWaitReason {
		j.Reason = memoryWaitReason
		j.StartCapacityWaiting = !absoluteStartBy(j)
		if j.Starts > 0 {
			j.Starts--
			j.AdmissionRequeues++
		}
	}
}

func (q *Queue) requeueLaunchCapacity(j *Job) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	// Fence every copy for every tier. Missing Started is never release proof.
	for i := range j.Copies {
		j.Copies[i].Recoverable = false
	}
	q.requeue(j, launchCapacityWaitReason, false)
}

func (q *Queue) launchCapacityBlocked(j *Job, m *Machine) bool {
	if len(j.LaunchCapacityWait) == 0 {
		return false
	}
	// A helper with uncertain cleanup prevents this logical job from moving
	// anywhere, including another host that happens to have free room.
	for _, c := range j.Copies {
		if !c.ReleaseConfirmed {
			return true
		}
	}
	h, ok := j.LaunchCapacityWait[targetKey(m)]
	if !ok {
		return false
	}
	if worker := q.s.Machines[m.ID]; worker != nil && worker.HostRAMGate != nil && h.Admission != nil &&
		h.Admission.Subcheck == memoryWaitReason && h.Session == m.Session && worker.Session == m.Session {
		// Only a RAM read taken after the worker released the refused attempt
		// can reopen the route. Placement still requires the full demand to fit.
		return !hostRAMGateAfter(worker, j)
	}
	if h.Evidence == "" {
		// Refused before the first post-restart report. Only a committed
		// pass after that report records the baseline; views stay blocked.
		if !q.unreported[m.ID] && q.records != nil && q.records.updating {
			q.touchJob(j.ID)
			h.Evidence = q.launchCapacityEvidence(j, m)
			j.LaunchCapacityWait[targetKey(m)] = h
		}
		return true
	}
	return h.Evidence == q.launchCapacityEvidence(j, m)
}

// The pending launch-capacity wait. A refusal whose launching attempt carried
// measured helper charge beyond its reservation is its own kind.
func launchCapacityHold(j *Job) *Hold {
	detail := "waiting for launch capacity or exact release; unchanged capacity evidence is not re-offered"
	for _, w := range j.LaunchCapacityWait {
		if w.Attempt != j.Attempt || w.Admission == nil {
			continue
		}
		if d := w.Admission; d.Subcheck == memoryWaitReason {
			available := "unknown"
			if d.Available != nil {
				available = fmt.Sprintf("%.3f GiB", *d.Available)
			}
			return newHold(memoryWaitReason, fmt.Sprintf("needs %.3f GiB, available %s, keep %.3f GiB, outstanding reservations %.3f GiB", d.Required, available, d.KeepGB, d.HeldGB))
		}
	}
	return newHold("launch_capacity", detail)
}

func (q *Queue) launchCapacityPending(j *Job) bool {
	if len(j.LaunchCapacityWait) == 0 {
		return false
	}
	for _, hold := range j.LaunchCapacityWait {
		if hold.Attempt == j.Attempt && (j.State == "queued" || hold.Machine == j.Machine && hold.Session == j.Session) {
			return true
		}
	}
	return false
}

// A qualified route with occupied resources is capacity waiting even before its
// first offer. Missing/incompatible/disabled hosts retain their existing startup
// diagnosis. This uses the existing fit checks with only resources released.
func (q *Queue) queuedCapacityStatus(j *Job, scans ...*placementScan) (waiting, ready bool) {
	if !dependencyReady(j) || cancelled(j) {
		return false, false
	}
	scan := q.placementScan(scans...)
	for _, m := range scan.machines {
		drained := q.s.HostMaintenance[hostID(m)].Mode == "drained"
		if !q.jobAllowsMachine(j, m.ID) || q.s.HostMaintenance[hostID(m)].Mode == "disabled" || !drained && (m.Gone || m.Leaving) {
			continue
		}
		room := *m
		if drained {
			// Maintenance may restart the worker; its absence is part of the
			// drain, not evidence that this qualified route stopped existing.
			room.Gone, room.Leaving = false, false
		}
		room.FreeCPUCores, room.CPUInUseCores = room.EffectiveCPUCores, 0
		for _, c := range q.memoryCandidates(j, m) {
			if !fitsResources(c, &room, room.TotalMemoryGB) {
				continue
			}
			if drained || !hostFits(m, c.Spec.Needs.MemoryGB, c.Spec.Needs.HostMemoryGB) || !cpuFits(c, m) || !q.batchSlotFree(m, c) || m.FreeMemoryGB < c.Spec.Needs.MemoryGB || !q.poolCustodyAllows(c, m) || q.workerRAMGate(c, m) != nil {
				waiting = true
			} else if pooledMemory(c) && !q.hostBackfillAllows(c, m, q.hostBackfillFences(scan)[hostID(m)]) {
				waiting = true
			} else {
				ready = true
			}
		}
	}
	return waiting && !ready, ready
}

// Feed the worker's measured deficit into ordinary release planning only.
// This neither discounts reservations nor authorizes admission. The existing
// planner chooses lower tiers; same-tier work remains protected. Exact releases
// and a changed capacity snapshot are still required by dispatch.
func (q *Queue) launchCapacityProjection(j *Job, m *Machine) *Machine {
	if short := q.workerRAMShortfall(j, m); short > 0 {
		// The worker's gate refuses before any offer, so its shortfall stands
		// in for the refusal detail an older worker would have returned.
		return withHostRoom(m, hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)-short)
	}
	h, ok := j.LaunchCapacityWait[targetKey(m)]
	if !ok || h.Evidence != q.launchCapacityEvidence(j, m) || h.Admission == nil {
		return m
	}
	d := h.Admission
	if d.Subcheck == memoryWaitReason && m.HostRAMGate != nil {
		// The current gate supersedes the refusal's reading, whose room can
		// include measured native startup loss. That loss never reclaims.
		return m
	}
	if d.Unavailable || d.Available == nil || d.Required+d.HeldGB+d.KeepGB <= *d.Available {
		return m
	}
	deficit := d.Required + d.HeldGB + d.KeepGB - *d.Available
	switch d.Subcheck {
	case memoryWaitReason, "live_host_ram", "live_host_growth", "aggregate_host_allowance", "configured_host_allowance":
		return withHostRoom(m, hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)-deficit)
	case "aggregate_cpu_allowance":
		copy := *m
		copy.EffectiveCPUCores -= max(0, copy.FreeCPUCores-j.Spec.Needs.cores()+int(deficit))
		return &copy
	}
	return m
}

// withHostRoom lowers m's signed host room to the worker's view. The worker's
// shortfall already contains any debt the queue also sees, so the planner must
// repay the smaller room once, not the queue's debt on top of it.
func withHostRoom(m *Machine, room float64) *Machine {
	copy := *m
	room = min(room, m.FreeHostMemoryGB-m.hostReservationDebtGB)
	copy.FreeHostMemoryGB, copy.hostReservationDebtGB = max(0, room), max(0, -room)
	return &copy
}
