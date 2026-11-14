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
		// A restored machine holds no capacity telemetry until its first
		// check-in (#1399). Leave the evidence empty; that report becomes the
		// baseline, so an unchanged reading is still not new capacity.
		evidence := ""
		if !q.unreported[m.ID] {
			evidence = q.launchCapacityEvidence(j, m)
		}
		j.LaunchCapacityWait[targetKey(m)] = LaunchCapacityWait{Machine: j.Machine, Session: j.Session,
			Attempt: j.Attempt, Admission: safeAdmissionDetail(r.ExecutionCause.Admission),
			Evidence: evidence}
	}
}

func (q *Queue) waitLaunchCapacity(j *Job, r Result) {
	q.recordLaunchCapacityWait(j, r)
	q.requeueLaunchCapacity(j)
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
	helper := 0.0
	for _, w := range j.LaunchCapacityWait {
		if w.Attempt != j.Attempt || w.Admission == nil {
			continue
		}
		for _, c := range w.Admission.Contributors {
			if c.Launching {
				helper = max(helper, c.HelperGB)
			}
		}
	}
	if helper > 0 {
		return newHold("helper_charge", detail+fmt.Sprintf("; launch helpers charged %.3f GiB beyond the reservation", helper))
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
func (q *Queue) queuedCapacityStatus(j *Job) (waiting, ready bool) {
	if !dependencyReady(j) || cancelled(j) {
		return false, false
	}
	for _, m := range q.plannerMachines() {
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
			temporary := false
			if c.MemoryEnvelope != nil && c.MemoryEnvelope.Refusal != "" {
				_, why, capacity := q.exploratoryMemoryStatus(j, m)
				if capacity && c.MemoryEnvelope.Refusal == why || c.MemoryEnvelope.Refusal == boundedMemoryCapacityWait {
					// Keep resolved demand and every structural fit check. Only
					// this known physical wait is ignored for route eligibility.
					copy := *c
					envelope := *c.MemoryEnvelope
					envelope.Refusal = ""
					copy.MemoryEnvelope = &envelope
					c, temporary = &copy, true
				}
			}
			if !fitsResources(c, &room, room.TotalMemoryGB) {
				continue
			}
			if drained || temporary || !hostFits(m, c.Spec.Needs.MemoryGB, c.Spec.Needs.HostMemoryGB) || !cpuFits(c, m) || !q.batchSlotFree(m, c) || m.FreeMemoryGB < c.Spec.Needs.MemoryGB {
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
	h, ok := j.LaunchCapacityWait[targetKey(m)]
	if !ok || h.Evidence != q.launchCapacityEvidence(j, m) || h.Admission == nil {
		return m
	}
	d := h.Admission
	if d.Unavailable || d.Available == nil || d.Required <= *d.Available {
		return m
	}
	copy := *m
	deficit := d.Required - *d.Available
	switch d.Subcheck {
	case "live_host_ram", "live_host_growth", "aggregate_host_allowance", "configured_host_allowance":
		copy.FreeHostMemoryGB = min(copy.FreeHostMemoryGB, hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)-deficit)
	case "aggregate_cpu_allowance":
		copy.EffectiveCPUCores -= max(0, copy.FreeCPUCores-j.Spec.Needs.cores()+int(deficit))
	default:
		return m
	}
	return &copy
}
