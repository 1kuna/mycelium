package myce

import (
	"fmt"
	"slices"
	"strings"
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

func launchCapacityRefusal(r Result) bool {
	c := r.ExecutionCause
	return !r.Success && !r.OutOfMemory && r.Interrupted && c != nil &&
		c.Operation == "launch" && c.Class == "resource_limit" && c.Predicate == "memory_capacity"
}

// Only capacity values and exact competing custody identities participate.
// Exclude this job: releasing its refused helper cannot by itself prove that
// repeating the same admission will succeed. Timestamps/contact are not room.
func (q *Queue) launchCapacityEvidence(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	parts := []string{fmt.Sprintf("%s/%s/%t/%g/%s/%g/%g/%g/%d/%d/%g", m.ID, m.Session,
		m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryError, m.HostMemoryBudgetGB,
		m.MemoryUsedGB, m.TotalMemoryGB, m.CPUBudgetCores, m.CPUReserveCores, m.ReservedMemoryGB)}
	if m.CapabilityProfile != nil {
		parts = append(parts, fmt.Sprint(m.CapabilityProfile.LogicalCPUs))
	}
	for _, o := range q.hostObligations(hostID(m)) {
		if o.Result.ID == j.ID {
			continue
		}
		parts = append(parts, obligationKey(o))
	}
	slices.Sort(parts)
	return strings.Join(parts, "|")
}

func (q *Queue) waitLaunchCapacity(j *Job, r Result) {
	m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if j.LaunchCapacityWait == nil {
		j.LaunchCapacityWait = map[string]LaunchCapacityWait{}
	}
	if m != nil {
		j.LaunchCapacityWait[targetKey(m)] = LaunchCapacityWait{Machine: j.Machine, Session: j.Session,
			Attempt: j.Attempt, Admission: safeAdmissionDetail(r.ExecutionCause.Admission),
			Evidence: q.launchCapacityEvidence(j, m)}
	}
	// Fence every copy for every tier. Missing Started is never release proof.
	for i := range j.Copies {
		j.Copies[i].Recoverable = false
	}
	q.requeue(j, "waiting for launch capacity", false)
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
	return ok && h.Evidence == q.launchCapacityEvidence(j, m)
}
