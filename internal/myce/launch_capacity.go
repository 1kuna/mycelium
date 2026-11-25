package myce

import (
	"fmt"
	"time"
)

// LaunchCapacityWait tracks retries on one physical host. Copies and host
// releases still own the refused attempt until exact cleanup is confirmed.
type LaunchCapacityWait struct {
	Machine   string           `json:"machine"`
	Session   string           `json:"session"`
	Attempt   int              `json:"attempt"`
	Admission *AdmissionDetail `json:"admission,omitempty"`
	Delay     time.Duration    `json:"delay,omitempty"`
	RetryAt   time.Time        `json:"retry_at,omitempty,omitzero"`
}

const launchCapacityWaitReason = "waiting for launch capacity"

// Start above the observed 26-second refusal cycle, then space repeated
// failures further apart. Cap the added latency after room returns.
const launchRetryInitial = time.Minute
const launchRetryMax = 15 * time.Minute

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

func (q *Queue) recordLaunchCapacityWait(j *Job, r Result) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	host := q.executionHost(j.Machine)
	if host == "" {
		host = j.Machine
	}
	prior := j.LaunchCapacityWait[host]
	if prior.Machine == j.Machine && prior.Session == j.Session && prior.Attempt == j.Attempt {
		return // Multiple receipts for the same refusal count once.
	}
	delay := min(launchRetryMax, max(launchRetryInitial, 2*prior.Delay))
	if j.LaunchCapacityWait == nil {
		j.LaunchCapacityWait = map[string]LaunchCapacityWait{}
	}
	j.LaunchCapacityWait[host] = LaunchCapacityWait{
		Machine: j.Machine, Session: j.Session, Attempt: j.Attempt,
		Admission: safeAdmissionDetail(r.ExecutionCause.Admission),
		Delay:     delay, RetryAt: q.clock.Now().Add(delay),
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
	return q.clock.Now().Before(j.LaunchCapacityWait[hostID(m)].RetryAt) || launchCapacityReleasePending(j)
}

func launchCapacityReleasePending(j *Job) bool {
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
	return false
}

func (w LaunchCapacityWait) hold(host string) *Hold {
	kind, detail := "launch_capacity", "worker refused launch capacity"
	if d := w.Admission; d != nil {
		if d.Subcheck == memoryWaitReason {
			kind = memoryWaitReason
			detail = fmt.Sprintf("last launch needed %.3f GiB, available %s", d.Required, d.poolRoomText())
		} else {
			detail = (RuntimeCause{Operation: "launch", Class: "resource_limit", Admission: d}).String()
		}
	}
	h := newHold(kind, detail)
	h.Scope, h.Expires = host, &w.RetryAt
	h.Clears = "the retry time passes or another eligible host takes the job, after exact release"
	return h
}

func (q *Queue) launchCapacityHold(j *Job) *Hold {
	if launchCapacityReleasePending(j) {
		return newHold("launch_capacity", "waiting for exact release of the refused attempt")
	}
	now := q.clock.Now()
	var hold *Hold
	for host, w := range j.LaunchCapacityWait {
		if now.Before(w.RetryAt) && (hold == nil || w.RetryAt.Before(*hold.Expires) || w.RetryAt.Equal(*hold.Expires) && host < hold.Scope) {
			hold = w.hold(host)
		}
	}
	return hold
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
	now := q.clock.Now()
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
			if drained || memoryRetryActive(j, hostID(m), now) || !hostFits(m, c.Spec.Needs.MemoryGB, c.Spec.Needs.HostMemoryGB) || !cpuFits(c, m) || !q.batchSlotFree(m, c) || m.FreeMemoryGB < c.Spec.Needs.MemoryGB || !q.poolCustodyAllows(c, m) {
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
