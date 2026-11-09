package myce

import (
	"slices"
	"strings"
	"time"
)

const deviceFaultRetryHold = "device_fault_last_healthy"

const deviceFaultRecoveryTimeout = "device_fault_recovery_timeout"

// Persist the suspended relative budget, independently of the original window
// used by a future started-attempt loss. Queue time orders both values.
type DeviceFaultStartWait struct {
	Since     time.Time     `json:"since"`
	Remaining time.Duration `json:"remaining"`
}

// Bound the recovery wait from loss detection, not from a heartbeat or restart.
// Reuse the existing maximum availability window rather than introducing a
// deployment-specific quiet period. Older typed-less outcomes retain queue time.
func deviceFaultRecoveryDeadline(j *Job) *time.Time {
	var loss time.Time
	for _, h := range j.AttemptHistory {
		if h.DeviceFault != nil {
			loss = maxTime(loss, h.DeviceFault.At)
		} else if h.Reason == "accelerator driver fault; device quarantined" {
			loss = maxTime(loss, h.EndedAt)
		}
	}
	if loss.IsZero() {
		return nil
	}
	deadline := loss.Add(MaxProductionStartWindow)
	return &deadline
}

func (q *Queue) resumeDeviceFaultStartBudget(j *Job) {
	if wait := j.DeviceFaultStartWait; wait != nil {
		if j.Spec.StartBy != nil && !absoluteStartBy(j) {
			deadline := q.clock.Now().Add(wait.Remaining)
			j.Spec.StartBy = &deadline
		}
		j.DeviceFaultStartWait = nil
	}
}

// A typed last-healthy wait is uncharged startup time. Resource contention
// keeps its existing semantics; this pause preserves only the budget remaining
// at entry. A disappearing route resumes the ordinary unavailable-route budget.
func (q *Queue) maintainDeviceFaultStartWait(j *Job) bool {
	if terminal(j.State) || cancelled(j) {
		j.DeviceFaultStartWait = nil
		return false
	}
	if j.State != "queued" || q.deviceFaultRetryWaitingReason(j) == "" {
		q.resumeDeviceFaultStartBudget(j)
		return false
	}
	now := q.clock.Now()
	if j.DeviceFaultStartWait == nil {
		remaining := time.Duration(0)
		if j.Spec.StartBy != nil && !absoluteStartBy(j) {
			remaining = max(0, j.Spec.StartBy.Sub(now))
			if j.StartCapacityWaiting || q.launchCapacityPending(j) {
				remaining = j.StartWindow
			}
		}
		j.DeviceFaultStartWait = &DeviceFaultStartWait{Since: now, Remaining: remaining}
		j.StartCapacityWaiting = false
	}
	// Caller cutoffs win, even when loss recovery or a capacity pause preceded
	// the fault. GiveUpAt retains its queued-only authority.
	deadline := deviceFaultRecoveryDeadline(j)
	if expired(j.Spec.GiveUpAt, now) && (!absoluteStartBy(j) || j.Spec.GiveUpAt.Before(*j.Spec.StartBy)) &&
		(deadline == nil || !j.Spec.GiveUpAt.After(*deadline)) {
		j.State, j.Reason, j.FinishedAt = "failed", "give-up time reached", &now
		return true
	}
	if absoluteStartBy(j) && expired(j.Spec.StartBy, now) && (deadline == nil || !j.Spec.StartBy.After(*deadline)) {
		return true
	}
	if expired(deadline, now) {
		j.State, j.DiagnosticCode = "failed", deviceFaultRecoveryTimeout
		j.Reason = deviceFaultRecoveryTimeout + ": no eligible recovery within the maximum production start window"
		j.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "timeout", Predicate: deviceFaultRecoveryTimeout}
		j.ExecutionEvidence = unstartedExecution(0)
		j.FinishedAt = &now
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
	}
	return true
}

// Queue-owned loss evidence survives inventory changes and worker upgrades.
type DeviceFaultLoss struct {
	Host     string    `json:"host"`
	Platform string    `json:"platform"`
	Devices  []string  `json:"devices"` // Stable identities, not runtime ordinals.
	At       time.Time `json:"at"`
}

func (q *Queue) deviceFaultRetryBlocked(j *Job, target *Machine) bool {
	if j == nil || target == nil || j.Spec.Needs.cpuOnly() || target.GPU == "cpu" {
		return false
	}
	var lossAt time.Time
	faulted := false
	for _, h := range j.AttemptHistory {
		if f := h.DeviceFault; f != nil {
			if f.Host == hostID(target) && f.Platform == target.GPU {
				faulted, lossAt = true, maxTime(lossAt, f.At)
			}
		} else if h.Reason == "accelerator driver fault; device quarantined" && h.Machine == target.ID {
			// Pre-upgrade queue history has no typed record. Limit inference to
			// this worker and the backend recorded at assignment, if available.
			for _, c := range j.Copies {
				if c.Attempt == h.Attempt && c.Machine == h.Machine && c.Session == h.Session {
					for _, d := range target.Devices {
						if d.Platform == target.GPU && slices.Contains(c.AllocatedDevices, d.ID) {
							faulted, lossAt = true, maxTime(lossAt, h.EndedAt)
						}
					}
				}
			}
		}
	}
	if !faulted {
		return false
	}
	healthy, selected := 0, 0
	proven := false
	// Host-wide inventory includes legacy per-device workers. Occupancy is
	// deliberately irrelevant: a busy healthy card still preserves capacity.
	seen := map[string]bool{}
	for _, m := range q.s.Machines {
		if hostID(m) != hostID(target) || m.Gone || m.Replacement != nil {
			continue
		}
		devices := m.Devices
		if devices == nil {
			devices = []MachineDevice{{ID: m.Device, Platform: m.GPU, Identity: m.ID + ":" + m.Device, Reason: m.MemoryError, BudgetGB: m.TotalMemoryGB}}
		}
		for _, d := range devices {
			key := deviceFaultKey(d)
			if d.Platform != target.GPU || seen[key] || d.Reason != "" || d.BudgetGB <= 0 || q.deviceAdmissionBlocked(allocationMachine(m, []string{d.ID})) {
				continue
			}
			seen[key] = true
			healthy++
			if m.ID == target.ID && slices.Contains(targetDevices(target), d.ID) {
				selected++
				// Only a cleared, health-proven card can be used as the sole
				// remaining card. An operator clear alone is not health proof.
				proven = proven || d.Recovery != nil && d.Recovery.ObservedAt.After(lossAt)
			}
		}
	}
	return healthy <= selected && !proven
}

func (q *Queue) deviceFaultRetryWaitingReason(j *Job, scans ...*placementScan) string {
	if !slices.ContainsFunc(j.AttemptHistory, func(h AttemptOutcome) bool {
		return h.DeviceFault != nil || h.Reason == "accelerator driver fault; device quarantined"
	}) {
		return ""
	}
	blocked := false
	for _, m := range q.planningMachines(scans...) {
		if !q.jobAllowsMachine(j, m.ID) || !executionTargetMatches(j.Spec, m) || !deviceAllowed(j, m) || m.GPU == "cpu" || m.MemoryError != "" || m.Gone || len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU) {
			continue
		}
		capacity := q.qualificationCapacity(m)
		for _, candidate := range q.memoryCandidates(j, m) {
			if !fits(candidate, &capacity, capacity.TotalMemoryGB) {
				continue // An undersized card is not an alternative to a held group.
			}
			if !q.deviceFaultRetryBlocked(candidate, m) {
				return ""
			}
			blocked = true
		}
	}
	if blocked {
		return deviceFaultRetryHold + ": preserving the host's last healthy accelerator after this job's driver fault; waiting for another healthy device or host, or a health-proven recovery"
	}
	return ""
}

func deviceFaultRetryReason(reason string) bool {
	return strings.HasPrefix(reason, deviceFaultRetryHold+":")
}
