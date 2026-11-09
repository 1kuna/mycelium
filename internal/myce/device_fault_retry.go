package myce

import (
	"slices"
	"strings"
	"time"
)

const deviceFaultRetryHold = "device_fault_last_healthy"

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
		if !q.deviceFaultRetryBlocked(j, m) {
			return ""
		}
		blocked = true
	}
	if blocked {
		return deviceFaultRetryHold + ": preserving the host's last healthy accelerator after this job's driver fault; waiting for another healthy device or host, or a health-proven recovery"
	}
	return ""
}

func deviceFaultRetryReason(reason string) bool {
	return strings.HasPrefix(reason, deviceFaultRetryHold+":")
}
