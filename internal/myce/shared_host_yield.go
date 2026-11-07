package myce

import (
	"math"
	"strings"
)

// Queue-owned, exact-attempt CPU evidence. PeakCores is a lower bound on the
// logical job's observed need, retained across checkpoint/requeue. A quiet or
// missing sample cannot erase demonstrated overuse. No additional sampler runs.
type SharedCPUObservation struct {
	Attempt        int     `json:"attempt"`
	Machine        string  `json:"machine"`
	Session        string  `json:"session"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	UsageSeconds   float64 `json:"usage_seconds"`
	PeakCores      float64 `json:"peak_cores"`
}

func (q *Queue) observeSharedCPU(j *Job, r *AttemptResources) {
	m := q.s.Machines[j.Machine]
	if m == nil || q.s.HostMaintenance[hostID(m)].SharedHost == nil || m.CapabilityProfile == nil ||
		m.CapabilityProfile.LogicalCPUs <= 0 || r.CPU.UsageSeconds == nil || r.CPU.Unavailable != "" || r.ElapsedSeconds <= 0 {
		return
	}
	usage, elapsed, peak := *r.CPU.UsageSeconds, r.ElapsedSeconds, 0.0
	previous := j.CPUObservation
	comparable := false
	if previous != nil {
		peak = previous.PeakCores
		if previous.Attempt == j.Attempt && previous.Machine == j.Machine && previous.Session == j.Session {
			if elapsed <= previous.ElapsedSeconds || usage < previous.UsageSeconds {
				return // Repeated, reordered or reset counters are not new evidence.
			}
			usage -= previous.UsageSeconds
			elapsed -= previous.ElapsedSeconds
			comparable = true
		}
	}
	// The physical logical CPU count bounds possible simultaneous execution;
	// the operator's smaller admission budget must not hide actual overuse.
	// Process-tree CPU can include supervisor time before observation began.
	// Establish an origin first; only adjacent counters demonstrate a rate.
	if comparable {
		peak = max(peak, min(float64(m.CapabilityProfile.LogicalCPUs), usage/elapsed))
	}
	j.CPUObservation = &SharedCPUObservation{j.Attempt, j.Machine, j.Session, r.ElapsedSeconds, *r.CPU.UsageSeconds, peak}
	// Completion creates cleanup custody before retaining its final counters.
	// Raise that exact receipt as well, before requeue clears the identity.
	key := releaseKey(HostRelease{Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt})
	if release, ok := q.s.HostReleases[key]; ok {
		release.CPUCores = max(release.CPUCores, jobCPUCharge(j, m))
		q.s.HostReleases[key] = release
	}
}

func jobCPUCharge(j *Job, m *Machine) int {
	declared := j.Spec.Needs.cores()
	if learned := j.CPUDemand; learned != nil {
		declared = max(declared, learned.ChargeCores)
	}
	if m.SharedHost == nil || j.CPUObservation == nil || !validMemoryValue(j.CPUObservation.PeakCores) {
		return declared
	}
	// The existing ledger is in whole logical cores; rounding up avoids
	// admitting a whole additional core against a fractional occupied core.
	return max(declared, int(math.Ceil(j.CPUObservation.PeakCores)))
}

// Reuse confirmed host-relative health evidence, including swap growth with
// scheduling latency. Unknown/stale probes withhold admission elsewhere but
// never authorize interrupting a payload. Disk/mount faults are not owner CPU
// or memory pressure and do not acquire this stop policy.
func (q *Queue) sharedOwnerPressure(m *Machine) string {
	if m.SharedHostYieldReason != "" && q.sharedPressureFresh(m) {
		return m.SharedHostYieldReason
	}
	if !hostHealthEvidenceValid(m.HostHealth, q.clock.Now().Sub(m.LastSeen)) {
		return ""
	}
	switch reason := hostHygieneReasonAt(m, q.clock.Now()); reason {
	case "host_memory_pressure", "host_swap_growth", "host_load_latency":
		return "shared host " + strings.ReplaceAll(strings.TrimPrefix(reason, "host_"), "_", " ")
	}
	return ""
}

func (q *Queue) yieldSharedOwnerPressure() {
	// Aggregate by physical host first; a healthy sibling must not clear a
	// warning produced by another worker's pressure observation.
	reasons := map[string]string{}
	shared := map[string]bool{}
	for _, m := range q.s.Machines {
		if m.Gone || q.unreported[m.ID] || m.Replacement != nil || q.s.HostMaintenance[hostID(m)].SharedHost == nil {
			continue
		}
		shared[hostID(m)] = true
		if reason := q.sharedOwnerPressure(m); reason != "" {
			reasons[hostID(m)] = reason
		}
	}
	for _, j := range q.liveJobs() {
		owner := q.s.Machines[j.Machine]
		if owner == nil || !shared[hostID(owner)] || j.State != "running" || cancelled(j) || plannedStop(j) {
			continue
		}
		reason := reasons[hostID(owner)]
		if reason == "" {
			if strings.HasPrefix(j.Reason, "shared host ") && strings.Contains(j.Reason, "; retained ") {
				j.Reason = ""
			}
			continue
		}
		production := j.Spec
		production.Class, production.Priority = "production", "production"
		if jobClass(j.Spec) == "production" || q.priorityMatch(j.Spec).Index <= q.priorityMatch(production).Index {
			j.Reason = reason + "; retained production tier work"
			continue
		}
		if !j.Spec.Resumable {
			j.Reason = reason + "; retained non-resumable work (no owner-pressure kill policy)"
			continue
		}
		for i := range j.Copies {
			c := &j.Copies[i]
			if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session {
				c.StopCause = sharedHostPressureCause
				c.PriorityStop = &PriorityPreemption{Cause: sharedHostPressureCause, Demand: "owner/" + hostID(owner), Victim: j.ID, Policy: q.priorityMatch(j.Spec)}
				c.Recoverable = false
			}
		}
		q.stopFor(j, "owner", "pressure")
		j.Reason = reason
	}
}
