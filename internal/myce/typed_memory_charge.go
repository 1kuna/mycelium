package myce

import "fmt"

// A worker with this capability launches a typed CPU job charged below its
// typed figure: it checks live RAM for the charge and still enforces the
// typed figure. Older workers are charged the typed figure (MY-42).
const measuredMemoryChargeCapability = "measured_memory_charge_v1"

const cgroupMemoryEnforcement = "memory ceilings enforced (cgroup v2)"

// typedMemoryCharge is what admission charges a typed CPU job on target m:
// what this work reached in accepted runs plus their measured uncertainty
// (MY-31), the exact work's or, with at least two accepted runs, its
// family's (#1466). The typed figure stays the enforced limit and caps the
// charge. It stays the charge where any of these holds:
//   - no accepted evidence, or an OOM, refusal or recovery in progress;
//   - production, which keeps its protection;
//   - the shared interactive host (MY-45), or a target whose guard
//     cannot contain the job at its typed figure (cgroup v2 or the Darwin RSS
//     guard can), or whose allowance cannot hold that figure;
//   - a worker without measuredMemoryChargeCapability.
//
// Growth past the charge is the existing by-use lender's: the guard stops it
// at the typed figure and yieldMemoryOvercommit before neighbours run out.
func (q *Queue) typedMemoryCharge(j *Job, m *Machine) (charge float64, source string, evidence int, ok bool) {
	n := j.Spec.Needs
	typed := n.HostMemoryGB
	if !q.learnedMemory() || n.Memory == "auto" || !n.cpuOnly() || !validRate(typed) || m.SharedHost != nil ||
		!contains(m.Capabilities, measuredMemoryChargeCapability) || m.MemoryEnforcement != cgroupMemoryEnforcement && m.MemoryEnforcement != darwinRSSGuardStatus ||
		j.OOMRetries > 0 || j.OOMCapacity != nil || j.MemoryOOMFloor != nil || q.priorityMatch(j.Spec).Tier == "production" {
		return 0, "", 0, false
	}
	if allowance := hostMemoryAllowanceGB(m); allowance <= 0 || hostNeed(m, 0, typed) > allowance {
		return 0, "", 0, false
	}
	estimate := q.memoryEstimate(j, m)
	if estimate.refusal != "" {
		return 0, "", 0, false
	}
	if host, ok := learnedHostGB(estimate); ok {
		charge, source, evidence = host, estimate.source, estimate.samples
	} else if host, runs, ok := q.familyMemoryBound(j, m, estimate); ok && runs >= memoryFamilyHintRuns {
		charge, source, evidence = host, "accepted family peaks + measured uncertainty", runs
	} else {
		return 0, "", 0, false
	}
	charge = max(charge, memberHostFloor(j, m), q.constrainedMemoryLower(j, estimate))
	if !validRate(charge) || charge >= typed {
		return 0, "", 0, false
	}
	return charge, fmt.Sprintf("%s; typed %g GiB is the limit", source, typed), evidence, true
}

// learnedHostGB is the learned host figure of a calibrated estimate: the
// accepted peak with its measured uncertainty, as auto sizing reserves it.
func learnedHostGB(e memoryEstimate) (float64, bool) {
	if e.host == nil || e.samples == 0 || e.escalation || e.foreign || e.uncalibrated || e.source == spentMemoryExploration || e.source == priorVersionMemorySource || e.measuredHostGB > 0 && boundedMemorySource(e.source) {
		return 0, false
	}
	host := hostMemoryPeakGB(e.host) * e.hostFactor
	if e.measuredHostGB > 0 {
		host = e.measuredHostGB
	}
	host = max(minimumHostMemoryGB, host)
	return host, validRate(host)
}

// familyMemoryBound is the family's accepted same-host peak plus the spread
// between its runs, without the first-run fallbacks coldMemoryBound adds.
func (q *Queue) familyMemoryBound(j *Job, m *Machine, estimate memoryEstimate) (float64, int, bool) {
	if !q.learnedMemory() {
		return 0, 0, false
	}
	peak, margin, runs := q.similarMemoryPeak(j, m)
	host := peak + margin
	if runs == 0 || !validRate(host) || host <= q.constrainedMemoryLower(j, estimate) {
		return 0, 0, false
	}
	return host, runs, true
}

// memberHostFloor is the largest explicit member cap: optional caps are
// declarations no charge goes below.
func memberHostFloor(j *Job, m *Machine) float64 {
	floor := max(minimumHostMemoryGB, j.Spec.Run.MemoryGB)
	for _, side := range j.Spec.Run.Sidecars {
		floor = max(floor, side.MemoryGB)
	}
	if e := qualifiedExtension(j, m); e != nil {
		if v := e.variant(machineTarget(m)); v != nil {
			floor = max(floor, v.Execution.Main.MemoryGB)
			for _, side := range v.Execution.Sidecars {
				floor = max(floor, side.MemoryGB)
			}
		}
	}
	return floor
}
