package myce

import (
	"fmt"
	"time"
)

// Reuse the installed unified-CUDA OS/driver reserve, not a crash-derived
// carve-out. A smaller host allowance protects the larger physical remainder.
const unifiedCUDAReserveGB = 16.0

func unifiedCUDA(m *Machine) bool { return m != nil && m.GPU == "cuda" && m.UnifiedMemory }

func hostHasUnifiedCUDA(m *Machine) bool {
	if unifiedCUDA(m) {
		return true
	}
	for _, d := range m.Devices {
		if d.Platform == "cuda" && d.UnifiedMemory {
			return true
		}
	}
	return false
}

func unifiedCUDAFloor(m *Machine) (float64, bool) {
	if m.CapabilityProfile == nil || m.CapabilityProfile.SystemMemoryBytes == 0 {
		return 0, false
	}
	total := float64(m.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
	floor := max(unifiedCUDAReserveGB, m.ReservedMemoryGB)
	for _, d := range m.Devices {
		if d.Platform == "cuda" && d.UnifiedMemory {
			floor = max(floor, d.ReserveMemoryGB)
		}
	}
	budget := m.HostMemoryBudgetGB
	if m.hostBudgetGB > 0 {
		budget = m.hostBudgetGB
	}
	if budget > 0 {
		floor = max(floor, total-budget)
	}
	return floor, true
}

// Host anon/current cannot establish overlap with CUDA allocations (#424).
// Credit at most the host vector; the full device vector remains growth even
// when the cgroup is larger. Complete device peaks are not current ownership.
func remainingAllocationGrowth(m *Machine, device, reservation float64, sample hostMemorySample, now time.Time) float64 {
	growth := remainingHostGrowth(reservation, sample, now)
	if unifiedCUDA(m) {
		growth = max(growth, device)
	}
	return growth
}

// Read only check-in snapshots under q.mu. Raw available RAM already includes
// all managed and unmanaged allocations: never add the external-RSS history.
// Subtract only outstanding growth and the reserve, once each.
func (q *Queue) unifiedCUDAHostRoom(m *Machine, room, growth float64) float64 {
	if !hostHasUnifiedCUDA(m) {
		return room
	}
	for _, sibling := range q.s.Machines {
		if hostID(sibling) != hostID(m) || sibling.Gone {
			continue
		}
		o := sibling.MemoryObservation
		floor, ok := unifiedCUDAFloor(sibling)
		now := q.clock.Now()
		if !ok || !validHostMemoryObservation(o) || sibling.LastSeen.After(now) || now.Sub(sibling.LastSeen) >= hostMemorySampleFreshFor {
			return min(room, -growth)
		}
		room = min(room, o.TotalGB-o.UsedGB-floor-growth)
	}
	return room
}

// Existing holds still own takeover/preemption. This additional admission-only
// check protects a sole qualified destination for queued, forecast production
// before the dependency becomes runnable. Cancellation removes it immediately.
func (q *Queue) unifiedCUDAProductionReason(j *Job, m *Machine) string {
	if !unifiedCUDA(m) || j.Spec.Needs.cpuOnly() || q.fleetPausesProduction() {
		return ""
	}
	host := hostID(m)
	charge := 0.0
	for _, candidate := range q.memoryCandidates(j, m) {
		charge = max(charge, hostNeed(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB))
	}
	room := m.FreeHostMemoryGB - m.hostReservationDebtGB
	if owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices); owner != nil && hostID(owner) == host && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") {
		// An unstarted offer already owns its reservation. Revalidate it once.
		room += remainingAllocationGrowth(owner, j.Spec.Needs.MemoryGB, hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), q.hostMemorySamples[j.Machine][jobKey(*j)], q.clock.Now())
	}
	for _, demand := range q.liveJobs() {
		if demand.State != "queued" || demand.Spec.Priority != "production" || cancelled(demand) || expired(demand.Spec.GiveUpAt, q.clock.Now()) || q.comparePriority(j.Spec, demand.Spec) <= 0 {
			continue
		}
		if demand.FanOut != nil && demand.Spec.FanOut != nil && demand.FanOut.Next >= len(demand.Spec.FanOut.Pieces) {
			continue
		}
		if _, expected := q.productionArrival(demand); !expected {
			continue
		}
		need, sole := 0.0, true
		for _, target := range q.plannerMachines() {
			if !q.jobAllowsMachine(demand, target.ID) || !q.reservationAllows(demand, target) {
				continue
			}
			// A production binding can move only to a work-scoped parity approval.
			if demand.ProductionHost != "" && hostID(target) != demand.ProductionHost && (!demand.Spec.extensionJob() || demand.Spec.ExtensionCheck != nil || !contains(q.s.Policies[demand.Spec.Project].ProductionParityHosts[demand.Spec.workType()], hostID(target))) {
				continue
			}
			for _, candidate := range q.memoryCandidates(demand, target) {
				if !fitsResources(candidate, target, target.TotalMemoryGB) {
					continue
				}
				if hostID(target) != host {
					now := q.clock.Now()
					if q.unreported[target.ID] || target.LastSeen.After(now) || now.Sub(target.LastSeen) >= min(q.lossTimeouts[0], min(q.lossTimeouts[1], q.lossTimeouts[2])) {
						continue
					}
					// Reuse #401's immediate-admission proof, forecasting only the
					// dependency's readiness. Approval alone is not spare capacity.
					copy := *demand
					copy.ProductionHost, copy.Spec.After = "", nil
					alternative := q.batchCandidateWithResources(&copy, target, nil)
					if alternative == nil || !q.workerIdle(target, alternative) || q.immediateCandidate(alternative, target) == nil {
						continue
					}
					sole = false
					continue
				}
				n := candidate.Spec.Needs
				estimate := q.memoryEstimate(candidate, target)
				if estimate.host != nil {
					n.HostMemoryGB = max(n.HostMemoryGB, hostMemoryPeakGB(estimate.host)*estimate.hostFactor)
				}
				if estimate.device != nil && estimate.device.DeviceGB != nil {
					n.MemoryGB = max(n.MemoryGB, *estimate.device.DeviceGB*estimate.deviceFactor)
				}
				envelope := hostNeed(target, n.MemoryGB, n.HostMemoryGB)
				floor, known := unifiedCUDAFloor(target)
				// An envelope that can never fit must not reserve this host
				// forever. Ignore live usage here: transient pressure still
				// deserves protection for production that can eventually fit.
				if !known || envelope > float64(target.CapabilityProfile.SystemMemoryBytes)/(1<<30)-floor {
					continue
				}
				need = max(need, envelope)
			}
		}
		if sole && need > 0 && charge+need > room {
			return fmt.Sprintf("host %s: unified CUDA production headroom for %s; %.3f GiB charge + %.3f GiB production envelope > %.3f GiB physical admission headroom", host, demand.ID, charge, need, room)
		}
	}
	return ""
}
