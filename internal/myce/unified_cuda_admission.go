package myce

import "fmt"

func unifiedCUDA(m *Machine) bool {
	return m != nil && backendFor(m.GPU).unifiedProcessAdmission && m.UnifiedMemory
}

func hostHasUnifiedCUDA(m *Machine) bool {
	if unifiedCUDA(m) {
		return true
	}
	for _, d := range m.Devices {
		if backendFor(d.Platform).unifiedProcessAdmission && d.UnifiedMemory {
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
	// Raw available memory already reflects OS and driver use. Only an
	// explicit reserve or configured physical allowance withholds more RAM.
	floor := m.ReservedMemoryGB
	for _, d := range m.Devices {
		if backendFor(d.Platform).unifiedProcessAdmission && d.UnifiedMemory {
			floor = max(floor, d.ReserveMemoryGB)
		}
	}
	budget := m.HostMemoryBudgetGB
	if m.EffectiveHostMemoryBudgetGB > 0 {
		budget = m.EffectiveHostMemoryBudgetGB
	}
	// The floor is compared with live used RAM, which already contains the
	// OS, driver and idle-service footprint. Any allowance, measured or
	// authored, is a cap on charges: physical-minus-allowance as a floor would
	// subtract that footprint a second time. Only the shared-interactive host
	// keeps the complement, as its stricter owner protection (MY-45).
	if budget > 0 && m.SharedHost != nil {
		floor = max(floor, total-budget)
	}
	return floor, true
}

// Existing holds still own takeover/preemption. This additional admission-only
// check protects a sole qualified destination for queued, forecast production
// before the dependency becomes runnable. Cancellation removes it immediately.
func (q *Queue) unifiedCUDAProductionReason(j *Job, m *Machine) string {
	if !unifiedCUDA(m) || j.Spec.Needs.cpuOnly() {
		return ""
	}
	host := hostID(m)
	charge := 0.0
	for _, candidate := range q.memoryCandidates(j, m) {
		charge = max(charge, hostNeed(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB))
	}
	// An offer already owns its reservation: revalidate it once, not twice.
	room := q.hostRoomAfter(m, []*Job{j}, nil)
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
			for _, candidate := range q.memoryCandidates(demand, target) {
				// Evidence belongs to this exact recipe, not a qualified sibling.
				if q.variantParityReason(candidate, target) != "" || demand.ProductionHost != "" && hostID(target) != demand.ProductionHost && !q.productionParityKnown(candidate, target) {
					continue
				}
				if !fitsResources(candidate, target, target.TotalMemoryGB) {
					continue
				}
				if hostID(target) != host {
					now := q.clock.Now()
					if q.unreported[target.ID] || !q.workerFresh(target, now) {
						continue
					}
					// Reuse #401's immediate-admission proof, forecasting only the
					// dependency's readiness. Approval alone is not spare capacity.
					copy := *candidate
					copy.ProductionHost, copy.Spec.After = "", nil
					copy.RequireProductionParity = demand.ProductionHost != "" && hostID(target) != demand.ProductionHost || demand.RequireProductionParity
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
