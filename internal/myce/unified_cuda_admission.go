package myce

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
