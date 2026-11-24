package myce

func observationAllowance(m *Machine) string {
	power, mode := "", ""
	if a := machineAvailability(m); a != nil {
		power, mode = a.Signals.Power, a.Mode
	}
	return identityHash([]any{m.CPUReserveCores, m.EffectiveCPUCores, m.EffectiveHostMemoryBudgetGB, m.TotalMemoryGB, m.ReservedMemoryGB, m.UnifiedMemory, power, mode})
}

// Operator allowances and power mode affect throughput comparability, not the
// recipe's memory need. Retain legacy keys; the normal recovery path rechecks
// every physical/recipe identity fence before memory readers use these rows.
func memoryIdentityUsable(p WorkObservation) bool {
	return p.StaleReason == "" || p.StaleReason == "resource allowance or power mode changed"
}

func memoryReferenceKey(e Extension, m *Machine, work string) (*ObservationKey, string) {
	v := e.variant(machineTarget(m))
	if v == nil {
		return nil, "recipe target unavailable"
	}
	ref, ok := v.References[work]
	if !ok {
		return nil, "reference unavailable"
	}
	needs := v.Needs
	s := Spec{Needs: needs}
	s.Needs.Platform = Platform{Required: []string{m.GPU}}
	if err := normalizeResources(&s); err != nil {
		return nil, "resource floors unavailable"
	}
	component, hardware := "cpu", m.ExtensionFingerprint
	if m.GPU != "cpu" {
		component = m.Device
		hardware = ""
		for _, d := range m.Devices {
			if d.ID == component {
				hardware = d.Identity
			}
		}
	}
	if hardware == "" || m.ExtensionFingerprint == "" {
		return nil, "component identity unavailable"
	}
	return referenceObservationKey(e, m, ref, work, s.Needs, nil, nil, component, hardware)
}

// Queue mutation only: retain historical evidence with an explicit reason.
