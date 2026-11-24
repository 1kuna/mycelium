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

// Queue mutation only: retain historical evidence with an explicit reason.
