package myce

import ()

func cpuBackfillMachine(id string, ram float64) Machine {
	m := cpuMachine(id, id, "cpu", 8, ram)
	m.CapabilityProfile.SystemMemoryBytes = uint64(2 * ram * (1 << 30))
	m.HostMemoryBudgetGB = ram
	m.Devices = []MachineDevice{} // Modern CPU worker: concurrent keyed attempts.
	return m
}
