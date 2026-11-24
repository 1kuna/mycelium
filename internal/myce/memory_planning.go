package myce

// Owned by one immutable planning pass, shared across its target machines.
// Never retained across placements, inventory changes, or transaction boundaries.
type memoryPlanningCache struct {
	components  map[memoryComponentKey]*Machine
	allocations map[allocationViewKey]*Machine
	// Storage observations grouped by family for this pass.
	storageFamilies map[string][]StorageObservation
	storagePrograms map[string]bool
}

type memoryComponentKey struct {
	owner  *Machine
	device string
	cpu    bool
}

// Observation identity reads inventory, not projected free room. Component
// copies are private to this immutable pass, including permanent-capacity views.
func (q *Queue) memoryObservationMachine(j *Job, owner *Machine) *Machine {
	key := memoryComponentKey{owner: owner, cpu: j.Spec.Needs.cpuOnly()}
	if len(j.AllocatedDevices) == 1 {
		key.device = j.AllocatedDevices[0]
	}
	cache := q.memoryPlanning
	if cache != nil && len(j.AllocatedDevices) <= 1 {
		if m := cache.components[key]; m != nil {
			return m
		}
	}
	m := allocationMachine(owner, j.AllocatedDevices)
	if key.cpu && owner.Devices != nil {
		m = cpuMachineView(owner)
	}
	if cache != nil && len(j.AllocatedDevices) <= 1 {
		if cache.components == nil {
			cache.components = map[memoryComponentKey]*Machine{}
		}
		cache.components[key] = m
	}
	return m
}

func newMemoryPlanningCache() *memoryPlanningCache { return &memoryPlanningCache{} }
