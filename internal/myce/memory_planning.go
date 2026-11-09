package myce

import "time"

// Owned by one immutable planning pass, shared across its target machines.
// Never retained across placements, inventory changes, or transaction boundaries.
type memoryPlanningCache struct {
	observations *memoryObservationCatalog
	history      *memoryHistoryCache
	constrained  map[constrainedMemoryKey]float64
	components   map[memoryComponentKey]*Machine
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

// Permanent-capacity views share retained execution evidence and the origin's
// observation catalog. Admission-dependent caches stay private to each view.
type memoryHistoryCache struct {
	index  retainedMemoryIndex
	owners map[retainedMemoryOwner]*Machine
	lower  map[memoryLowerKey]memoryConstraintEvidence
}

// Keys separate the full comparable work/configuration and target OS.
// Raw cold facts may be reused; lower bounds and owner views belong to a pass.
type memoryHistoryKey struct{ work, os string }

type memoryLowerKey struct {
	work     memoryWorkKey
	os       string
	discrete bool
	after    time.Time
}
type constrainedMemoryKey struct {
	lower       memoryLowerKey
	job         string
	constrained float64
}
type similarMemoryKey struct {
	family, os string
	gpu        bool
	layout     string
}

func newMemoryPlanningCache() *memoryPlanningCache {
	return &memoryPlanningCache{
		history: &memoryHistoryCache{lower: map[memoryLowerKey]memoryConstraintEvidence{}}, constrained: map[constrainedMemoryKey]float64{},
	}
}
func memoryLowerIdentity(key ObservationKey, configuration string) memoryLowerKey {
	return memoryLowerKey{work: memoryWorkIdentity(key, configuration), os: key.Target.OS}
}
func constrainedMemoryIdentity(j *Job, estimate memoryEstimate) constrainedMemoryKey {
	key := constrainedMemoryKey{job: j.ID, constrained: estimate.constrainedGB}
	if estimate.key != nil {
		key.lower = memoryLowerIdentity(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program))
	}
	return key
}
