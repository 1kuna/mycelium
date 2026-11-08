package myce

import "time"

// Owned by one immutable planning pass, shared across its target machines.
// Never retained across placements, inventory changes, or transaction boundaries.
type memoryPlanningCache struct {
	observations *memoryObservationCatalog
	transfers    map[memoryTransferKey]memoryTransferResult
	history      *memoryHistoryCache
	constrained  map[constrainedMemoryKey]float64
}

// Only retained execution evidence is shared with permanent-capacity views.
// Admission-dependent estimates stay in each view's own planning cache.
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
