package myce

// Owned by one immutable placementScan, shared across its target machines.
// Never retained across placements, inventory changes, or transaction boundaries.
type memoryPlanningCache struct {
	observations *memoryObservationCatalog
	lower        map[memoryLowerKey]float64
	constrained  map[constrainedMemoryKey]float64
	similar      map[similarMemoryKey]similarMemoryResult
	cold         map[coldMemoryKey]coldMemoryResult
}
type memoryLowerKey struct {
	work memoryWorkKey
	os   string
}
type constrainedMemoryKey struct {
	lower       memoryLowerKey
	job         string
	constrained float64
}
type similarMemoryKey struct {
	family, os string
	gpu        bool
}
type similarMemoryResult struct {
	peak     float64
	evidence int
}
type coldMemoryKey struct {
	family           similarMemoryKey
	constrained      constrainedMemoryKey
	allowance, floor float64
	source           string
}
type coldMemoryResult struct {
	host     float64
	source   string
	evidence int
	ok       bool
}

func newMemoryPlanningCache() *memoryPlanningCache {
	return &memoryPlanningCache{
		lower: map[memoryLowerKey]float64{}, constrained: map[constrainedMemoryKey]float64{}, similar: map[similarMemoryKey]similarMemoryResult{}, cold: map[coldMemoryKey]coldMemoryResult{},
	}
}
func memoryLowerIdentity(key ObservationKey, configuration string) memoryLowerKey {
	return memoryLowerKey{memoryWorkIdentity(key, configuration), key.Target.OS}
}
func constrainedMemoryIdentity(j *Job, estimate memoryEstimate) constrainedMemoryKey {
	key := constrainedMemoryKey{job: j.ID, constrained: estimate.constrainedGB}
	if estimate.key != nil {
		key.lower = memoryLowerIdentity(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program))
	}
	return key
}
