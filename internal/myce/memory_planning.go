package myce

// Owned by one immutable planning pass, shared across its target machines.
// Never retained across placements, inventory changes, or transaction boundaries.
type memoryPlanningCache struct {
	observations *memoryObservationCatalog
	history      *memoryHistoryCache
	constrained  map[constrainedMemoryKey]float64
	similar      map[similarMemoryKey]similarMemoryResult
	cold         map[coldMemoryKey]coldMemoryResult
}

// Only retained execution evidence is shared with permanent-capacity views.
// Admission-dependent estimates stay in each view's own planning cache.
type memoryHistoryCache struct {
	jobs  map[memoryHistoryKey][]*Job
	lower map[memoryLowerKey]float64
}

// Retained constrained executions used to be scanned once per distinct work
// key, even with memoized answers. Index work/OS once in the same
// immutable pass. Borrow jobs only for that pass; reader-side freshness,
// ownership, qualification and exact-attempt fences remain authoritative.
func (c *memoryPlanningCache) retainedMemoryJobs(q *Queue, key memoryLowerKey) []*Job {
	if c.history.jobs == nil {
		c.history.jobs = map[memoryHistoryKey][]*Job{}
		for _, j := range q.memoryHistory().s.Jobs {
			if j.Timing == nil || j.Timing.Key == nil || j.StartedAt == nil || j.FinishedAt == nil || len(j.MemoryAttempts) == 0 {
				continue
			}
			k := j.Timing.Key
			id := memoryHistoryKey{k.Work, k.Target.OS}
			c.history.jobs[id] = append(c.history.jobs[id], j)
		}
	}
	return c.history.jobs[memoryHistoryKey{key.work.work, key.os}]
}

type memoryHistoryKey struct{ work, os string }

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
		history: &memoryHistoryCache{lower: map[memoryLowerKey]float64{}}, constrained: map[constrainedMemoryKey]float64{}, similar: map[similarMemoryKey]similarMemoryResult{}, cold: map[coldMemoryKey]coldMemoryResult{},
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
