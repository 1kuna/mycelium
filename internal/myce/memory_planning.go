package myce

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
	work     memoryWorkKey
	os       string
	discrete bool
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
		history: &memoryHistoryCache{lower: map[memoryLowerKey]float64{}}, constrained: map[constrainedMemoryKey]float64{},
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
