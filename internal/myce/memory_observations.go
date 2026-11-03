package myce

// Exactly sameMemoryWork's comparability boundary. Target, host, identity and
// freshness remain reader-side fences.
type memoryWorkKey struct {
	work, extension, configuration string
	reference                      WorkReference
	concurrency, batchSize         int
	check                          bool
}

func memoryWorkIdentity(k ObservationKey, configuration string) memoryWorkKey {
	return memoryWorkKey{k.Work, k.ExtensionDigest, configuration, k.Reference, k.Concurrency, k.BatchSize, k.Check}
}

type memoryObservationCatalog struct {
	work   map[memoryWorkKey][]*WorkObservation
	family map[similarMemoryKey][]*WorkObservation
}

// Copy each aggregate once, then lend immutable pointers to both readers. Only
// a transaction or an immutable planning pass retains the index. Qualification
// capacity views share the origin's observations, not its admission state.
func (q *Queue) memoryObservationCatalog() *memoryObservationCatalog {
	if q.memoryOrigin != nil {
		return q.memoryOrigin.memoryObservationCatalog()
	}
	index := q.memoryObservationIndex
	if index == nil && q.memoryPlanning != nil {
		index = q.memoryPlanning.observations
	}
	if index == nil {
		index = &memoryObservationCatalog{work: map[memoryWorkKey][]*WorkObservation{}, family: map[similarMemoryKey][]*WorkObservation{}}
		recovered := q.recoveredMemoryObservations()
		for _, point := range q.s.WorkObservations {
			if recovery, ok := recovered[point.Key]; ok {
				point = recovery.point
			}
			if point.MemoryConfiguration != "" {
				key := memoryWorkIdentity(point.Key, point.MemoryConfiguration)
				index.work[key] = append(index.work[key], &point)
			}
			key := similarMemoryKey{point.MemoryFamily, point.Key.Target.OS, point.Key.Component != "cpu"}
			index.family[key] = append(index.family[key], &point)
		}
		if q.records != nil && q.records.updating {
			q.memoryObservationIndex = index
		}
		if q.memoryPlanning != nil {
			q.memoryPlanning.observations = index
		}
	}
	return index
}
func (q *Queue) memoryObservations(k ObservationKey, configuration string) []*WorkObservation {
	return q.memoryObservationCatalog().work[memoryWorkIdentity(k, configuration)]
}
func (q *Queue) putWorkObservation(id string, point WorkObservation) {
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	q.s.WorkObservations[id] = point
	q.memoryObservationIndex = nil
}
