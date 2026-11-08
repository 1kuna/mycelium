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
	extensions map[string][]*WorkObservation
	targets    map[memoryTransferTarget]map[memoryWorkKey][]*WorkObservation
	work       map[memoryWorkKey][]*WorkObservation
	family     map[similarMemoryKey][]*WorkObservation
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
		index = &memoryObservationCatalog{targets: map[memoryTransferTarget]map[memoryWorkKey][]*WorkObservation{}, work: map[memoryWorkKey][]*WorkObservation{}, family: map[similarMemoryKey][]*WorkObservation{}}
		index.extensions = map[string][]*WorkObservation{}
		for _, point := range q.s.WorkObservations {
			index.extensions[point.Key.ExtensionDigest] = append(index.extensions[point.Key.ExtensionDigest], &point)
			if point.MemoryConfiguration != "" {
				key := memoryWorkIdentity(point.Key, point.MemoryConfiguration)
				index.work[key] = append(index.work[key], &point)
				target := transferTarget(point.Key, point.MemoryLayout)
				if index.targets[target] == nil {
					index.targets[target] = map[memoryWorkKey][]*WorkObservation{}
				}
				index.targets[target][key] = append(index.targets[target][key], &point)
			}
			layout := point.MemoryLayout
			if point.Key.Component == "cpu" {
				layout = ""
			}
			key := similarMemoryKey{point.MemoryFamily, point.Key.Target.OS, point.Key.Component != "cpu", layout}
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
	q.evidenceGeneration++
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	q.s.WorkObservations[id] = point
	q.memoryObservationIndex = nil
}
