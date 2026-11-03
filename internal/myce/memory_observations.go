package myce

// This is exactly sameMemoryWork's comparability boundary. Host, target and
// freshness remain reader-side fences: cross-host transfer and constrained
// lower bounds must still see every comparable observation.
type memoryWorkKey struct {
	work, extension, configuration string
	reference                      WorkReference
	concurrency, batchSize         int
	check                          bool
}

func memoryWorkIdentity(k ObservationKey, configuration string) memoryWorkKey {
	return memoryWorkKey{k.Work, k.ExtensionDigest, configuration, k.Reference, k.Concurrency, k.BatchSize, k.Check}
}

// Build once per transaction, copying each aggregate only once. Readers borrow
// immutable pointers instead of copying/scanning all curves for every queued
// job and every target. Ordinary read-only callers get a fresh local index.
func (q *Queue) memoryObservations(k ObservationKey, configuration string) []*WorkObservation {
	index := q.memoryObservationIndex
	if index == nil {
		index = map[memoryWorkKey][]*WorkObservation{}
		for _, point := range q.s.WorkObservations {
			if point.MemoryConfiguration == "" {
				continue
			}
			key := memoryWorkIdentity(point.Key, point.MemoryConfiguration)
			index[key] = append(index[key], &point)
		}
		if q.records != nil && q.records.updating {
			q.memoryObservationIndex = index
		}
	}
	return index[memoryWorkIdentity(k, configuration)]
}

func (q *Queue) putWorkObservation(id string, point WorkObservation) {
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	q.s.WorkObservations[id] = point
	q.memoryObservationIndex = nil
}
