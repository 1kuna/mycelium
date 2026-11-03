package myce

import (
	"context"
	"time"
)

// Every reader ignores throughput and memory evidence older than
// workCurveMaxAge, yet each distinct plain command kept its rows forever,
// persisted and scanned per memory candidate (#597). Maintenance deletes rows
// whose every clock is past that age, in batches small enough for one short
// commit per tick (as fileReclaimBatch). Two kinds are read at any age and
// stay: forbidden points, which sweeps never retry, and constrained memory
// peaks, which raise a first-run bound as invalidation provenance (#559).
const observationPruneBatch = 64

func expiredWorkObservation(p WorkObservation, now time.Time) bool {
	return !p.Forbidden && !p.MemoryPeak.constrained() && now.Sub(p.RecordedAt) > workCurveMaxAge && now.Sub(p.MemoryRecordedAt) > workCurveMaxAge
}

func expiredStorageObservation(p StorageObservation, now time.Time) bool {
	return now.Sub(p.RecordedAt) > workCurveMaxAge && now.Sub(p.OutputRecordedAt) > workCurveMaxAge && now.Sub(p.LowerBoundAt) > workCurveMaxAge
}

// Find candidates under the read lock only, then delete rechecked rows in a
// separate small update. The normal commit removes their SQLite rows.
func (q *Queue) pruneObservations(requestContexts ...context.Context) error {
	if q.simulation {
		return nil
	}
	ctx := requestContext(requestContexts)
	unlock := requestReadLock(ctx, &q.mu)
	now := q.clock.Now()
	var work, storage []string
	for id, p := range q.s.WorkObservations {
		if len(work) < observationPruneBatch && expiredWorkObservation(p, now) {
			work = append(work, id)
		}
	}
	for id, p := range q.s.StorageObservations {
		if len(storage) < observationPruneBatch && expiredStorageObservation(p, now) {
			storage = append(storage, id)
		}
	}
	unlock()
	if len(work)+len(storage) == 0 {
		return nil
	}
	return q.updateRequest(ctx, func() error {
		now := q.clock.Now()
		for _, id := range work {
			if p, ok := q.s.WorkObservations[id]; ok && expiredWorkObservation(p, now) {
				delete(q.s.WorkObservations, id)
				q.memoryObservationIndex = nil
			}
		}
		for _, id := range storage {
			if p, ok := q.s.StorageObservations[id]; ok && expiredStorageObservation(p, now) {
				delete(q.s.StorageObservations, id)
			}
		}
		return nil
	}, nil)
}
