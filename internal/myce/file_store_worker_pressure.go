package myce

import (
	"context"
	"errors"
	"maps"
	"time"
)

// Keep the most recent physically refused write as demand evidence. Attempt
// and input-claim owners can be checked again. A preflight with no durable
// owner contributes to one fresh, co-located pressure measurement.
type blockedFileStoreWrite struct {
	at         time.Time
	files      FileManifest
	attempt    fileStoreAttempt
	claim      string
	submission Spec
	exclude    string
}

func (s *FileStore) noteBlockedWrite(ctx context.Context, files FileManifest, exclude string, err error) {
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Cause == nil {
		return
	}
	cause := refusal.Cause
	e := cause.FileCapacity
	if (cause.Predicate != fileCapacityBacking && cause.Predicate != fileCapacityProduction) ||
		e == nil || e.Bound != "physical_volume" || e.Production || e.GrowthBytes == 0 {
		return
	}
	attempt, _ := ctx.Value(fileStoreAttemptKey{}).(fileStoreAttempt)
	claim, _ := ctx.Value(fileInputClaimKey{}).(string)
	submission, _ := ctx.Value(fileStoreSubmissionKey{}).(Spec)
	if attempt.ID == "" && submission.Project == "" {
		return
	}
	demand := &blockedFileStoreWrite{at: e.ObservedAt, files: maps.Clone(files), attempt: attempt, claim: claim, submission: submission, exclude: exclude}
	for old := s.pressure.Load(); old == nil || !old.at.After(demand.at); old = s.pressure.Load() {
		if s.pressure.CompareAndSwap(old, demand) {
			return
		}
	}
}

type workerFileStorePressure struct {
	TargetFreeBytes uint64 `json:"target_free_bytes"`
	ShortfallBytes  uint64 `json:"shortfall_bytes"`
}

func (q *Queue) localFileStoreWorker(machine, session string) bool {
	m := q.s.Machines[machine]
	return m != nil && m.Session == session && session != "" && !m.Gone && q.diskHost != "" && hostID(m) == q.diskHost
}

// Check the registered worker and exact filesystem before exposing pressure.
// Capacity inventory and manifest projection happen outside q.mu.
func (q *Queue) localFileStorePressure(ctx context.Context, machine, session, volume string) workerFileStorePressure {
	if volume == "" {
		return workerFileStorePressure{}
	}
	unlock := requestReadLock(ctx, &q.mu)
	store, host := q.files, q.diskHost
	local := q.localFileStoreWorker(machine, session)
	unlock()
	if !local || store == nil || store.productionReserve == 0 || !store.lifecycle.TryRLock() {
		return workerFileStorePressure{}
	}
	defer store.lifecycle.RUnlock()
	status, payloads, err := store.capacity(ctx)
	if err != nil || status.diskVolume == "" || volume != diskVolumeID(host, status.diskVolume) {
		return workerFileStorePressure{}
	}

	var growth uint64
	var projected map[string]storedPayload
	demand := store.pressure.Load()
	if demand != nil {
		projected = maps.Clone(payloads)
		for _, ref := range demand.files {
			size := uint64(ref.SizeBytes)
			old := projected[ref.SHA256].length
			growth = addDiskBytes(growth, size-min(size, old))
			projected[ref.SHA256] = storedPayload{length: max(size, old)}
		}
	}

	unlock = requestReadLock(ctx, &q.mu)
	defer unlock()
	if q.files != store || q.diskHost != host || !q.localFileStoreWorker(machine, session) {
		return workerFileStorePressure{}
	}
	need := q.storeNeedWithReserve(payloads, 0, "")
	if demand != nil && growth > 0 && q.blockedFileStoreWriteStillOrdinary(ctx, demand, growth) {
		// A preflight without a durable owner can authorize a single bounded
		// trim. A new refusal supplies fresh evidence if the submitter returns.
		if demand.attempt.ID != "" || demand.claim != "" || store.pressure.CompareAndSwap(demand, nil) {
			separateWrite := q.storeNeedWithReserve(projected, growth, demand.exclude)
			if separateWrite <= status.allowanceFree {
				need = max(need, separateWrite)
			}
		}
	}
	if need > status.allowanceFree || need <= status.volumeFree {
		return workerFileStorePressure{}
	}
	target := addDiskBytes(addDiskBytes(status.FloorBytes, status.uploadRemaining), need)
	if target > status.diskTotal || target <= status.DiskFreeBytes {
		return workerFileStorePressure{}
	}
	return workerFileStorePressure{TargetFreeBytes: target, ShortfallBytes: target - status.DiskFreeBytes}
}

// q.mu held. Accepted reservations and the remaining reserve share the same
// accounting as upload admission. Tus lengths are present in payloads.
func (q *Queue) storeNeedWithReserve(payloads map[string]storedPayload, growth uint64, exclude string) uint64 {
	reserved := q.outputReservationsFor(payloads, exclude, true)
	reserve := uint64(q.fileStoreProductionReserve)
	return addDiskBytes(addDiskBytes(growth, q.outputReservations(payloads, exclude)), reserve-min(reserve, reserved))
}

func (q *Queue) blockedFileStoreWriteStillOrdinary(ctx context.Context, demand *blockedFileStoreWrite, growth uint64) bool {
	if demand.attempt.ID != "" {
		ctx = context.WithValue(ctx, fileStoreAttemptKey{}, demand.attempt)
	} else {
		if demand.claim != "" {
			claim, ok := q.s.FileInputClaims[demand.claim]
			if !ok || !claim.SettledAt.IsZero() {
				return false
			}
		}
		ctx = context.WithValue(ctx, fileStoreSubmissionKey{}, demand.submission)
	}
	production, identified, err := q.fileStoreWriteClass(ctx, demand.files, growth)
	return err == nil && identified && !production
}
