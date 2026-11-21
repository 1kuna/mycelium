package myce

import (
	"context"
	"errors"
	"maps"
	"strings"
	"time"
)

// A smaller retry must not displace a larger blocked write before hygiene
// sees it. Revalidate against current allocations and ownership at the poll.
// An unowned preflight expires after 15 minutes. A compiler pin held up
// relief for about 14 minutes in #1921, across multiple hygiene polls.
const unownedFileStoreDemandWindow = 15 * time.Minute

type blockedFileStoreWrite struct {
	at         time.Time
	growth     uint64
	files      FileManifest
	attempt    fileStoreAttempt
	claim      string
	submission Spec
	exclude    string
	volume     string
}

func (d *blockedFileStoreWrite) expired(now time.Time) bool {
	return d.attempt.ID == "" && d.claim == "" && !now.Before(d.at.Add(unownedFileStoreDemandWindow))
}

func (s *FileStore) noteBlockedWrite(ctx context.Context, files FileManifest, exclude, volume string, err error) {
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Cause == nil {
		return
	}
	cause := refusal.Cause
	e := cause.FileCapacity
	if (cause.Predicate != fileCapacityBacking && cause.Predicate != fileCapacityProduction) ||
		e == nil || e.Bound != "physical_volume" || e.Production || e.GrowthBytes == 0 || volume == "" {
		return
	}
	attempt, _ := ctx.Value(fileStoreAttemptKey{}).(fileStoreAttempt)
	claim, _ := ctx.Value(fileInputClaimKey{}).(string)
	submission, _ := ctx.Value(fileStoreSubmissionKey{}).(Spec)
	if attempt.ID == "" && submission.Project == "" {
		return
	}
	demand := &blockedFileStoreWrite{at: e.ObservedAt, growth: e.GrowthBytes, files: maps.Clone(files), attempt: attempt, claim: claim, submission: submission, exclude: exclude, volume: volume}
	for {
		old := s.pressure.Load()
		if old != nil {
			newer := old.at.After(demand.at)
			if old.volume != volume && newer {
				return // A late refusal from a different mount cannot displace the new mount.
			}
			if old.volume == volume && !old.expired(demand.at) && (old.growth > demand.growth || old.growth == demand.growth && newer) {
				return
			}
		}
		if s.pressure.CompareAndSwap(old, demand) {
			return
		}
	}
}

type workerFileStorePressure struct {
	TargetFreeBytes uint64 `json:"target_free_bytes"`
	ShortfallBytes  uint64 `json:"shortfall_bytes"`
	BlockedWrite    bool   `json:"blocked_write,omitempty"`
}

func localFileStoreWorker(base *state, machine, session, host string) bool {
	m := base.Machines[machine]
	return m != nil && m.Session == session && session != "" && !m.Gone && host != "" && hostID(m) == host
}

func (p *publishedReadIndex) diskDemandBase() (*state, error) {
	if p == nil {
		return nil, errors.New("queue read state unavailable")
	}
	record := readGet(p.root, "state")
	if record == nil {
		return nil, errors.New("queue read state unavailable")
	}
	value, err := record.decode("state")
	if err != nil {
		return nil, err
	}
	return value.(*state), nil
}

// Load the committed jobs off-lock. Worker-local placement also needs the
// learned storage records, while file-store relief only needs live jobs.
func (p *publishedReadIndex) diskDemandQueue(ctx context.Context, base *state, clock Clock, placement bool) (*Queue, error) {
	s := *base
	s.Jobs = map[string]*Job{}
	var err error
	readWalk(p.root, true, func(key string, record *readRecord) {
		id, ok := strings.CutPrefix(key, "jobs/")
		if !ok || err != nil {
			return
		}
		var value any
		value, err = record.decode(key)
		if err == nil {
			s.Jobs[id] = value.(*Job)
		}
	})
	if placement {
		s.StorageObservations = map[string]StorageObservation{}
		readWalkPrefix(p.root, "storage_observations/", func(key string, record *readRecord) {
			if err != nil {
				return
			}
			var value any
			value, err = record.decode(key)
			if err == nil {
				s.StorageObservations[strings.TrimPrefix(key, "storage_observations/")] = *value.(*StorageObservation)
			}
		})
	}
	if err != nil {
		return nil, err
	}
	return &Queue{clock: clock, s: s, calls: p.calls, models: *p.capture.models}, ctx.Err()
}

// Check the registered worker and exact filesystem before exposing pressure.
// Capacity inventory and manifest projection happen outside q.mu.
func (q *Queue) localFileStorePressure(ctx context.Context, machine, session, volume string) workerFileStorePressure {
	if volume == "" {
		return workerFileStorePressure{}
	}
	index := q.publishedReads.Load()
	base, err := index.diskDemandBase()
	if err != nil {
		return workerFileStorePressure{}
	}
	store, host := index.capture.source.files, index.capture.source.diskHost
	if !localFileStoreWorker(base, machine, session, host) || store == nil || store.productionReserve == 0 || !store.lifecycle.TryRLock() {
		return workerFileStorePressure{}
	}
	defer store.lifecycle.RUnlock()
	demand := store.pressure.Load()
	status, payloads, err := store.capacity(ctx)
	if err != nil || status.diskVolume == "" || volume != diskVolumeID(host, status.diskVolume) {
		return workerFileStorePressure{}
	}

	// Use the latest committed reservations after inventory, and discard
	// advice if a new relevant publication crosses the projection.
	index = q.publishedReads.Load()
	base, err = index.diskDemandBase()
	if err != nil || index.capture.source.files != store || index.capture.source.diskHost != host ||
		!localFileStoreWorker(base, machine, session, host) {
		return workerFileStorePressure{}
	}
	source, err := index.diskDemandQueue(ctx, base, q.clock, false)
	if err != nil {
		return workerFileStorePressure{}
	}
	need := source.storeNeedWithReserve(payloads, 0, "", store.productionReserve)
	var writeNeed uint64
	if demand != nil && demand.volume == status.diskVolume && !demand.expired(q.clock.Now()) {
		var growth uint64
		projected := maps.Clone(payloads)
		for _, ref := range demand.files {
			size := uint64(ref.SizeBytes)
			old := projected[ref.SHA256].length
			growth = addDiskBytes(growth, size-min(size, old))
			projected[ref.SHA256] = storedPayload{length: max(size, old)}
		}
		if growth > 0 && source.blockedFileStoreWriteStillOrdinary(ctx, demand, growth) {
			writeNeed = source.storeNeedWithReserve(projected, growth, demand.exclude, store.productionReserve)
		}
	}
	latest := q.publishedReads.Load()
	if latest.root != index.root || latest.capture.source.files != store || latest.capture.source.diskHost != host ||
		store.pressure.Load() != demand || ctx.Err() != nil {
		return workerFileStorePressure{}
	}
	unavailable := addDiskBytes(status.FloorBytes, status.uploadRemaining)
	maxVolumeFree := status.diskTotal - min(status.diskTotal, unavailable)
	blocked := writeNeed > status.volumeFree && writeNeed <= min(status.allowanceFree, maxVolumeFree)
	if blocked {
		need = max(need, writeNeed)
	} else if demand != nil && !store.pressure.CompareAndSwap(demand, nil) {
		return workerFileStorePressure{}
	}
	if need > status.allowanceFree || need <= status.volumeFree {
		return workerFileStorePressure{}
	}
	target := addDiskBytes(unavailable, need)
	if target > status.diskTotal || target <= status.DiskFreeBytes {
		return workerFileStorePressure{}
	}
	return workerFileStorePressure{TargetFreeBytes: target, ShortfallBytes: target - status.DiskFreeBytes, BlockedWrite: blocked}
}

// On an immutable queue view, use the same reservation accounting as upload
// admission. Tus lengths are present in payloads.
func (q *Queue) storeNeedWithReserve(payloads map[string]storedPayload, growth uint64, exclude string, reserve uint64) uint64 {
	reserved := q.outputReservationsFor(payloads, exclude, true)
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
