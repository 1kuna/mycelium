package myce

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"math/bits"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// RetentionCapacity measures the queue process's own host, never a worker's
// inventory. Available bytes already exclude other users of the machine.
// Unknown readings withhold collection; they never grant deletion authority.
type RetentionCapacity struct {
	MemoryAvailableBytes uint64 `json:"memory_available_bytes"`
	DiskAvailableBytes   uint64 `json:"disk_available_bytes"`
}

type EvidenceRetention struct {
	At             time.Time         `json:"at"`
	Capacity       RetentionCapacity `json:"capacity"`
	RetainedBytes  uint64            `json:"retained_bytes"`
	BudgetBytes    uint64            `json:"budget_bytes"`
	EligibleBytes  uint64            `json:"eligible_bytes"`
	ProtectedBytes uint64            `json:"protected_bytes"`
	ReclaimedBytes uint64            `json:"reclaimed_bytes"`
	DriftedRows    int               `json:"drifted_rows"`
	OverBudget     bool              `json:"over_budget"`
	Unavailable    bool              `json:"unavailable"`
}

type evidenceMaintenance struct {
	mu       sync.Mutex // Maintenance only; status never acquires this mutex.
	capacity func(context.Context) (RetentionCapacity, error)
	// Retained keys let a new capacity-pressure episode revisit unchanged
	// history one record at a time, without scanning/copying the queue map.
	history         []string
	historySeen     map[string]bool
	cursor          int
	revisiting      bool
	overBudget      bool
	expiry          evidenceExpiries
	plannedExternal map[string][32]byte
}

// One deadline per reporting host, updated in place. Check-in cadence cannot
// accumulate obsolete heap entries. This index belongs only to maintenance.
type evidenceExpiry struct {
	key   string
	at    time.Time
	index int
}
type evidenceExpiries struct {
	rows  []*evidenceExpiry
	byKey map[string]*evidenceExpiry
}

func (e evidenceExpiries) Len() int           { return len(e.rows) }
func (e evidenceExpiries) Less(i, j int) bool { return e.rows[i].at.Before(e.rows[j].at) }
func (e evidenceExpiries) Swap(i, j int) {
	e.rows[i], e.rows[j] = e.rows[j], e.rows[i]
	e.rows[i].index = i
	e.rows[j].index = j
}
func (e *evidenceExpiries) Push(v any) {
	row := v.(*evidenceExpiry)
	row.index = len(e.rows)
	e.rows = append(e.rows, row)
}
func (e *evidenceExpiries) Pop() any {
	n := len(e.rows) - 1
	row := e.rows[n]
	e.rows[n] = nil
	e.rows = e.rows[:n]
	delete(e.byKey, row.key)
	return row
}
func (e *evidenceExpiries) set(key string, at time.Time) {
	if row := e.byKey[key]; row != nil {
		if at.IsZero() {
			heap.Remove(e, row.index)
		} else {
			row.at = at
			heap.Fix(e, row.index)
		}
		return
	}
	if !at.IsZero() {
		if e.byKey == nil {
			e.byKey = map[string]*evidenceExpiry{}
		}
		row := &evidenceExpiry{key: key, at: at}
		e.byKey[key] = row
		heap.Push(e, row)
	}
}

// WithRetentionCapacity supplies local measurements, including deterministic
// laptop fixtures. It does not change retention or confer release authority.
func WithRetentionCapacity(read func(context.Context) (RetentionCapacity, error)) QueueOption {
	return func(q *Queue) { q.evidenceMaintenance.capacity = read }
}

var queueHostMemorySource = NewHostMemorySource

// SetQueueHostMemorySource supplies the queue process's own host-memory probe
// during embedded process setup. It must not run concurrently with queue reads.
// Worker inventory overrides do not change this source.
func SetQueueHostMemorySource(source func(float64) (MemorySource, error)) {
	queueHostMemorySource = source
}

func queueRetentionCapacity(ctx context.Context, dir string) (RetentionCapacity, error) {
	source, err := queueHostMemorySource(0)
	if err != nil {
		return RetentionCapacity{}, err
	}
	gb, err := source(ctx)
	if err != nil || !validMemoryValue(gb) {
		return RetentionCapacity{}, errors.New("queue memory capacity unavailable")
	}
	disk, err := diskFree(dir)
	if err != nil || disk < 0 {
		return RetentionCapacity{}, errors.New("queue disk capacity unavailable")
	}
	return RetentionCapacity{MemoryAvailableBytes: uint64(math.Min(gb*(1<<30), math.MaxUint64)), DiskAvailableBytes: uint64(disk)}, nil
}

// The budget buys a detached diagnostic generation and a durable rewrite of
// retained evidence. Each requires one additional copy of the measured state.
// Subtract the larger live heap from available RAM too: a cold/unknown heap is
// never replaced with an installation-sized reserve or a percentage of RAM.
// The target is soft. Custody, unique data and live learning can exceed it.
func evidenceBudget(capacity RetentionCapacity, stateBytes, heapBytes uint64) uint64 {
	memoryReserve := max(stateBytes, heapBytes)
	memory := capacity.MemoryAvailableBytes - min(capacity.MemoryAvailableBytes, memoryReserve)
	disk := capacity.DiskAvailableBytes - min(capacity.DiskAvailableBytes, stateBytes)
	return min(memory, disk)
}

// Incremental candidate intake follows committed immutable record changes.
// It never scans/decodes the complete retained job or custody history per Tick.
func (q *Queue) enqueueEvidence(key string) {
	if q.evidencePending == nil {
		q.evidencePending = map[string]bool{}
	}
	if !q.evidencePending[key] {
		q.evidencePending[key] = true
		q.evidenceRows = append(q.evidenceRows, key)
	}
}
func (q *Queue) noteEvidenceRecord(key string, old, current []byte) {
	if bytes.Equal(old, current) {
		return
	}
	q.evidenceBytes -= min(q.evidenceBytes, uint64(len(old)))
	q.evidenceBytes = addDiskBytes(q.evidenceBytes, uint64(len(current)))
	if strings.HasPrefix(key, "jobs/") || strings.HasPrefix(key, "work_observations/") || strings.HasPrefix(key, "external_memory/") {
		q.enqueueEvidence(key)
	}
}
func (q *Queue) evidencePressure() (bool, time.Duration) {
	p := q.mu.pressure.Load()
	if p == nil {
		return false, 0
	}
	busy, _, _ := p.measurement()
	p.mu.Lock()
	service := time.Duration(p.baseline)
	p.mu.Unlock()
	return busy, service
}

func (q *Queue) maintainEvidence(ctx context.Context) error {
	m := &q.evidenceMaintenance
	if q.simulation || !m.mu.TryLock() {
		return nil
	}
	defer m.mu.Unlock()
	if busy, _ := q.evidencePressure(); busy {
		return nil
	}
	// Host syscalls run before acquiring control. Capacity changes alone refresh
	// accounting, even when no evidence row changed.
	read := m.capacity
	var capacity RetentionCapacity
	var err error
	if read != nil {
		capacity, err = read(ctx)
	} else {
		capacity, err = queueRetentionCapacity(ctx, filepath.Dir(q.lock.Name()))
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	unlock := requestReadLock(ctx, &q.mu)
	now := q.clock.Now()
	report := EvidenceRetention{At: now, Capacity: capacity, Unavailable: err != nil, RetainedBytes: q.evidenceBytes}
	report.BudgetBytes = evidenceBudget(capacity, q.evidenceBytes, mem.HeapAlloc)
	unlock()
	overBudget := err == nil && report.RetainedBytes > report.BudgetBytes
	if overBudget && !m.overBudget {
		m.cursor, m.revisiting = 0, true
	}
	m.overBudget = overBudget
	started := q.clock.Now()
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		busy, service := q.evidencePressure()
		if busy {
			break
		}
		unlock = requestLock(ctx, &q.mu)
		var key string
		revisit := false
		due := err == nil && m.expiry.Len() > 0 && !m.expiry.rows[0].at.After(now)
		if due {
			key = m.expiry.rows[0].key
		} else if len(q.evidenceRows) > 0 {
			key = q.evidenceRows[0]
			q.evidenceRows = q.evidenceRows[1:]
			if len(q.evidenceRows) == 0 {
				q.evidenceRows = nil
			}
			delete(q.evidencePending, key)
		} else if overBudget && m.revisiting && m.cursor < len(m.history) {
			key = m.history[m.cursor]
			m.cursor++
			revisit = true
		} else {
			m.revisiting = false
			unlock()
			break
		}
		data := q.persisted[key]
		revision := q.fileReferenceRevision
		open := len(q.records.live) > 0 || len(q.calls) > 0
		id := strings.TrimPrefix(key, "work_observations/")
		pinned := q.evidenceLearned[id]
		hostCount := max(1, len(q.s.ExternalMemory))
		unlock()
		if (strings.HasPrefix(key, "jobs/") || strings.HasPrefix(key, "external_memory/")) && !m.historySeen[key] {
			if m.historySeen == nil {
				m.historySeen = map[string]bool{}
			}
			m.historySeen[key] = true
			m.history = append(m.history, key)
		}
		var apply func()
		var replacement []byte
		completed := true
		external := strings.HasPrefix(key, "external_memory/") && err == nil
		fingerprint, alreadyPlanned := m.plannedExternal[key]
		drift := false
		deferWork := false
		switch {
		case len(data) == 0: // Removed independently; nothing to decode.
		case external && !due && !revisit && alreadyPlanned && fingerprint == sha256.Sum256(data):
			// A maintenance rewrite is already planned. Its commit notification
			// must not feed budget/slot changes back into another rewrite.
		case strings.HasPrefix(key, "work_observations/"):
			var point WorkObservation
			if e := json.Unmarshal(data, &point); e != nil {
				return e
			}
			if expiredWorkObservation(point, now) && !pinned {
				if open {
					deferWork = true
				} else {
					apply = func() { delete(q.s.WorkObservations, id); q.memoryObservationIndex = nil }
					drift = true
				}
			}
		case strings.HasPrefix(key, "jobs/") && err == nil && report.RetainedBytes > report.BudgetBytes:
			var job Job
			if e := json.Unmarshal(data, &job); e != nil {
				return e
			}
			if !mutableJob(&job) {
				compactRedundantAttemptEvidence(&job)
				replacement, _ = json.Marshal(job)
				if len(replacement) < len(data) {
					apply = func() { q.touchJob(job.ID).AttemptHistory = job.AttemptHistory }
				}
			}
		case strings.HasPrefix(key, "external_memory/") && err == nil:
			var ring externalMemoryRing
			if e := json.Unmarshal(data, &ring); e != nil {
				return e
			}
			// Allocate each reporting host an equal share of measured headroom. A
			// serialization quantum also prevents a fast producer from enlarging the
			// control path beyond one measured ordinary transaction's service.
			budget := report.BudgetBytes / uint64(hostCount)
			encodeStart := q.clock.Now()
			encoded, _ := json.Marshal(ring)
			encodeCost := q.clock.Now().Sub(encodeStart)
			if service > 0 && encodeCost > 0 {
				budget = min(budget, uint64(float64(len(encoded))*float64(service)/float64(encodeCost)))
			}
			if compactExternalEvidence(&ring, now, budget) {
				host := strings.TrimPrefix(key, "external_memory/")
				replacement, _ = json.Marshal(ring)
				apply = func() { q.s.ExternalMemory[host] = &ring }
			}
			var next time.Time
			for _, bucket := range ring.Buckets {
				// The reader includes the exact boundary sample. One duration
				// quantum later no public window can still read that bucket.
				at := maxTime(bucket.Start, bucket.End).Add(7*24*time.Hour + time.Nanosecond)
				if next.IsZero() || at.Before(next) {
					next = at
				}
			}
			m.expiry.set(key, next)
		}
		if deferWork {
			unlock = requestLock(ctx, &q.mu)
			if q.evidenceDeferred == nil {
				q.evidenceDeferred = map[string]bool{}
			}
			q.evidenceDeferred[key] = true
			unlock()
		}
		if apply != nil {
			applied := false
			if e := q.updateRequest(ctx, func() error {
				if q.fileReferenceRevision != revision {
					q.enqueueEvidence(key)
					return nil
				}
				apply()
				applied = true
				return nil
			}, nil); e != nil {
				unlock = requestLock(ctx, &q.mu)
				q.enqueueEvidence(key)
				unlock()
				return e
			}
			if applied {
				report.ReclaimedBytes = addDiskBytes(report.ReclaimedBytes, uint64(max(0, len(data)-len(replacement))))
				data = replacement
				if drift {
					report.DriftedRows++
				}
			} else {
				completed = false
				if external {
					m.expiry.set(key, now)
				}
			}
		}
		if external && completed {
			if m.plannedExternal == nil {
				m.plannedExternal = map[string][32]byte{}
			}
			m.plannedExternal[key] = sha256.Sum256(data)
		}
		// An individual record is the indivisible progress unit. Yield after the
		// measured service quantum; cold installations do one record to learn cost.
		if service == 0 || q.clock.Now().Sub(started) >= service {
			break
		}
	}
	unlock = requestLock(ctx, &q.mu)
	report.RetainedBytes = q.evidenceBytes
	report.ProtectedBytes = report.RetainedBytes
	report.OverBudget = !report.Unavailable && report.RetainedBytes > report.BudgetBytes
	q.evidenceRetention = &report
	unlock()
	return nil
}

// A repeated observation has lower diagnostic value than a distinct one.
// Preserve first/latest provenance, every distinct attempt identity and value,
// and every custody/learning record. Only byte-identical duplicate rows can go.
func compactRedundantAttemptEvidence(j *Job) {
	seen := map[string]bool{}
	keep := make([]AttemptOutcome, 0, len(j.AttemptHistory))
	for i, row := range j.AttemptHistory {
		value := row
		data, _ := json.Marshal(value)
		key := string(data)
		if i == 0 || i == len(j.AttemptHistory)-1 || !seen[key] {
			keep = append(keep, row)
		}
		seen[key] = true
	}
	j.AttemptHistory = keep
}

// Capture has two simultaneous byte copies (raw input and encoded publication).
// This physical copy count bounds optional diagnostic allocation from measured
// local headroom. Missing capacity captures no extra bytes; source files remain.
func diagnosticCapacityBytes(dir string) int64 {
	capacity, err := queueRetentionCapacity(context.Background(), dir)
	if err != nil {
		return 0
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return int64(min(uint64(math.MaxInt64-1), evidenceBudget(capacity, 0, mem.HeapAlloc)/2))
}

// Adjacent distribution summaries can be coalesced without losing counts or
// maxima. Reader windows are public query semantics, not retention defaults.
// When capacity forces coarser coverage, first_bucket_at exposes its boundary.
func compactExternalEvidence(r *externalMemoryRing, now time.Time, budget uint64) bool {
	before, _ := json.Marshal(r)
	// Physical histogram footprint is measurable from the existing encoding.
	bucketBytes := max(uint64(1), uint64(len(before))/uint64(max(1, len(r.Buckets))))
	slots := max(1, int(min(uint64(math.MaxInt-1), budget/bucketBytes)))
	r.Slots = slots
	width := externalBucketSpan(slots)
	buckets := map[int]*externalMemoryBucket{}
	for _, b := range r.Buckets {
		if maxTime(b.Start, b.End).Before(now.Add(-7 * 24 * time.Hour)) {
			continue
		}
		start := b.Start.Truncate(width)
		index := int(start.UnixNano() / int64(width) % int64(slots+1))
		buckets[index] = mergeExternalMemoryBucket(buckets[index], b)
		buckets[index].Start = start
	}
	r.Buckets = buckets
	after, _ := json.Marshal(r)
	return !bytes.Equal(before, after)
}

// Merge immutable histogram generations, including separately qualified idle facts.
func mergeExternalMemoryBucket(a, b *externalMemoryBucket) *externalMemoryBucket {
	if b == nil {
		return a
	}
	if a == nil {
		copy := *b
		copy.Counts, copy.Values = maps.Clone(b.Counts), maps.Clone(b.Values)
		copy.ByteCounts, copy.ByteMax = maps.Clone(b.ByteCounts), maps.Clone(b.ByteMax)
		copy.ByteMin = maps.Clone(b.ByteMin)
		copy.Quiescent = mergeExternalMemoryBucket(nil, b.Quiescent)
		return &copy
	}
	a.Start = minTime(a.Start, b.Start)
	a.End = maxTime(maxTime(a.End, a.Start), maxTime(b.End, b.Start))
	a.Windows += b.Windows
	a.Quiescent = mergeExternalMemoryBucket(a.Quiescent, b.Quiescent)
	if a.ByteCounts == nil {
		a.ByteCounts, a.ByteMax = map[uint8]uint64{}, map[uint8]float64{}
	}
	for bin, n := range b.ByteCounts {
		if lower, ok := b.ByteMin[bin]; ok {
			if a.ByteMin == nil {
				a.ByteMin = map[uint8]float64{}
			}
			if prior, exists := a.ByteMin[bin]; !exists || lower < prior {
				a.ByteMin[bin] = lower
			}
		}
		a.ByteCounts[bin] += n
		a.ByteMax[bin] = max(a.ByteMax[bin], b.ByteMax[bin])
	}
	for bin, n := range b.Counts {
		value := externalBinUpper(int(bin))
		byteBin := uint8(bits.Len64(uint64(math.Ceil(value * (1 << 30)))))
		a.ByteCounts[byteBin] += uint64(n)
		a.ByteMax[byteBin] = max(a.ByteMax[byteBin], value)
	}
	for encoded, n := range b.Values {
		value := math.Float64frombits(encoded)
		byteBin := uint8(bits.Len64(uint64(math.Ceil(value * (1 << 30)))))
		a.ByteCounts[byteBin] += n
		a.ByteMax[byteBin] = max(a.ByteMax[byteBin], value)
	}
	if b.MaxGB > a.MaxGB {
		a.MaxGB, a.PeakAt = b.MaxGB, b.PeakAt
	}
	return a
}
