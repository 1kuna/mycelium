package myce

import (
	"math"
)

type CopyReport struct {
	Attempt        string  `json:"attempt,omitempty"`
	BytesPerSecond float64 `json:"bytes_per_second,omitempty"`
	Model          string  `json:"model"`
	State          string  `json:"state"`
	Bytes          int64   `json:"bytes"`
	Total          int64   `json:"total"`
	Error          string  `json:"error,omitempty"`
}

// Cold estimates use measured throughput, with a visible conservative 40 MB/s
// prior until the first copy. Cached copies have no NAS transfer time.
func (q *Queue) loadSeconds(model string, m *Machine) float64 {
	if contains(m.Models, model) || contains(m.CachedModels, model) {
		return 0
	}
	mm, ok := q.model(model)
	if !ok || mm.SizeBytes <= 0 {
		return 0
	}
	remaining := mm.SizeBytes
	for _, r := range m.CopyReports {
		if r.Model == model {
			remaining = max(0, remaining-min(mm.SizeBytes, r.Bytes))
		}
	}
	speed := m.CopyBytesPerSecond
	if speed <= 0 || math.IsNaN(speed) || math.IsInf(speed, 0) {
		speed = 40_000_000
	}
	return float64(remaining) / speed
}
func (q *Queue) workerIdle(m *Machine, demand *Job) bool {
	if q.launchCapacityBlocked(demand, m) || !q.batchSlotFree(m, demand) || q.untrackedPreparations(m) > 0 {
		return false
	}
	for _, l := range q.s.PreparationPlans {
		// A copy already serving this model demand remains useful preparation.
		if l.Kind == "copy" && l.Key == demand.Spec.Needs.Model {
			continue
		}
		if l.Machine == m.ID && l.Session == m.Session && l.Started && !q.preparationShared(m, demand, &l) {
			return false
		}
	}
	return true
}

// Preparation offers must continue while a build runs, even though that
// worker is busy for placement steering.
func (q *Queue) workerBatchIdle(m *Machine) bool {
	return q.workerBatchIdleExcept(m, nil)
}

// Recovery may adopt its own exact custody, never a sibling or older copy's.
func (q *Queue) workerBatchIdleExcept(m *Machine, own *HostRelease) bool {
	if m.Gone || m.Leaving || q.unreported[m.ID] {
		return false
	}
	for _, j := range q.liveJobs() {
		if own != nil && j.ID == own.Job && j.Machine == own.Machine && j.Session == own.Session && j.Attempt == own.Attempt {
			continue
		}
		if batchOnTarget(j, m) && batchReservesCapacity(j) {
			return false
		}
	}
	for _, r := range q.s.HostReleases {
		if own != nil && releaseKey(r) == releaseKey(*own) {
			continue
		}
		if q.batchReleaseOnTarget(r, m) {
			return false
		}
	}
	return true
}

// placementBetter applies the same rule to foreground placement and build plans.
func (q *Queue) placementBetter(j *Job, a, b *Machine, scans ...*placementScan) bool {
	return q.placementBetterWithTie(j, a, b, true, scans...)
}

func (q *Queue) placementBetterWithTie(j *Job, a, b *Machine, allowTie bool, scans ...*placementScan) bool {
	value, knownValue := q.valueCompare(a, b, scans...)
	if value != 0 {
		return value < 0
	}
	if j.Spec.Needs.cpuOnly() && (a.GPU == "cpu") != (b.GPU == "cpu") {
		return a.GPU == "cpu"
	}
	if a.batchTenants != b.batchTenants {
		return a.batchTenants < b.batchTenants // An idle card beats sharing a busy one (#538).
	}
	aa, bb := affinity(j, a), affinity(j, b)
	if aa != bb {
		return aa > bb
	}
	if len(j.Spec.Needs.Platform.Preferred) > 0 {
		ap, bp := contains(j.Spec.Needs.Platform.Preferred, a.GPU), contains(j.Spec.Needs.Platform.Preferred, b.GPU)
		if ap != bp {
			return ap
		}
	}
	if better, known := q.completionBetter(j, a, b, allowTie); known {
		return better
	}
	if modelLoaded(j, a) != modelLoaded(j, b) {
		return modelLoaded(j, a)
	}
	ac, bc := q.loadSeconds(j.Spec.Needs.Model, a)+environmentSeconds(j, a), q.loadSeconds(j.Spec.Needs.Model, b)+environmentSeconds(j, b)
	if ac != bc {
		return ac < bc
	}
	return allowTie && q.deviceUseTieBetter(a, b, knownValue && targetKey(a) < targetKey(b))
}

// Keep all target projections on the same qualified-work comparator.
func (q *Queue) targetPlacementBetter(j *Job, a, b *Machine, scans ...*placementScan) bool {
	if j.Spec.extensionJob() {
		return q.capabilityPlacementBetter(j, a, b, scans...)
	}
	return q.placementBetter(j, a, b, scans...)
}
