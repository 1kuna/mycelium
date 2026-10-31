package myce

import (
	"fmt"
	"sort"
	"time"
)

// WorkLearningChange binds the exact accepted mutation, without duplicating the
// learning algorithm or publishing its worker payload. Stored in the job ledger
// in the same transaction as the curve and the accepted completion.
type WorkLearningChange struct {
	Key    ObservationKey `json:"key"`
	Before string         `json:"before"`
	After  string         `json:"after"`
}

func curveMeasurementDigest(p WorkObservation) string {
	p.StaleReason = "" // Availability/identity labels are not persisted measurements.
	return identityHash(p)
}

func (q *Queue) recordCurveLearning(j *Job, old WorkObservation, exists bool, point WorkObservation) {
	before := ""
	if exists {
		before = curveMeasurementDigest(old)
	}
	e := ledgerAttempt(j, "work_learning", "accepted_sample")
	e.WorkLearning = &WorkLearningChange{Key: point.Key, Before: before, After: curveMeasurementDigest(point)}
	q.appendJobLedger(j, e)
}

type CurvePreservationEvidence struct {
	Job      string    `json:"job"`
	Attempt  int       `json:"attempt"`
	Sequence int       `json:"sequence"`
	At       time.Time `json:"at"`
}

type CurvePreservationDiff struct {
	Key           ObservationKey              `json:"key"`
	SamplesBefore int                         `json:"samples_before"`
	SamplesAfter  int                         `json:"samples_after"`
	Evidence      []CurvePreservationEvidence `json:"evidence"`
}

type CurvePreservationReceipt struct {
	Failure    string                  `json:"failure,omitempty"`
	FailedKey  *ObservationKey         `json:"failed_key,omitempty"`
	KeysBefore int                     `json:"keys_before"`
	KeysAfter  int                     `json:"keys_after"`
	Changes    []CurvePreservationDiff `json:"changes"`
}

// VerifyCurvePreservation requires authenticated inspection snapshots plus exact
// job-ledger pages captured by the release caller. It never contacts a worker,
// changes placement or accepts a guessed explanation from final job status.
func VerifyCurvePreservation(before, after Snapshot, pages []JobLedgerPage) (CurvePreservationReceipt, error) {
	receipt := CurvePreservationReceipt{KeysBefore: len(before.WorkCurves), KeysAfter: len(after.WorkCurves), Changes: []CurvePreservationDiff{}}
	fail := func(reason string, keys ...ObservationKey) (CurvePreservationReceipt, error) {
		receipt.Failure = reason
		if len(keys) > 0 {
			receipt.FailedKey = &keys[0]
		}
		return receipt, fmt.Errorf("curve preservation: %s", reason)
	}
	if before.Now.IsZero() || after.Now.Before(before.Now) {
		return fail("invalid server snapshot interval")
	}
	index := func(curves []WorkObservation) (map[ObservationKey]WorkObservation, bool) {
		m := map[ObservationKey]WorkObservation{}
		for _, p := range curves {
			if _, ok := m[p.Key]; ok {
				return nil, false
			}
			m[p.Key] = p
		}
		return m, true
	}
	old, ok := index(before.WorkCurves)
	if !ok {
		return fail("duplicate before key")
	}
	current, ok := index(after.WorkCurves)
	if !ok {
		return fail("duplicate after key")
	}
	for key := range old {
		if _, ok := current[key]; !ok {
			return fail("curve key removed", key)
		}
	}
	type event struct {
		change   WorkLearningChange
		evidence CurvePreservationEvidence
	}
	events := map[ObservationKey][]event{}
	seen := map[string]bool{}
	for _, page := range pages {
		for _, e := range page.Entries {
			if e.WorkLearning == nil {
				continue
			}
			id := fmt.Sprintf("%s/%d", page.JobID, e.Sequence)
			if seen[id] {
				return fail("duplicate learning evidence")
			}
			seen[id] = true
			if e.Kind != "work_learning" || e.Reason != "accepted_sample" || page.JobID == "" || e.Attempt < 1 || e.Sequence < 1 || e.Machine == "" || e.Session == "" || e.ExecutionID != fmt.Sprintf("%s/%d", page.JobID, e.Attempt) {
				return fail("incomplete learning provenance")
			}
			if !e.At.After(before.Now) || e.At.After(after.Now) {
				continue
			}
			events[e.WorkLearning.Key] = append(events[e.WorkLearning.Key], event{*e.WorkLearning, CurvePreservationEvidence{page.JobID, e.Attempt, e.Sequence, e.At}})
		}
	}
	// Snapshot order is canonical in production; sort here for reproducible receipts.
	keys := make([]ObservationKey, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return identityHash(keys[i]) < identityHash(keys[j]) })
	for _, key := range keys {
		next := current[key]
		prev, exists := old[key]
		if next.Samples < prev.Samples || next.RecordedAt.Before(prev.RecordedAt) || next.MemoryRecordedAt.Before(prev.MemoryRecordedAt) {
			return fail("sample count or evidence clock decreased", key)
		}
		start := ""
		if exists {
			start = curveMeasurementDigest(prev)
		}
		end := curveMeasurementDigest(next)
		if start == end {
			continue
		}
		delta := next.Samples - prev.Samples
		if delta <= 0 {
			return fail("changed measurement without new accepted samples", key)
		}
		diff := CurvePreservationDiff{Key: key, SamplesBefore: prev.Samples, SamplesAfter: next.Samples, Evidence: []CurvePreservationEvidence{}}
		candidates := events[key]
		lastAt := before.Now
		for count := 0; count < delta; count++ {
			match := -1
			for i, e := range candidates {
				if e.change.Before == start {
					if match != -1 {
						return fail("ambiguous accepted-sample chain", key)
					}
					match = i
				}
			}
			if match < 0 {
				return fail("unexplained measurement change or missing accepted samples", key)
			}
			e := candidates[match]
			if e.evidence.At.Before(lastAt) || e.change.After == "" || e.change.After == start {
				return fail("invalid accepted-sample chain", key)
			}
			start, lastAt = e.change.After, e.evidence.At
			diff.Evidence = append(diff.Evidence, e.evidence)
			candidates = append(candidates[:match], candidates[match+1:]...)
		}
		if start != end {
			return fail("accepted samples do not explain final measurements", key)
		}
		receipt.Changes = append(receipt.Changes, diff)
	}
	return receipt, nil
}
