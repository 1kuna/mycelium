package myce

import (
	"slices"
	"time"
)

// No elapsed interval supplies a missing measurement. This exact power-of-two
// duration survives wire float conversion; it represents an unknown cutoff,
// not a tuned timeout. Cold work retains custody after cooperative TERM.
const unmeasuredBatchGrace = time.Duration(1 << 62)

// Small queue-owned observations, committed with the same transaction as the
// exact-attempt report. They never establish checkpoint ownership or release.
type batchLifecycle struct {
	ReportAt      time.Time     `json:"report_at"`
	ReportMin     time.Duration `json:"report_min"`
	ReportMax     time.Duration `json:"report_max"`
	Attempt       int           `json:"attempt"`
	Session       string        `json:"session"`
	Machine       string        `json:"machine"`
	CheckpointAt  time.Time     `json:"checkpoint_at"`
	CheckpointMin time.Duration `json:"checkpoint_min"`
	CheckpointMax time.Duration `json:"checkpoint_max"`
}

func (q *Queue) observeBatchProgress(j *Job, generation int64) {
	if q.s.BatchLifecycle == nil {
		q.s.BatchLifecycle = map[string]batchLifecycle{}
	}
	v := q.s.BatchLifecycle[j.ID]
	now := q.clock.Now()
	if v.Attempt != j.Attempt || v.Session != j.Session || v.Machine != j.Machine {
		v = batchLifecycle{Attempt: j.Attempt, Session: j.Session, Machine: j.Machine}
		if j.StartedAt != nil {
			v.CheckpointAt = *j.StartedAt
			v.ReportAt = *j.StartedAt
		}
	}
	if !v.ReportAt.IsZero() && now.After(v.ReportAt) {
		d := now.Sub(v.ReportAt)
		if v.ReportMin == 0 || d < v.ReportMin {
			v.ReportMin = d
		}
		v.ReportMax = max(v.ReportMax, d)
	}
	v.ReportAt = now
	if generation <= j.Progress || generation <= 0 {
		q.s.BatchLifecycle[j.ID] = v
		q.boundLearnedBatchStop(j, now)
		return
	}
	if !v.CheckpointAt.IsZero() && now.After(v.CheckpointAt) {
		d := now.Sub(v.CheckpointAt)
		if v.CheckpointMin == 0 || d < v.CheckpointMin {
			v.CheckpointMin = d
		}
		v.CheckpointMax = max(v.CheckpointMax, d)
	}
	v.CheckpointAt = now
	q.s.BatchLifecycle[j.ID] = v
	q.boundLearnedBatchStop(j, now)
}

// Use this attempt's observed checkpoint cadence and this job's confirmed
// cleanup durations on the same machine. The observed range supplies jitter
// headroom; no cross-machine multiplier or universal wall time is assumed.
func (q *Queue) batchStopGrace(j *Job) time.Duration {
	if q.productionGrace > 0 {
		return q.productionGrace
	}
	if !j.Spec.Resumable && j.Spec.Needs.cpuOnly() {
		return 0
	}
	var low, high time.Duration
	add := func(d time.Duration) {
		if d <= 0 {
			return
		}
		if low == 0 || d < low {
			low = d
		}
		high = max(high, d)
	}
	if v := q.s.BatchLifecycle[j.ID]; v.Attempt == j.Attempt && v.Session == j.Session && v.Machine == j.Machine {
		add(v.CheckpointMin)
		add(v.CheckpointMax)
		if !j.Spec.Resumable {
			add(v.ReportMin)
			add(v.ReportMax)
		}
	}
	if !j.Spec.Resumable && j.AssignedAt != nil && j.StartedAt != nil {
		add(j.StartedAt.Sub(*j.AssignedAt))
	}
	for _, c := range j.Copies {
		if c.Machine == j.Machine && c.ReleaseConfirmed && c.StoppedAt != nil && c.ReleasedAt != nil {
			add(c.ReleasedAt.Sub(*c.StoppedAt))
		}
	}
	if high == 0 {
		return unmeasuredBatchGrace
	}
	// Saturate at the representation boundary rather than overflowing a long
	// checkpoint cadence into immediate KILL.
	return high + min(high-low, unmeasuredBatchGrace-high)
}

// Comparable accepted starts describe the installation's actual queue and
// preparation latency. This is advisory: overload cannot create a new caller
// deadline, and unrelated recipes/targets cannot shorten an authored cutoff.
func (q *Queue) observedPlacementLatency(j *Job) time.Duration {
	var observed time.Duration
	for _, sample := range q.s.ProductionStarts {
		old := q.s.Jobs[sample.Job]
		if old == nil || !slices.Equal(old.Spec.Run.Command, j.Spec.Run.Command) || old.Spec.Environment.Name != j.Spec.Environment.Name || old.Spec.Run.Docker != j.Spec.Run.Docker {
			continue
		}
		observed = max(observed, time.Duration(sample.Seconds*float64(time.Second)))
	}
	return observed
}

// Unknown cleanup has no predicted release credit. It must not attract more
// demand to an unmeasured target while other targets can make progress.
func unmeasuredBatchStop(j *Job) bool {
	if j.StopAt == nil {
		return false
	}
	for _, c := range j.Copies {
		if c.Attempt == j.Attempt && c.Session == j.Session && c.Machine == j.Machine && c.StoppedAt != nil {
			return j.StopAt.Sub(*c.StoppedAt) == unmeasuredBatchGrace
		}
	}
	return false
}

func (q *Queue) boundLearnedBatchStop(j *Job, now time.Time) {
	if j.State == "stopping" && unmeasuredBatchStop(j) {
		if grace := q.batchStopGrace(j); grace < unmeasuredBatchGrace {
			deadline := now.Add(grace)
			j.StopAt = &deadline
		}
	}
}
