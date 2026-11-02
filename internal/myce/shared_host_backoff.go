package myce

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// Three interrupted executions in ten minutes capture the observed 1–3 minute
// owner-latency restart loop. Fifteen minutes gives the owner a quiet interval,
// costing this job only that host; other eligible hosts remain available.
const sharedLatencyWindow = 10 * time.Minute
const sharedLatencyThreshold = 3
const sharedLatencyBackoff = 15 * time.Minute

type SharedHostBackoff struct {
	Stops []time.Time `json:"stops,omitempty"`
	Until time.Time   `json:"until"`
}

func (q *Queue) recordSharedHostLatencyStop(j *Job, r Result) {
	if !r.Interrupted || r.Success || j.Reason != "shared host owner scheduling latency" {
		return
	}
	stop := priorityStop(j)
	m := q.s.Machines[j.Machine]
	if stop == nil || stop.Cause != sharedHostPressureCause || m == nil {
		return
	}
	host, now := hostID(m), q.clock.Now()
	b := j.SharedHostBackoff[host]
	kept := []time.Time{}
	for _, at := range b.Stops {
		if !at.Before(now.Add(-sharedLatencyWindow)) && !at.After(now) {
			kept = append(kept, at)
		}
	}
	b.Stops = append(kept, now)
	if len(b.Stops) >= sharedLatencyThreshold {
		b.Until = now.Add(sharedLatencyBackoff)
		b.Stops = nil
	}
	if j.SharedHostBackoff == nil {
		j.SharedHostBackoff = map[string]SharedHostBackoff{}
	}
	j.SharedHostBackoff[host] = b
}

func (q *Queue) sharedHostBackoffReason(j *Job, m *Machine) string {
	if b, ok := j.SharedHostBackoff[hostID(m)]; ok && q.clock.Now().Before(b.Until) {
		return fmt.Sprintf("shared host owner scheduling latency backoff on %s until %s", hostID(m), b.Until.UTC().Format(time.RFC3339))
	}
	return ""
}

func (q *Queue) hostRetryStorageWaitingReason(j *Job) string {
	blocked := false
	for _, m := range q.s.Machines {
		if len(m.UnavailableRoots) > 0 || q.sharedHostBackoffReason(j, m) != "" {
			blocked = true
			break
		}
	}
	if !blocked {
		return ""
	}
	reasons := []string{}
	for _, m := range q.plannerMachines() {
		if m.Gone || !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		// Ignore temporary root failures when deciding whether this is otherwise a
		// compatible target. No filesystem work occurs in this queue-side check.
		candidate := *m
		candidate.UnavailableRoots = nil
		candidate.CheckpointUnavailable = false
		if !fits(j, &candidate, max(m.TotalMemoryGB, m.FreeMemoryGB)) {
			continue
		}
		reason := q.sharedHostBackoffReason(j, m)
		if reason == "" {
			reason = q.storageRootReason(j, m)
		}
		if reason == "" {
			return ""
		}
		reasons = append(reasons, reason)
	}
	slices.Sort(reasons)
	return strings.Join(slices.Compact(reasons), "; ")
}
