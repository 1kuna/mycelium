package myce

import (
	"slices"
	"strings"
	"time"
)

type SharedHostBackoff struct {
	RecoverySeconds float64   `json:"recovery_seconds,omitempty"`
	QuietSince      time.Time `json:"quiet_since,omitempty"`
	LastObservation time.Time `json:"last_observation,omitempty"`
}

func (q *Queue) recordSharedHostLatencyStop(j *Job, r Result) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	if !r.Interrupted || r.Success || r.OutOfMemory || j.Reason != "shared host owner scheduling latency" {
		return
	}
	stop := priorityStop(j)
	m := q.s.Machines[j.Machine]
	if stop == nil || stop.Cause != sharedHostPressureCause || m == nil {
		return
	}
	host, now := hostID(m), q.clock.Now()
	b := j.SharedHostBackoff[host]
	// A stopped execution already proves interference. Require a quiet span
	// covering its measured time to interference, rather than permitting two
	// more disruptive restarts before an arbitrary fifteen-minute timer.
	span := now.Sub(j.Queued)
	if j.StartedAt != nil {
		span = now.Sub(*j.StartedAt)
	}
	if m.SharedHostPressure != nil {
		span = max(span, m.SharedHostPressure.Probe.freshness())
	}
	b.RecoverySeconds = max(b.RecoverySeconds, max(time.Nanosecond, span).Seconds())
	b.QuietSince, b.LastObservation = time.Time{}, time.Time{}
	if j.SharedHostBackoff == nil {
		j.SharedHostBackoff = map[string]SharedHostBackoff{}
	}
	j.SharedHostBackoff[host] = b
}

func (q *Queue) sharedHostBackoffReason(j *Job, m *Machine) string {
	if b, ok := j.SharedHostBackoff[hostID(m)]; ok && b.RecoverySeconds > 0 {
		return "shared host owner scheduling latency backoff awaiting measured recovery"
	}
	return ""
}

// Update only on successful, distinct observations in the check-in transaction.
// Diagnostic reads never advance recovery. A gap/failure restarts the quiet
// span; passing wall time or queue contact is not affirmative recovery.
func (q *Queue) recoverSharedHostBackoff(m *Machine) {
	for _, j := range q.liveJobs() {
		b, ok := j.SharedHostBackoff[hostID(m)]
		if !ok || b.RecoverySeconds <= 0 {
			continue
		}
		p := m.SharedHostPressure
		if !q.sharedPressureFresh(m) || m.SharedHostYieldReason != "" {
			b.QuietSince, b.LastObservation = time.Time{}, time.Time{}
		} else if p.ReceivedAt.After(b.LastObservation) {
			if b.QuietSince.IsZero() || b.LastObservation.IsZero() || p.ReceivedAt.Sub(b.LastObservation) >= p.Probe.freshness() {
				b.QuietSince = p.ReceivedAt
			}
			b.LastObservation = p.ReceivedAt
			if p.ReceivedAt.Sub(b.QuietSince).Seconds() >= b.RecoverySeconds {
				delete(j.SharedHostBackoff, hostID(m))
				continue
			}
		}
		j.SharedHostBackoff[hostID(m)] = b
	}
}

// Shared-host backoff and unreachable storage roots that hold this job.
func (q *Queue) hostRetryStorageWait(j *Job) *Hold {
	blocked := false
	for _, m := range q.s.Machines {
		if len(m.UnavailableRoots) > 0 || q.sharedHostBackoffReason(j, m) != "" {
			blocked = true
			break
		}
	}
	if !blocked {
		return nil
	}
	kind := "storage_root"
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
		if reason != "" {
			kind = "shared_host_backoff"
		} else {
			reason = q.storageRootReason(j, m)
		}
		if reason == "" {
			return nil
		}
		reasons = append(reasons, reason)
	}
	if len(reasons) == 0 {
		return nil
	}
	slices.Sort(reasons)
	return newHold(kind, strings.Join(slices.Compact(reasons), "; "))
}
