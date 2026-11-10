package myce

import (
	"errors"
	"fmt"
	"time"
)

const wallTimeLimit = "wall_time_limit"

// Omission is unlimited: no measured duration is suitable for every workload.
func (s Spec) wallTimeDuration() time.Duration {
	d, _ := time.ParseDuration(s.WallTime)
	return d
}
func validateWallTime(s Spec) error {
	if s.WallTime == "" {
		return nil
	}
	d, err := time.ParseDuration(s.WallTime)
	if err != nil || d <= 0 {
		return errors.New("wall_time must be a positive Go duration, or omitted for unlimited execution")
	}
	return nil
}
func wallTimeCause() *RuntimeCause {
	return &RuntimeCause{Operation: "execution", Class: wallTimeLimit}
}
func wallTimeStopped(j *Job) bool {
	for _, c := range j.Copies {
		if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session && c.StopCause == wallTimeLimit {
			return true
		}
	}
	return false
}
func wallTimeExpired(j *Job, now time.Time) bool {
	at := wallTimeDeadline(j)
	return at != nil && !now.Before(*at)
}
func wallTimeDeadline(j *Job) *time.Time {
	if j.StartedAt == nil || j.Spec.wallTimeDuration() == 0 {
		return nil
	}
	at := j.StartedAt.Add(j.Spec.wallTimeDuration())
	return &at
}
func (q *Queue) maintainWallTimeLimits() {
	now := q.clock.Now()
	for _, j := range q.liveJobs() {
		if j.State != "running" || cancelled(j) {
			continue
		}
		if at := wallTimeDeadline(j); at != nil && !now.Before(*at) {
			q.stop(j, wallTimeLimit)
			j.ExecutionCause = wallTimeCause()
			for i := range j.Copies {
				c := &j.Copies[i]
				if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session {
					c.StopCause = wallTimeLimit
					c.Recoverable = false
				}
			}
		}
	}
}

// Use ordinary command-failure policy (no automatic retry), never loss policy.
// Terminal state is not release proof: the existing copies/releases retain custody.
func (q *Queue) finishWallTime(j *Job) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	now := q.clock.Now()
	j.State, j.Reason, j.FinishedAt = "failed", "execution failed: execution: wall_time_limit", &now
	j.StopAt = nil
	j.ExecutionCause = wallTimeCause()
	q.recordAttemptOutcome(j, attemptOutcome(j, Result{ExecutionCause: wallTimeCause()}))
}

type WallTimeStatus struct {
	Limit            string     `json:"limit"`
	ExpiresAt        *time.Time `json:"expires_at,omitempty"`
	RemainingSeconds *float64   `json:"remaining_seconds,omitempty"`
}

func (q *Queue) projectWallTime(j *Job) {
	if j.Spec.WallTime == "" {
		return
	}
	j.WallTime = &WallTimeStatus{Limit: j.Spec.WallTime, ExpiresAt: wallTimeDeadline(j)}
	if at := j.WallTime.ExpiresAt; at != nil {
		now := q.clock.Now()
		if j.FinishedAt != nil {
			now = *j.FinishedAt
		}
		left := max(0, at.Sub(now).Seconds())
		j.WallTime.RemainingSeconds = &left
	}
}
func wallTimeText(j *Job) string {
	if j.WallTime == nil {
		return ""
	}
	text := "; wall limit " + j.WallTime.Limit
	if left := j.WallTime.RemainingSeconds; left != nil {
		text += fmt.Sprintf("; wall allowance %.2fs remaining", *left)
	}
	return text
}

// File workers acknowledge physical release before publishing failure artifacts.
// A wall-time release may already have made the job terminal. Permit only this
// exact failed execution's opted-in diagnostic manifest, never success outputs.
func wallTimeDiagnosticsAllowed(j *Job, r Result) bool {
	return j != nil && j.State == "failed" && !cancelled(j) &&
		j.ID == r.ID && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session &&
		j.ExecutionCause != nil && j.ExecutionCause.Class == wallTimeLimit &&
		!r.Success && !r.ReleaseOnly && r.ExecutionCause != nil && r.ExecutionCause.Class == wallTimeLimit &&
		j.Spec.Diagnostics != nil && r.Diagnostics != nil
}
func (q *Queue) wallTimeVerificationJob(r Result) (*Job, error) {
	j, err := q.current(r)
	if err != nil && wallTimeDiagnosticsAllowed(q.s.Jobs[r.ID], r) {
		return q.touchJob(r.ID), nil
	}
	return j, err
}
