package myce

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// Six five-second observations tolerate ordinary probe/check-in jitter. This is
// diagnostic freshness only; it never authorizes launch, loss or cancellation.
const attemptLivenessMaxAge = 30 * time.Second

// Signal values are cumulative process-tree CPU seconds or logical work-dir
// bytes. Missing/failed/incomplete observations cannot establish inactivity.
type LivenessSignal struct {
	Value       *float64 `json:"value,omitempty"`
	AgeSeconds  float64  `json:"age_seconds"`
	Compared    bool     `json:"compared"`
	Advancing   bool     `json:"advancing"`
	IdleSeconds float64  `json:"idle_seconds"`
	Unavailable bool     `json:"unavailable,omitempty"`
	sampledAt   time.Time
}

type LivenessReport struct {
	CPU     LivenessSignal `json:"cpu"`
	WorkDir LivenessSignal `json:"work_dir"`
}

type AttemptLiveness struct {
	LivenessReport
	Attempt     int       `json:"attempt"`
	Machine     string    `json:"machine"`
	Session     string    `json:"session"`
	ReceivedAt  time.Time `json:"received_at"`
	State       string    `json:"state"`
	IdleSeconds *float64  `json:"idle_seconds,omitempty"`
	Reason      string    `json:"reason,omitempty"`
}

func (r *LivenessReport) aged(now time.Time) *LivenessReport {
	if r == nil {
		return nil
	}
	v := *r
	for _, s := range []*LivenessSignal{&v.CPU, &v.WorkDir} {
		if !s.sampledAt.IsZero() {
			s.AgeSeconds = max(0, now.Sub(s.sampledAt).Seconds())
		}
	}
	return &v
}

func validLivenessSignal(s LivenessSignal) bool {
	return validMemoryValue(s.AgeSeconds) && validMemoryValue(s.IdleSeconds) && (s.Value == nil || validMemoryValue(*s.Value)) && (!s.Advancing || s.Compared && s.Value != nil)
}

// The caller already fenced job, attempt, machine and worker session. Only a
// small immutable value is retained here; all OS work belongs to the worker.
func (q *Queue) retainLiveness(j *Job, r *AttemptReport) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	if r.Liveness == nil || !r.Started || !validLivenessSignal(r.Liveness.CPU) || !validLivenessSignal(r.Liveness.WorkDir) {
		return
	}
	v := &AttemptLiveness{LivenessReport: *r.Liveness, Attempt: j.Attempt, Machine: j.Machine, Session: j.Session, ReceivedAt: q.clock.Now()}
	j.Liveness = v
	for i := range j.Copies {
		c := &j.Copies[i]
		if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session {
			c.Liveness = v
		}
	}
}

func projectAttemptLiveness(in *AttemptLiveness, now time.Time, active bool) *AttemptLiveness {
	if in == nil {
		return &AttemptLiveness{State: "unknown", Reason: "worker has not reported liveness"}
	}
	v := *in
	elapsed := max(0, now.Sub(v.ReceivedAt).Seconds())
	v.CPU.AgeSeconds += elapsed
	v.WorkDir.AgeSeconds += elapsed
	v.State, v.Reason, v.IdleSeconds = "unknown", "missing, unavailable or stale signal; two comparable samples required for idle", nil
	fresh := func(s LivenessSignal) bool {
		return s.Value != nil && !s.Unavailable && s.AgeSeconds <= attemptLivenessMaxAge.Seconds()
	}
	cpu, dir := fresh(v.CPU), fresh(v.WorkDir)
	switch {
	case !active:
		v.Reason = "attempt is not executing; retained last observation"
	case cpu && (v.CPU.Advancing || dir && v.WorkDir.Advancing):
		v.State, v.Reason = "advancing", ""
	case cpu && dir && v.CPU.Compared && v.WorkDir.Compared:
		idle := min(v.CPU.IdleSeconds, v.WorkDir.IdleSeconds)
		v.State, v.Reason, v.IdleSeconds = "idle", "", &idle
	}
	return &v
}

func (q *Queue) projectLiveness(j *Job) {
	active := (j.State == "running" || j.State == "stopping") && j.StartedAt != nil && !j.ReleaseConfirmed
	if active || j.Liveness != nil {
		if j.Liveness != nil && (j.Liveness.Attempt != j.Attempt || j.Liveness.Machine != j.Machine || j.Liveness.Session != j.Session) {
			j.Liveness = nil
		}
		j.Liveness = projectAttemptLiveness(j.Liveness, q.clock.Now(), active)
		j.Liveness.Attempt, j.Liveness.Machine, j.Liveness.Session = j.Attempt, j.Machine, j.Session
	}
	j.Copies = slices.Clone(j.Copies)
	for i := range j.Copies {
		c := &j.Copies[i]
		current := c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session
		if c.Liveness != nil || current && active {
			c.Liveness = projectAttemptLiveness(c.Liveness, q.clock.Now(), current && active)
			c.Liveness.Attempt, c.Liveness.Machine, c.Liveness.Session = c.Attempt, c.Machine, c.Session
		}
	}
}

func livenessText(v *AttemptLiveness) string {
	state := "liveness unknown"
	if v.State == "advancing" {
		state = "alive and advancing"
	}
	if v.State == "idle" && v.IdleSeconds != nil {
		state = fmt.Sprintf("alive but idle for %.1f min", *v.IdleSeconds/60)
	}
	signal := func(s LivenessSignal, unit string) string {
		if s.Value == nil {
			return "unknown"
		}
		text := fmt.Sprintf("%g%s age %.1fs", *s.Value, unit, s.AgeSeconds)
		if s.Unavailable || s.AgeSeconds > attemptLivenessMaxAge.Seconds() {
			text += " unavailable/stale"
		}
		return text
	}
	return fmt.Sprintf("%s (CPU %s; work-dir %s; activity is not useful-progress proof)", state, signal(v.CPU, "s"), signal(v.WorkDir, "B"))
}

// Tracks only successive successful observations. A counter reset, failed scan
// or observation gap breaks continuity, so it can never turn unknown into idle.
type livenessCounter struct {
	value       float64
	at, changed time.Time
	valid       bool
}

func (c *livenessCounter) observe(value float64, err error, now time.Time, previous LivenessSignal) LivenessSignal {
	if err != nil {
		c.valid = false
		previous.Unavailable, previous.Compared, previous.Advancing = true, false, false
		return previous
	}
	v := LivenessSignal{Value: &value, sampledAt: now}
	if c.valid && now.After(c.at) && now.Sub(c.at) <= attemptLivenessMaxAge && value >= c.value {
		v.Compared, v.Advancing = true, value > c.value
		if v.Advancing {
			c.changed = now
		}
		v.IdleSeconds = max(0, now.Sub(c.changed).Seconds())
	} else {
		c.changed = now
	}
	c.value, c.at, c.valid = value, now, true
	return v
}

type attemptLivenessSampler struct {
	mu     sync.Mutex
	report LivenessReport
	cancel func()
	done   chan struct{}
}

func (s *attemptLivenessSampler) snapshot(now time.Time) *LivenessReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.report.aged(now)
}
