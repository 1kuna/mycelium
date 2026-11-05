package myce

import (
	"fmt"
)

// Runtime is a read-only execution projection, never a scheduling input.
type JobRuntime struct {
	ElapsedSeconds       *float64 `json:"elapsed_seconds,omitempty"`
	RemainingSeconds     *float64 `json:"remaining_seconds,omitempty"`
	RemainingWorkUnknown bool     `json:"remaining_work_unknown"`
	Source               string   `json:"source,omitempty"`
	Unit                 string   `json:"unit,omitempty"`
	Samples              int      `json:"samples"`
	HistoryAgeSeconds    *float64 `json:"history_age_seconds,omitempty"`
	Reason               string   `json:"reason,omitempty"`
	Limitation           string   `json:"limitation"`
}

func (q *Queue) projectRuntime(j *Job) {
	q.projectWallTime(j)
	q.projectLiveness(j)
	if j.ApplicationProgress != nil {
		p := *j.ApplicationProgress
		if p.Attempt != j.Attempt || p.Session != j.Session || p.Machine != j.Machine || j.State == "queued" {
			j.ApplicationProgress = nil
		} else {
			p.AgeSeconds += max(0, q.clock.Now().Sub(p.ReceivedAt).Seconds())
			p.Stale = p.AgeSeconds > applicationProgressMaxAge.Seconds()
			j.ApplicationProgress = &p
		}
	}
	if j.State != "running" && j.State != "stopping" {
		return
	}
	r := &JobRuntime{RemainingWorkUnknown: true, Limitation: "whole-job estimate from comparable history, not a completion deadline; includes result handoff, excludes queue wait and preparation; stage reports do not predict later stages"}
	j.Runtime = r
	if j.StartedAt == nil {
		r.Reason = "execution has not acknowledged Started"
		return
	}
	elapsed := max(0, q.clock.Now().Sub(*j.StartedAt).Seconds())
	r.ElapsedSeconds = &elapsed
	r.Reason = "no comparable history: workload/reference metadata unavailable"
	t := j.Timing
	if t == nil {
		return
	}
	if t.Reason != "" {
		r.Reason = "no comparable history: " + t.Reason
		return
	}
	if t.Execution.ID != j.ID || t.Execution.Attempt != j.Attempt || t.Machine != j.Machine || t.Session != j.Session {
		r.Reason = "no comparable history: attempt changed"
		return
	}
	if t.Key == nil {
		return
	}
	r.Reason = "no comparable history: no accepted matching measurement"
	r.Unit = t.Key.Reference.Unit
	if t.PredictedSeconds == nil || t.HistoryRecordedAt == nil {
		if t.HistoryReason != "" {
			r.Reason = "no comparable history: " + t.HistoryReason
		}
		return
	}
	age := max(0, q.clock.Now().Sub(*t.HistoryRecordedAt).Seconds())
	r.HistoryAgeSeconds, r.Samples, r.Source = &age, t.Samples, t.HistorySource
	if age > workCurveMaxAge.Seconds() {
		r.Reason = "no comparable history: measurement older than 30 days"
		return
	}
	if !validRate(*t.PredictedSeconds) {
		return
	}
	if elapsed >= *t.PredictedSeconds {
		r.Reason = "historical estimate exceeded; remaining time unknown"
		return
	}
	remaining := *t.PredictedSeconds - elapsed
	r.RemainingSeconds, r.Reason = &remaining, ""
	r.RemainingWorkUnknown = false
}

func runtimeText(j *Job) string {
	text := wallTimeText(j)
	if j.Liveness != nil {
		text += "; " + livenessText(j.Liveness)
	}
	if r := j.Runtime; r != nil {
		if r.ElapsedSeconds != nil {
			text += fmt.Sprintf("; elapsed %.2fs", *r.ElapsedSeconds)
		}
		if r.RemainingSeconds != nil {
			text += fmt.Sprintf("; remaining ~%.2fs (%s; %d samples; history age %.0fs; %s)", *r.RemainingSeconds, r.Source, r.Samples, *r.HistoryAgeSeconds, r.Unit)
		} else {
			text += "; remaining unknown: " + r.Reason
		}
	}
	if p := j.ApplicationProgress; p != nil {
		text += "; stage "
		if p.StageIndex == nil {
			text += "unknown"
		} else {
			text += fmt.Sprint(*p.StageIndex)
		}
		if p.StageCount == nil {
			text += "/unknown"
		} else {
			text += fmt.Sprintf("/%d", *p.StageCount)
		}
		text += fmt.Sprintf("; reported stage progress %g/%g %s (attempt %d; age %.1fs", p.Completed, p.Total, p.Unit, p.Attempt, p.AgeSeconds)
		if p.Stale {
			text += "; stale"
		}
		text += "; advisory)"
		text += fmt.Sprintf("; total work known: %t", p.TotalWorkKnown)
		if p.TotalWorkKnown && p.StageCount != nil && *p.StageCount == 1 && !p.Stale {
			text += fmt.Sprintf("; whole-job reported progress %g/%g %s (advisory; job state decides completion)", p.Completed, p.Total, p.Unit)
		} else {
			text += "; whole-job progress unknown (stage-local counts)"
		}
	} else if j.Runtime != nil {
		text += "; stage unknown/unknown; total work known: unknown; whole-job progress unknown (no application report)"
	}
	if j.Runtime != nil || j.ApplicationProgress != nil {
		if j.Progress > 0 {
			text += fmt.Sprintf("; checkpoint generation %d (recovery evidence, not work progress)", j.Progress)
		} else {
			text += "; checkpoint evidence: none reported"
		}
	}
	return text
}
