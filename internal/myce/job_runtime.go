package myce

import (
	"fmt"
)

// Runtime is a read-only execution projection, never a scheduling input.
type JobRuntime struct {
	ElapsedSeconds *float64 `json:"elapsed_seconds,omitempty"`
	// LastAcceptedSeconds is this kind of work's last accepted duration (MY-49).
	LastAcceptedSeconds *float64 `json:"last_accepted_seconds,omitempty"`
	Reason              string   `json:"reason,omitempty"`
	Limitation          string   `json:"limitation"`
	// Always emitted for the previous release's v1 reply (MY-42). Remaining
	// work is never forecast (MY-49); samples is the accepted history count.
	RemainingWorkUnknown bool `json:"remaining_work_unknown"`
	Samples              int  `json:"samples"`
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
			p.Stale = p.CadenceSeconds > 0 && p.AgeSeconds > p.CadenceSeconds
			j.ApplicationProgress = &p
		}
	}
	if j.State != "running" && j.State != "stopping" {
		return
	}
	r := &JobRuntime{RemainingWorkUnknown: true, Limitation: "history of this kind of work, not a forecast or completion deadline"}
	j.Runtime = r
	if j.StartedAt == nil {
		r.Reason = "execution has not acknowledged Started"
		return
	}
	elapsed := max(0, q.clock.Now().Sub(*j.StartedAt).Seconds())
	r.ElapsedSeconds = &elapsed
	r.Reason = "last accepted duration unknown: workload/reference metadata unavailable"
	t := j.Timing
	switch {
	case t == nil:
	case t.Reason != "":
		r.Reason = "last accepted duration unknown: " + t.Reason
	case t.Execution.ID != j.ID || t.Execution.Attempt != j.Attempt || t.Machine != j.Machine || t.Session != j.Session:
		r.Reason = "last accepted duration unknown: attempt changed"
	case t.LastAcceptedSeconds != nil:
		r.LastAcceptedSeconds, r.Reason, r.Samples = t.LastAcceptedSeconds, "", t.Samples
	case t.HistoryReason != "":
		r.Reason = "last accepted duration unknown: " + t.HistoryReason
	case t.Key != nil:
		r.Reason = "last accepted duration unknown: no accepted matching measurement"
	}
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
		if r.LastAcceptedSeconds != nil {
			text += fmt.Sprintf("; last accepted duration %.2fs", *r.LastAcceptedSeconds)
		} else if r.Reason != "" {
			text += "; " + r.Reason
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
