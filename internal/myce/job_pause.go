package myce

import (
	"context"
	"strings"
	"time"
)

// Pause is queue ownership, never execution or cleanup custody. Revision also
// survives resume so a delayed request cannot release a later pause.
type JobPause struct {
	Reason string     `json:"reason"`
	Until  *time.Time `json:"until,omitempty"`
	Parent string     `json:"parent,omitempty"`
}
type PauseJobRequest struct {
	Reason string     `json:"reason"`
	Until  *time.Time `json:"until,omitempty"`
}
type ResumeJobRequest struct {
	IfRevision uint64 `json:"if_revision"`
}

func (q *Queue) PauseJob(id string, request PauseJobRequest, contexts ...context.Context) (*Job, error) {
	if strings.TrimSpace(request.Reason) == "" {
		return nil, &HTTPError{Status: 400, Code: "job_pause_invalid", Message: "pause requires a reason"}
	}
	return q.changeJobPause(id, &request, 0, requestContext(contexts))
}
func (q *Queue) ResumeJob(id string, revision uint64, contexts ...context.Context) (*Job, error) {
	return q.changeJobPause(id, nil, revision, requestContext(contexts))
}
func (q *Queue) changeJobPause(id string, request *PauseJobRequest, revision uint64, ctx context.Context) (*Job, error) {
	var out *Job
	err := q.updateRequest(ctx, func() error {
		q.maintain()
		j, err := q.findObservationJob(id)
		if err != nil {
			return err
		}
		if q.s.Jobs[j.ID] == nil {
			return &HTTPError{Status: 409, Code: "job_pause_conflict", Message: "only queued batch jobs can pause"}
		}
		if request != nil {
			if j.State != "queued" {
				return &HTTPError{Status: 409, Code: "job_pause_conflict", Message: "only queued jobs can pause; assigned or running attempts cannot be paused"}
			}
			if request.Until != nil && !request.Until.After(q.clock.Now()) {
				return &HTTPError{Status: 400, Code: "job_pause_invalid", Message: "until must be a future absolute time"}
			}
			q.pauseJob(j, JobPause{Reason: request.Reason, Until: request.Until})
		} else {
			if j.State != "paused" || j.Pause == nil || j.Pause.Parent != "" || j.PauseRevision != revision {
				return &HTTPError{Status: 409, Code: "job_pause_revision_conflict", Message: "resume requires the current pause revision; parent-owned children resume with their parent"}
			}
			q.resumeJob(j)
		}
		q.maintain()
		copy := cloneJob(j)
		out = &copy
		return nil
	}, nil)
	return out, err
}
func (q *Queue) pauseJob(j *Job, pause JobPause) {
	q.touchJob(j.ID)
	// Detach caller timestamps before retaining them in mutable state.
	if pause.Until != nil {
		until := *pause.Until
		pause.Until = &until
	}
	j.State, j.Pause, j.Reason = "paused", &pause, pause.Reason
	j.PauseRevision++
	j.ProductionHost = "" // Derived placement demand is selected anew on resume.
	// Offers are revocable authority, not work. Replan from queued demand on the
	// next check-in, including cold extensions whose offers have no Demand ID.
	for id, p := range q.s.PreparationPlans {
		if !p.Started {
			delete(q.s.PreparationPlans, id)
		}
	}
}
func (q *Queue) resumeJob(j *Job) {
	q.touchJob(j.ID)
	j.State, j.Pause, j.Reason = "queued", nil, ""
	j.PauseRevision++
}
func (q *Queue) maintainJobPauses() {
	now := q.clock.Now()
	for _, j := range q.liveJobs() {
		if j.State == "paused" && j.Pause != nil && j.Pause.Parent == "" && expired(j.Pause.Until, now) {
			q.resumeJob(j)
		}
	}
	// Assigned children drain. Retried children join the parent's pause before
	// any planning, with their original queue/fairness fields untouched.
	for _, parent := range q.liveJobs() {
		if parent.FanOut == nil {
			continue
		}
		for _, id := range parent.FanOut.Children {
			child := q.s.Jobs[id]
			if child == nil {
				continue
			}
			if parent.State == "paused" && parent.Pause != nil && child.State == "queued" {
				pause := *parent.Pause
				pause.Parent = parent.ID
				q.pauseJob(child, pause)
			} else if parent.State != "paused" && child.State == "paused" && child.Pause != nil && child.Pause.Parent == parent.ID {
				q.resumeJob(child)
			}
		}
	}
}
