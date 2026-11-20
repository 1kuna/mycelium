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
	Reason         string     `json:"reason"`
	Until          *time.Time `json:"until,omitempty"`
	Project        string     `json:"project,omitempty"`
	Yield          bool       `json:"yield,omitempty"`
	ConfirmRestart bool       `json:"confirm_restart,omitempty"`
	IfAttempt      *int       `json:"if_attempt,omitempty"`
}

// YieldPause retains owner intent without claiming execution has been released.
// The ordinary maintenance stop and release protocol owns the attempt throughout.
type YieldPause struct {
	Execution
	Pause JobPause `json:"pause"`
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
			if request.Project != "" && request.Project != j.Spec.Project {
				return &HTTPError{Status: 403, Code: "job_project_mismatch", Message: "job belongs to another project"}
			}
			if request.Yield && (request.Project == "" || request.IfAttempt == nil) {
				return &HTTPError{Status: 400, Code: "job_pause_invalid", Message: "yield requires project and if_attempt"}
			}
			if request.IfAttempt != nil && *request.IfAttempt != j.Attempt {
				return ErrStale
			}
			if j.State != "queued" && !(request.Yield && (j.State == "running" || j.State == "stopping" && j.YieldPause != nil)) {
				return &HTTPError{Status: 409, Code: "job_pause_conflict", Message: "only queued jobs can pause; assigned or running attempts cannot be paused"}
			}
			if request.Until != nil && !request.Until.After(q.clock.Now()) {
				return &HTTPError{Status: 400, Code: "job_pause_invalid", Message: "until must be a future absolute time"}
			}
			pause := JobPause{Reason: request.Reason, Until: request.Until}
			if j.State == "queued" {
				q.pauseJob(j, pause)
			} else {
				if !j.Spec.Resumable && !request.ConfirmRestart {
					return &HTTPError{Status: 409, Code: "job_yield_confirmation_required", Message: "non-resumable work loses progress; confirm_restart is required"}
				}
				q.touchJob(j.ID)
				if pause.Until != nil {
					until := *pause.Until
					pause.Until = &until
				}
				j.YieldPause = &YieldPause{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Pause: pause}
				deadline := q.clock.Now().Add(StopGrace)
				q.maintainHostObligation(HostObligation{Result: resultIdentity(j)}, &deadline)
			}
		} else {
			if j.State != "paused" || j.Pause == nil || j.Pause.Parent != "" || j.PauseRevision != revision {
				return &HTTPError{Status: 409, Code: "job_pause_revision_conflict", Message: "resume requires the current pause revision; parent-owned children resume with their parent"}
			}
			q.resumeJob(j)
		}
		q.settleMaintenance()
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
	q.closeAdmissionWait(j, "paused")
	j.State, j.Pause, j.Reason = "paused", &pause, pause.Reason
	j.PauseRevision++
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
func (q *Queue) maintainJobPause(parent *Job) {
	if terminal(parent.State) && parent.YieldPause != nil {
		q.touchJob(parent.ID)
		parent.YieldPause = nil
	}
	if parent.State == "paused" && parent.Pause != nil && parent.Pause.Parent == "" && expired(parent.Pause.Until, q.clock.Now()) {
		q.resumeJob(parent)
	}
	if parent.FanOut == nil {
		return
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
