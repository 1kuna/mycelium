package myce

import (
	"context"
	"strings"
)

// JobControl records the last fenced write. Legacy and parent-owned transitions
// can advance Job.PauseRevision independently without forgetting the generation.
type JobControl struct {
	Generation uint64 `json:"generation"`
	Action     string `json:"action"`
	Revision   uint64 `json:"revision"`
}

type FencedPauseRequest struct {
	Project           string  `json:"project"`
	Reason            string  `json:"reason"`
	IfAttempt         *int    `json:"if_attempt"`
	IfRevision        *uint64 `json:"if_revision,omitempty"`
	ControlGeneration uint64  `json:"control_generation"`
}

type FencedResumeRequest struct {
	IfRevision        *uint64 `json:"if_revision"`
	ControlGeneration uint64  `json:"control_generation"`
}

func (q *Queue) FencedPause(ctx context.Context, id string, request FencedPauseRequest) (*Job, error) {
	if strings.TrimSpace(request.Project) == "" || strings.TrimSpace(request.Reason) == "" || request.IfAttempt == nil || *request.IfAttempt < 0 || request.ControlGeneration == 0 {
		return nil, &HTTPError{Status: 400, Code: "job_pause_invalid", Message: "fenced pause requires project, reason, if_attempt and a positive control_generation"}
	}
	return q.changeFencedJobControl(ctx, id, func(j *Job) error {
		if request.Project != j.Spec.Project {
			return &HTTPError{Status: 403, Code: "job_project_mismatch", Message: "job belongs to another project"}
		}
		if *request.IfAttempt != j.Attempt {
			return ErrStale
		}
		generation := request.ControlGeneration
		if c := j.Control; c != nil && generation <= c.Generation {
			if generation == c.Generation && c.Action == "pause" && c.Revision == j.PauseRevision && directJobPause(j) && j.Pause.Reason == request.Reason &&
				(request.IfRevision == nil || *request.IfRevision == c.Revision-1) {
				return nil
			}
			return jobControlGenerationConflict()
		}
		if request.IfRevision != nil && *request.IfRevision != j.PauseRevision {
			return &HTTPError{Status: 409, Code: "job_pause_revision_conflict", Message: "fenced pause requires the current pause revision"}
		}
		if j.State != "queued" && !directJobPause(j) {
			return &HTTPError{Status: 409, Code: "job_pause_conflict", Message: "fenced pause requires queued work or a direct hold"}
		}
		q.pauseJob(j, JobPause{Reason: request.Reason})
		j.Control = &JobControl{Generation: generation, Action: "pause", Revision: j.PauseRevision}
		return nil
	})
}

func (q *Queue) FencedResume(ctx context.Context, id string, request FencedResumeRequest) (*Job, error) {
	if request.IfRevision == nil || request.ControlGeneration == 0 {
		return nil, &HTTPError{Status: 400, Code: "job_resume_invalid", Message: "fenced resume requires if_revision and a positive control_generation"}
	}
	return q.changeFencedJobControl(ctx, id, func(j *Job) error {
		generation, revision := request.ControlGeneration, *request.IfRevision
		if c := j.Control; c != nil {
			if generation < c.Generation {
				return jobControlGenerationConflict()
			}
			if generation == c.Generation && c.Action == "resume" {
				if c.Revision == j.PauseRevision && revision == c.Revision-1 {
					return nil
				}
				return jobControlGenerationConflict()
			}
		}
		if j.PauseRevision != revision {
			return &HTTPError{Status: 409, Code: "job_pause_revision_conflict", Message: "fenced resume requires the current pause revision"}
		}
		switch {
		case directJobPause(j):
			q.resumeJob(j)
		case j.State == "queued":
			// Seal the generation without restarting a runnable job's start window.
			q.touchJob(j.ID)
			j.PauseRevision++
		default:
			return &HTTPError{Status: 409, Code: "job_pause_conflict", Message: "fenced resume requires queued work or a direct hold; parent-owned children resume with their parent"}
		}
		j.Control = &JobControl{Generation: generation, Action: "resume", Revision: j.PauseRevision}
		return nil
	})
}

func directJobPause(j *Job) bool {
	return j.State == "paused" && j.Pause != nil && j.Pause.Parent == ""
}

func jobControlGenerationConflict() error {
	return &HTTPError{Status: 409, Code: "job_control_generation_conflict", Message: "control generation has already been used or superseded; read the current job"}
}

func (q *Queue) changeFencedJobControl(ctx context.Context, id string, change func(*Job) error) (*Job, error) {
	var out *Job
	err := q.updateRequest(ctx, func() error {
		q.maintain()
		j := q.s.Jobs[id]
		if j == nil {
			j = q.retainedJob(id)
		}
		if j == nil {
			return &HTTPError{Status: 404, Code: "job_unknown", Message: "job is unknown; use its exact ID", JobID: id}
		}
		if cancelled(j) {
			return &HTTPError{Status: 409, Code: "job_control_cancelled", Message: "cancellation excludes further job controls"}
		}
		if err := change(j); err != nil {
			return err
		}
		q.settleMaintenance()
		copy := cloneJob(j)
		out = &copy
		return nil
	}, nil)
	return out, err
}
