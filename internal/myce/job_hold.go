package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// JobHold is mutable admission state, independent of the immutable submission.
// Expiry changes eligibility, not the revision of the owner's last instruction.
type JobHold struct {
	Revision uint64     `json:"revision"`
	Paused   bool       `json:"paused"`
	Reason   string     `json:"reason,omitempty"`
	Until    *time.Time `json:"until,omitempty"`
	Active   bool       `json:"active"`
}

type JobHoldRequest struct {
	ExpectedRevision *uint64    `json:"expected_revision"`
	Reason           string     `json:"reason,omitempty"`
	Until            *time.Time `json:"until,omitempty"`
}

func (h *JobHold) active(now time.Time) bool {
	return h != nil && h.Paused && !expired(h.Until, now)
}

func (q *Queue) jobHoldReason(j *Job) string {
	if j == nil || terminal(j.State) || cancelled(j) {
		return ""
	}
	if j.Hold.active(q.clock.Now()) {
		return "job held: " + j.Hold.Reason
	}
	// A parent gates further materialization and unstarted children, never a
	// payload that has already crossed Started. Existing waves retain custody.
	if j.Batch != nil && j.StartedAt == nil {
		if parent := q.s.Jobs[j.Batch.Parent]; parent != nil && parent.Hold.active(q.clock.Now()) {
			return "parent job held: " + parent.Hold.Reason
		}
	}
	return ""
}

func (q *Queue) ChangeJobHold(id string, request JobHoldRequest, pause bool, contexts ...context.Context) (JobHold, error) {
	var out JobHold
	if request.ExpectedRevision == nil {
		return out, reject("job_hold_invalid", errors.New("expected_revision required"))
	}
	if pause && strings.TrimSpace(request.Reason) == "" {
		return out, reject("job_hold_invalid", errors.New("reason required"))
	}
	if !pause && (request.Reason != "" || request.Until != nil) {
		return out, reject("job_hold_invalid", errors.New("resume accepts only expected_revision"))
	}
	err := q.updateRequest(requestContext(contexts), func() error {
		j := q.s.Jobs[id]
		if j == nil {
			jobs := make([]*Job, 0, len(q.s.Jobs))
			for _, candidate := range q.s.Jobs {
				jobs = append(jobs, candidate)
			}
			var err error
			j, err = FindJob(jobs, id)
			if err != nil {
				return err
			}
			id = j.ID
		}
		revision := uint64(0)
		if j.Hold != nil {
			revision = j.Hold.Revision
		}
		if revision != *request.ExpectedRevision {
			return &HTTPError{Status: http.StatusConflict, Code: "job_hold_revision_conflict", CurrentRevision: &revision, Message: fmt.Sprintf("job hold revision changed; current revision %d", revision)}
		}
		if j.State != "queued" || cancelled(j) {
			return &HTTPError{Status: http.StatusConflict, Code: "job_hold_state_conflict", Message: "only queued jobs can be held or resumed; assigned and running executions cannot be paused"}
		}
		if revision == ^uint64(0) {
			return reject("job_hold_invalid", errors.New("job hold revision exhausted"))
		}
		if pause && expired(request.Until, q.clock.Now()) {
			return reject("job_hold_invalid", errors.New("until must be in the future"))
		}
		out = JobHold{Revision: revision + 1, Paused: pause, Active: pause}
		if pause {
			out.Reason = request.Reason
			if request.Until != nil {
				until := *request.Until
				out.Until = &until
			}
		}
		stored := out
		if out.Until != nil {
			until := *out.Until
			stored.Until = &until
		}
		stored.Active = false // Derived by every receipt/status read.
		q.touchJob(id).Hold = &stored
		return nil
	}, nil)
	return out, err
}

func (a API) registerJobHolds(mux *requestMux) {
	for _, operation := range []string{"hold", "resume"} {
		mux.HandleFunc("POST /api/jobs/{id}/"+operation, func(w http.ResponseWriter, r *http.Request) {
			var request JobHoldRequest
			if !decode(w, r, &request) {
				return
			}
			hold, err := a.Queue.ChangeJobHold(r.PathValue("id"), request, operation == "hold", r.Context())
			respondOperation(w, hold, err, "job_hold_invalid")
		})
	}
}
