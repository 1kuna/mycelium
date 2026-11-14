package myce

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

// Opt in through a header so previous queues keep accepting batch-v1 JSON.
const completionCollectHeader = "X-Myce-Collect-Completion"

// CompletionState is persisted with the job in the queue's SQLite transaction.
// DeliveryID is frozen before invoking a receiver: interrupted delivery always
// reuses it, including from a different local receipt or observer process.
type CompletionState struct {
	CollectedAt  *time.Time `json:"collected_at,omitempty"`
	SupersededBy string     `json:"superseded_by,omitempty"`
	DeliveryID   string     `json:"delivery_id,omitempty"`
	DeliveredAt  *time.Time `json:"delivered_at,omitempty"`
}

type CompletionRequest struct {
	Operation  string `json:"operation"`
	DeliveryID string `json:"delivery_id,omitempty"`
}
type CompletionReply struct {
	Allowed    bool   `json:"allowed"`
	DeliveryID string `json:"delivery_id,omitempty"`
}

func completionState(j *Job) *CompletionState {
	if j.Completion == nil {
		j.Completion = &CompletionState{}
	}
	return j.Completion
}

func (q *Queue) completionJob(id string) (*Job, error) {
	if j := q.retainedJob(id); j != nil {
		return j, nil
	}
	j, err := q.findObservationJob(id)
	if err != nil {
		return nil, err
	}
	j = q.retainedJob(j.ID)
	if j == nil {
		return nil, &HTTPError{Status: 404, Code: "job_unknown", Message: "job is unknown"}
	}
	return j, nil
}

// Only an explicit replacement link is authority to suppress a different job.
// Same-key submissions are immutable replays, never implicit replacements.
func (q *Queue) supersedeCompletion(replacement *Job) error {
	if replacement.Spec.Supersedes == "" {
		return nil
	}
	old, err := q.completionJob(replacement.Spec.Supersedes)
	if err != nil {
		return err
	}
	if old.ID == replacement.ID || old.Spec.Project != replacement.Spec.Project {
		return &HTTPError{Status: 409, Code: "job_supersedes_invalid", Message: "supersedes must name a different job in the same project"}
	}
	completionState(old).SupersededBy = replacement.ID
	return nil
}

func (q *Queue) collectCompletions(ctx context.Context, states []WaitState) error {
	ids := []string{}
	for _, state := range states {
		if terminal(state.State) {
			ids = append(ids, state.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	return q.updateRequest(ctx, func() error {
		for _, id := range ids {
			// Model calls have no detached job notifier to disarm.
			if q.calls[id] != nil {
				continue
			}
			j, err := q.completionJob(id)
			if err != nil {
				return err
			}
			q.collectCompletion(j)
		}
		return nil
	}, nil)
}
func (q *Queue) collectCompletion(j *Job) {
	if !terminal(j.State) {
		return
	}
	state := completionState(j)
	if state.CollectedAt == nil {
		now := q.clock.Now()
		state.CollectedAt = &now
	}
}

func (q *Queue) ChangeCompletion(ctx context.Context, id string, request CompletionRequest) (CompletionReply, error) {
	var out CompletionReply
	err := q.updateRequest(ctx, func() error {
		j, err := q.completionJob(id)
		if err != nil {
			return err
		}
		switch request.Operation {
		case "collect":
			if !terminal(j.State) {
				return &HTTPError{Status: 409, Code: "job_not_terminal", Message: "result is not terminal"}
			}
			q.collectCompletion(j)
		case "claim", "delivered":
			if request.DeliveryID == "" {
				return &HTTPError{Status: 400, Code: "completion_invalid", Message: "delivery_id is required"}
			}
			state := completionState(j)
			if request.Operation == "delivered" {
				if state.DeliveryID != request.DeliveryID {
					return &HTTPError{Status: 409, Code: "completion_conflict", Message: "delivery_id does not match the claimed completion"}
				}
				if state.DeliveredAt == nil {
					now := q.clock.Now()
					state.DeliveredAt = &now
				}
			} else if terminal(j.State) && !cancelled(j) && state.CollectedAt == nil && state.SupersededBy == "" && state.DeliveredAt == nil {
				if state.DeliveryID == "" {
					state.DeliveryID = request.DeliveryID
				}
				out.Allowed = true
			}
			out.DeliveryID = state.DeliveryID
		default:
			return &HTTPError{Status: 400, Code: "completion_invalid", Message: "unknown completion operation"}
		}
		return nil
	}, nil)
	return out, err
}

func (a API) registerCompletions(mux *requestMux) {
	mux.HandleFunc("POST /api/jobs/{id}/completion", func(w http.ResponseWriter, r *http.Request) {
		var request CompletionRequest
		if !decode(w, r, &request) {
			return
		}
		reply, err := a.Queue.ChangeCompletion(r.Context(), r.PathValue("id"), request)
		respondOperation(w, reply, err, "completion_unavailable")
	})
}
func (c Client) ChangeCompletion(ctx context.Context, id string, request CompletionRequest) (CompletionReply, error) {
	var reply CompletionReply
	err := c.Call(ctx, "POST", "/api/jobs/"+url.PathEscape(id)+"/completion", request, &reply)
	return reply, err
}

// Older queues omit completion. Current queues advertise even an empty state
// on Job replies, so failures to persist collection never silently lose a wake.
func (c Client) CollectCompletion(ctx context.Context, j *Job) error {
	if j == nil || j.Completion == nil || !terminal(j.State) {
		return nil
	}
	_, err := c.ChangeCompletion(ctx, j.ID, CompletionRequest{Operation: "collect"})
	return err
}
