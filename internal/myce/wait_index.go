package myce

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// WaitState is a scalar public observation, never an execution/diagnostic payload.
type WaitState struct {
	ID        string      `json:"id"`
	State     string      `json:"state"`
	Reason    string      `json:"reason,omitempty"`
	Revision  uint64      `json:"revision"`
	Spec      *WaitSpec   `json:"spec,omitempty"`
	StartedAt *time.Time  `json:"started_at,omitempty"`
	Finding   *JobFinding `json:"finding,omitempty"`
}

// WaitSpec contains only scalar observation metadata, never execution inputs.
// It lets new clients honor deadlines without a diagnostic GET bootstrap.
type WaitSpec struct {
	Deadline        *time.Time `json:"deadline,omitempty"`
	StartBy         *time.Time `json:"start_by,omitempty"`
	ExpectedMinutes float64    `json:"expected_minutes,omitempty"`
}

func waitTimestamp(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// Job projects bounded batch metadata for consumer observation.
func (state WaitState) Job() *Job {
	job := &Job{ID: state.ID, State: state.State, Reason: state.Reason, StartedAt: waitTimestamp(state.StartedAt), Finding: state.Finding}
	if state.Spec != nil {
		job.Spec.Deadline = waitTimestamp(state.Spec.Deadline)
		job.Spec.StartBy = waitTimestamp(state.Spec.StartBy)
		job.Spec.ExpectedMinutes = state.Spec.ExpectedMinutes
	}
	return job
}

type WaitRequest struct {
	IDs     []string          `json:"ids"`
	After   map[string]uint64 `json:"after,omitempty"`
	Seconds int               `json:"seconds,omitempty"`
}
type WaitReply struct {
	Jobs []WaitState `json:"jobs"`
}

// The writer holds q.mu before mu; readers take only mu. No waiter takes q.mu.
// Publication happens only after persistence succeeds. No timers, I/O, heavy
// projections, or subscriber goroutines live in the commit path.
type observationIntent struct{ project, class, priority string }

func (i observationIntent) spec() Spec {
	return Spec{Project: i.project, Class: i.class, Priority: i.priority}
}

type waitStateIndex struct {
	mu   sync.Mutex
	jobs map[string]WaitState
	// Only scalar intent and immutable policy tiers, never Job payloads. This
	// lets observation protect production without waiting for the control lock.
	intents    map[string]observationIntent
	policy     PriorityPolicy
	priorities *observationPrefix
	revision   uint64
	changed    chan struct{}
}

func (q *Queue) waitIndex() *waitStateIndex {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.waitStates == nil {
		q.waitStates = &waitStateIndex{jobs: map[string]WaitState{}, changed: make(chan struct{})}
		q.waitStates.policy = q.effectivePriorityPolicy()
		for _, r := range q.s.Submissions {
			if r.Kind == submissionBinding && r.Job != nil {
				q.waitStates.setIntent(r.JobID, r.Job)
			}
		}
		for id, j := range q.s.Jobs {
			q.waitStates.set(id, j)
		}
		for id, c := range q.calls {
			q.waitStates.set(id, waitCallJob(c))
		}
	}
	return q.waitStates
}
func waitCallJob(c *ModelCall) *Job {
	j := &Job{ID: c.ID, State: "queued", Spec: Spec{Project: c.Project, Class: c.Class, Priority: c.Priority}}
	if c.Machine != "" {
		j.State = "running"
	}
	if c.Error != "" {
		j.State, j.Reason = "failed", c.Error
	}
	return j
}
func (s *waitStateIndex) set(id string, j *Job) {
	old, exists := s.jobs[id]
	if j == nil {
		if !exists {
			return
		}
		delete(s.jobs, id)
	} else {
		s.setIntent(id, j)
		next := WaitState{ID: id, State: j.State, Reason: j.Reason, StartedAt: waitTimestamp(j.StartedAt)}
		if c := j.WorkerDataCleanup; c != nil {
			if next.Reason != "" {
				next.Reason += "; "
			}
			next.Reason += "worker data cleanup: " + c.State
		}
		if j.State == "failed" && (!exists || old.State != "failed") {
			next.Finding = terminalJobFinding(j)
		} else if exists && old.State == "failed" {
			next.Finding = old.Finding
		}
		next.Spec = &WaitSpec{Deadline: waitTimestamp(j.Spec.Deadline), StartBy: waitTimestamp(j.Spec.StartBy), ExpectedMinutes: j.Spec.ExpectedMinutes}
		if exists && old.State == next.State && old.Reason == next.Reason && reflect.DeepEqual(old.Spec, next.Spec) && reflect.DeepEqual(old.StartedAt, next.StartedAt) {
			return
		}
		s.revision++
		next.Revision = s.revision
		s.jobs[id] = next
	}
	close(s.changed)
	s.changed = make(chan struct{})
}

// Called under q.mu, before transaction records are discarded. Cold retained
// history is never rescanned for each waiter or each commit.
func (q *Queue) publishWaitStates(oldCalls map[string]*ModelCall) {
	s := q.waitStates
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	policy := q.effectivePriorityPolicy() // committed tiers are immutable
	if !reflect.DeepEqual(s.policy.Tiers, policy.Tiers) {
		s.policy = policy
		s.priorities = nil
		for id, intent := range s.intents {
			s.indexIntent(id, intent)
		}
	}
	for id := range q.records.jobs {
		s.set(id, q.s.Jobs[id])
		s.setIntent(id, q.observationJob(id))
	}
	for digest, old := range q.records.submissions {
		if old != nil && old.JobID != "" {
			s.setIntent(old.JobID, q.observationJob(old.JobID))
		}
		if r := q.s.Submissions[digest]; r != nil && r.JobID != "" {
			s.setIntent(r.JobID, q.observationJob(r.JobID))
		}
	}

	for id, c := range q.calls {
		s.set(id, waitCallJob(c))
	}
	for id := range oldCalls {
		if q.calls[id] == nil {
			s.set(id, q.s.Jobs[id])
			s.setIntent(id, q.observationJob(id))
		}
	}
}

func (s *waitStateIndex) observationPriority(id string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.priorities
	for i := 0; node != nil && i < len(id); i++ {
		node = node.children[id[i]]
	}
	if node != nil {
		if node.exact != nil {
			return node.exact.tier, node.exact.production
		}
		if len(id) >= 8 && node.best != nil {
			return node.best.tier, node.best.production
		}
	}
	return len(s.policy.Tiers), false
}
func (s *waitStateIndex) read(request WaitRequest) ([]WaitState, <-chan struct{}, bool, error) {
	return s.readContext(context.Background(), request)
}
func (s *waitStateIndex) readContext(ctx context.Context, request WaitRequest) ([]WaitState, <-chan struct{}, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if allocation := projectionAllocation(ctx); allocation != nil {
		// Three fixed slices can coexist through public reply and encoding.
		width := 3 * int64(reflect.TypeFor[WaitState]().Size())
		if int64(len(request.IDs)) > (allocation.limit-allocation.held)/width || !allocation.charge(width*int64(len(request.IDs))) {
			return nil, nil, false, projectionRefusal(allocation.limit - allocation.held)
		}
	}
	jobs := make([]WaitState, 0, len(request.IDs))
	changed := false
	for _, id := range request.IDs {
		j, ok := s.jobs[id]
		if !ok && len(id) >= 8 {
			for key, candidate := range s.jobs {
				if strings.HasPrefix(key, id) {
					if ok {
						return nil, nil, false, &HTTPError{Status: 409, Code: "job_ambiguous", Message: "job prefix is ambiguous"}
					}
					j, ok = candidate, true
				}
			}
		}
		if !ok {
			return nil, nil, false, &HTTPError{Status: 404, Code: "job_unknown", Message: "unknown job"}
		}
		changed = changed || request.After[j.ID] != j.Revision || terminal(j.State)
		if err := chargeProjectionValue(ctx, j); err != nil {
			return nil, nil, false, err
		}
		jobs = append(jobs, j)
	}
	return jobs, s.changed, changed, nil
}
func (s *waitStateIndex) wait(ctx context.Context, request WaitRequest) (WaitReply, error) {
	return s.waitFor(ctx, request, time.Duration(request.Seconds)*time.Second)
}
func (s *waitStateIndex) waitFor(ctx context.Context, request WaitRequest, duration time.Duration) (WaitReply, error) {
	// This is an HTTP transport wait, not queue/scheduler time. One bounded timer
	// per request; cancellation releases it. State/limits use the injected Clock.
	timer := time.NewTimer(duration)
	defer timer.Stop()
	var mark int64
	allocation := projectionAllocation(ctx)
	if allocation != nil {
		mark = allocation.held
	}
	for {
		if allocation != nil {
			allocation.rewind(mark)
		}
		jobs, wake, changed, err := s.readContext(ctx, request)
		if err != nil {
			return WaitReply{}, err
		}
		if changed || duration == 0 {
			return publicWaitReply(jobs), nil
		}
		select {
		case <-ctx.Done():
			return WaitReply{}, ctx.Err()
		case <-timer.C:
			return publicWaitReply(jobs), nil
		case <-wake:
		}
	}
}
func publicWaitReply(jobs []WaitState) WaitReply {
	for i := range jobs {
		jobs[i].Reason = statusJobValue(&Job{State: jobs[i].State, Reason: jobs[i].Reason}).Reason
	}
	return WaitReply{Jobs: jobs}
}
func (a API) registerWait(mux *requestMux) {
	// Initialize once, before any request. All subsequent reads avoid q.mu.
	index := a.Queue.waitIndex()
	mux.HandleFunc("POST /api/wait", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Myce-Wait", "batch-v1")
		var request WaitRequest
		if !decode(w, r, &request) {
			return
		}
		duration, err := a.Queue.requestBudgets.waitDuration(r, request.Seconds, a.Queue.clock.Now())
		if err != nil {
			respond(w, nil, err)
			return
		}
		limit, err := a.Queue.requestBudgets.pageLimit(r.Context(), reflect.TypeFor[WaitState]())
		if err != nil {
			respond(w, nil, err)
			return
		}
		w.Header().Set("X-Myce-Max-Wait-IDs", strconv.Itoa(limit))
		if len(request.IDs) == 0 || len(request.IDs) > limit {
			respond(w, nil, &HTTPError{Status: 413, Code: "wait_capacity_exceeded", Message: fmt.Sprintf("wait requires 1..%d IDs at current queue capacity", limit)})
			return
		}
		result, err := index.waitFor(r.Context(), request, duration)
		respondOperation(w, result, err, "status_unavailable")
	})
}
