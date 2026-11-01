package myce

import (
	"context"
	"errors"
	"maps"
	"net/url"
	"slices"
	"sort"
	"time"
)

// Cancellation excludes new attempts/assignments. A previously assigned attempt
// may still begin before its next check; release is independent of exclusion.
type Cancellation struct {
	JobID          *string    `json:"job_id"`
	Project        string     `json:"project"`
	SubmissionKey  *string    `json:"submission_key"`
	State          string     `json:"state"`
	WillNotRun     bool       `json:"will_not_run"`
	Release        *string    `json:"release"`
	AcknowledgedAt *time.Time `json:"acknowledged_at"`
}

type CancelSubmissionRequest struct {
	Project       string `json:"project"`
	SubmissionKey string `json:"submission_key"`
}

func cancelled(j *Job) bool { return j.CancelledAt != nil || j.State == "cancelled" }

// Cancel preserves the queue's original Go interface for existing callers.
func (q *Queue) Cancel(id string) error { _, err := q.CancelJob(id); return err }

func (q *Queue) CancelJob(id string, requestContexts ...context.Context) (Cancellation, error) {
	requestCtx := requestContext(requestContexts)

	var out Cancellation
	err := q.updateRequest(requestCtx, func() error {
		q.maintainSubmissions()
		if q.retainedJob(id) == nil {
			jobs := []*Job{}
			for _, j := range q.s.Jobs {
				jobs = append(jobs, j)
			}
			for _, record := range q.s.Submissions {
				if record.Job != nil {
					jobs = append(jobs, record.Job)
				}
			}
			resolved, err := FindJob(jobs, id)
			if err != nil {
				return err
			}
			id = resolved.ID
		}
		if j := q.retainedJob(id); j != nil {
			q.cancelJob(j)
			q.maintainExtensionQualifications()
			out = q.cancellation(j, q.s.Jobs[id] == nil)
			return nil
		}
		return &HTTPError{Status: 404, Code: "job_unknown", Message: "job is unknown", JobID: id}
	}, nil)
	return out, err
}

func (q *Queue) CancelSubmission(project, key string, requestContexts ...context.Context) (Cancellation, error) {
	requestCtx := requestContext(requestContexts)

	if err := validateSubmissionKey(key); err != nil {
		return Cancellation{}, err
	}
	if project == "" {
		return Cancellation{}, errors.New("project is required")
	}
	var out Cancellation
	err := q.updateRequest(requestCtx, func() error {
		q.maintainSubmissions()
		if q.s.Submissions == nil {
			q.s.Submissions = map[string]*submissionRecord{}
		}
		digest := submissionDigest(project, key)
		q.expireSubmission(digest)
		record := q.touchSubmission(digest)
		if record == nil {
			now := q.clock.Now()
			record = &submissionRecord{Kind: submissionReservation, Project: project, Key: key, Created: &now, CancelledAt: &now}
			q.putSubmission(digest, record)
		}
		out = Cancellation{Project: project, SubmissionKey: &key, WillNotRun: true, Release: releaseState("not_applicable"), State: "absent"}
		if record.Kind == submissionExpired {
			out.State, out.Release = "unknown", nil
			return nil
		}
		if record.Kind == submissionReservation {
			if record.CancelledAt == nil {
				record.CancelledAt = record.Created // S02 reservations stored creation only.
			}
			at := record.CancelledAt.UTC()
			out.AcknowledgedAt = &at
			return nil
		}
		j := q.touchJob(record.JobID)
		removed := j == nil
		if removed {
			j = record.Job
		}
		if j == nil {
			return errors.New("submission binding has no retained job")
		}
		q.cancelJob(j)
		q.maintainExtensionQualifications()
		out = q.cancellation(j, removed)
		return nil
	}, nil)
	return out, err
}

// Called only under update. Keep terminal state/results, but persist exclusion
// even for failed jobs that an operator loss refund could otherwise requeue.
func (q *Queue) cancelJob(j *Job) {
	q.touchJob(j.ID)
	for _, child := range q.cancellationMembers(j) {
		if !cancelled(child) {
			q.cancelJob(child)
		}
	}
	if j.CancelledAt == nil {
		now := q.clock.Now()
		j.CancelledAt = &now
	}
	for i := range j.Copies {
		j.Copies[i].Recoverable = false
	}
	if j.State == "queued" || j.State == "paused" || j.State == "waiting" {
		q.finishCancellation(j)
	} else if !terminal(j.State) {
		if j.State == "running" {
			q.stop(j, "cancel requested")
		}
		j.Reason = "cancel requested"
	}
}

func (q *Queue) finishCancellation(j *Job) {
	outcome := attemptOutcome(j, Result{Interrupted: true})
	j.State, j.StopAt = "cancelled", nil
	j.Reason = "cancel requested"
	if j.FinishedAt == nil {
		now := q.clock.Now()
		j.FinishedAt = &now
	}
	q.recordAttemptOutcome(j, outcome)
}

func (q *Queue) cancellation(j *Job, removed bool) Cancellation {
	at := j.CancelledAt.UTC()
	out := Cancellation{JobID: &j.ID, Project: j.Spec.Project, State: j.State, WillNotRun: true, Release: releaseState("not_applicable"), AcknowledgedAt: &at}
	if j.Spec.SubmissionKey != "" {
		key := j.Spec.SubmissionKey
		out.SubmissionKey = &key
	}
	if removed {
		out.State = "removed"
	}
	out.Release = releaseState(q.observedRelease(j))
	// Do not return pointers into mutable queue state after unlocking.
	id := j.ID
	out.JobID = &id
	return out
}

func (q *Queue) cancellationAttempted(j *Job) bool {
	if j.Attempt > 0 {
		return true
	}
	for _, child := range q.cancellationMembers(j) {
		if q.cancellationAttempted(child) {
			return true
		}
	}
	return false
}

// CancellationScope enumerates the retained declaration and registered jobs.
// Undispatched pieces have no invented job or attempt. Members are flat and
// include waves and dependents (also descendants of dependent fan-outs).
type CancellationScope struct {
	Pieces  []CancellationPiece  `json:"pieces"`
	Members []CancellationMember `json:"members"`
}
type CancellationMember struct {
	JobID      string `json:"job_id"`
	State      string `json:"state"`
	WillNotRun bool   `json:"will_not_run"`
	Release    string `json:"release"`
}
type CancellationPiece struct {
	Key          string   `json:"key"`
	Materialized bool     `json:"materialized"`
	Accepted     bool     `json:"accepted"`
	WaveID       string   `json:"wave_id,omitempty"`
	WillNotRun   bool     `json:"will_not_run"`
	Release      string   `json:"release"`
	Dependents   []string `json:"dependents"`
}

func (q *Queue) observedRelease(j *Job) string {
	if q.jobReleasePending(j) {
		return "pending"
	}
	if q.cancellationAttempted(j) {
		return "confirmed"
	}
	return "not_applicable"
}

// Only projections call this, while holding the snapshot read lock or operating
// on detached state. It reads declaration/identity/custody only, with no I/O.
func (q *Queue) projectCancellation(out, stored *Job) {
	if stored.CancelledAt != nil {
		ack := q.cancellation(stored, false)
		out.Cancellation = &ack
	}
	if stored.Spec.FanOut == nil || stored.FanOut == nil {
		return
	}
	scope := &CancellationScope{Pieces: []CancellationPiece{}, Members: []CancellationMember{}}
	members := map[string]*Job{}
	var collect func(*Job)
	collect = func(parent *Job) {
		for _, child := range q.cancellationMembers(parent) {
			if members[child.ID] == nil {
				members[child.ID] = child
				collect(child)
			}
		}
	}
	collect(stored)
	waves := map[string]*Job{}
	for _, child := range members {
		scope.Members = append(scope.Members, CancellationMember{JobID: child.ID, State: child.State, WillNotRun: cancelled(child), Release: q.observedRelease(child)})
		if child.Batch != nil && child.Batch.Parent == stored.ID {
			for _, p := range child.Batch.Pieces {
				waves[p.Key] = child
			}
		}
	}
	sort.Slice(scope.Members, func(i, k int) bool { return scope.Members[i].JobID < scope.Members[k].JobID })
	for _, p := range stored.Spec.FanOut.Pieces {
		_, accepted := stored.FanOut.Results[p.Key]
		piece := CancellationPiece{Key: p.Key, Accepted: accepted, WillNotRun: cancelled(stored), Release: "not_applicable", Dependents: []string{}}
		if wave := waves[p.Key]; wave != nil {
			piece.Materialized, piece.WaveID, piece.Release = true, wave.ID, q.observedRelease(wave)
		}
		for _, child := range members {
			if a := child.Spec.After; a != nil && a.Job == stored.ID && a.Piece == p.Key {
				piece.Dependents = append(piece.Dependents, child.ID)
				if q.jobReleasePending(child) {
					piece.Release = "pending"
				} else if piece.Release == "not_applicable" && q.cancellationAttempted(child) {
					piece.Release = "confirmed"
				}
			}
		}
		sort.Strings(piece.Dependents)
		scope.Pieces = append(scope.Pieces, piece)
	}
	out.CancellationScope = scope
}

func (c Client) Cancel(ctx context.Context, id string) (Cancellation, error) {
	var out Cancellation
	err := c.Call(ctx, "POST", "/api/jobs/"+url.PathEscape(id)+"/cancel", nil, &out)
	return out, err
}
func (c Client) CancelSubmission(ctx context.Context, project, key string) (Cancellation, error) {
	var out Cancellation
	err := c.Call(ctx, "POST", "/api/jobs/cancel", CancelSubmissionRequest{Project: project, SubmissionKey: key}, &out)
	return out, err
}

// Only an exact execution acknowledgement (or the existing fresh-session
// cleanup protocol) can mark a copy released. Loss and grace expiry never do.
func (q *Queue) confirmCopyRelease(r Result) {
	j := q.retainedJob(r.ID)
	if j == nil {
		return
	}
	for i := range j.Copies {
		c := &j.Copies[i]
		if c.Attempt == r.Attempt && c.Machine == r.Machine && c.Session == r.Session {
			if !c.ReleaseConfirmed {
				if !j.Spec.Run.Docker && c.ProcessID == "" && j.Ledger != nil && !provenPrelaunchRefusal(j, r) {
					j.Ledger = &JobLedgerInfo{Complete: false, HighWater: len(j.LedgerEntries)}
				}
				// Existing exact release authority settles every generation in this copy.
				latest := map[string]JobLedgerEntry{}
				for _, e := range j.LedgerEntries {
					if e.Attempt == r.Attempt && e.Machine == r.Machine && e.Session == r.Session && e.CustodyID != "" {
						latest[e.CustodyID] = e
					}
				}
				for _, id := range slices.Sorted(maps.Keys(latest)) {
					e := latest[id]
					if e.Release != "confirmed" {
						at := q.clock.Now()
						e.Kind = "custody_released"
						e.Reason = "execution_acknowledged"
						e.Release = "confirmed"
						e.CustodyEnd = &at
						q.appendJobLedger(j, e)
					}
				}
				q.appendJobLedger(j, JobLedgerEntry{Attempt: r.Attempt, Machine: r.Machine, Session: r.Session, Kind: "released", Reason: "execution_acknowledged", Release: "confirmed"})
			}
			c.ReleaseConfirmed = true
			c.ReleaseReason = "execution_acknowledged"
			c.Recoverable = false
		}
	}
	if j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
		j.ReleaseConfirmed = true
	}
}

// Only a modern worker's durable refusal receipt proves an unstarted copy.
// Absence of a PID or Started alone says nothing about legacy execution.
func provenPrelaunchRefusal(j *Job, r Result) bool {
	cause := safeRuntimeCause(r.ExecutionCause)
	if r.Success || !r.Interrupted || r.ReleasePending || cause == nil || !cause.PrelaunchRefused {
		return false
	}
	for _, e := range j.LedgerEntries {
		if e.Attempt == r.Attempt && e.Machine == r.Machine && e.Session == r.Session && (e.Kind == "started" || e.CustodyID != "" || e.ProcessID != "") {
			return false
		}
	}
	return true
}

// Import only copies without S03 evidence. New copies carry an explicit pending
// reason, so reopening cannot mistake an untracked modern copy for legacy data.
// The old runtime's retained records were its source of outstanding custody.
func (q *Queue) importCancellationEvidence() bool {
	changed := false
	jobs := map[string]*Job{}
	for id, j := range q.s.Jobs {
		jobs[id] = j
	}
	for _, record := range q.s.Submissions {
		if record.Job != nil && jobs[record.JobID] == nil {
			jobs[record.JobID] = record.Job
		}
	}
	for _, j := range jobs {
		if j.CancelledAt == nil && (j.Reason == "cancel requested" || j.State == "cancelled") {
			now := q.clock.Now()
			j.CancelledAt = &now
			changed = true
		}
		for i := range j.Copies {
			c := &j.Copies[i]
			if c.ReleaseConfirmed || c.ReleaseReason != "" {
				continue
			}
			current := c.Machine == j.Machine && c.Session == j.Session && c.Attempt == j.Attempt
			_, retained := q.s.HostReleases[releaseKey(HostRelease{Job: j.ID, Machine: c.Machine, Session: c.Session, Attempt: c.Attempt})]
			c.ReleaseReason = "awaiting_release"
			if current && j.ReleaseConfirmed {
				c.ReleaseConfirmed, c.ReleaseReason = true, "current_release_confirmed"
			} else if !current && !retained {
				c.ReleaseConfirmed, c.ReleaseReason = true, "legacy_no_retained_custody"
			}
			changed = true
		}
	}
	return changed
}

func (q *Queue) confirmPreviousCopies(c Checkin) {
	old := q.s.Machines[c.Machine.ID]
	if !c.Fresh || old == nil || c.PreviousSession != old.Session || c.Machine.Session == old.Session {
		return
	}
	confirm := func(j *Job) {
		for _, copy := range j.Copies {
			if copy.Machine == c.Machine.ID && copy.Session == c.PreviousSession {
				q.confirmCopyRelease(Result{Execution: Execution{ID: j.ID, Attempt: copy.Attempt}, Machine: copy.Machine, Session: copy.Session})
			}
		}
	}
	for _, j := range q.s.Jobs {
		confirm(j)
	}
	for _, record := range q.s.Submissions {
		if record.Job != nil && q.s.Jobs[record.JobID] == nil {
			confirm(record.Job)
		}
	}
}

func (q *Queue) retainedJob(id string) *Job {
	if j := q.touchJob(id); j != nil {
		return j
	}
	if q.records != nil && q.records.updating {
		if key := q.records.bindings[id]; key != "" {
			if r := q.touchSubmission(key); r != nil {
				return r.Job
			}
		}
	} else {
		for _, record := range q.s.Submissions {
			if record.Kind == submissionBinding && record.JobID == id {
				return record.Job
			}
		}
	}
	return nil
}

func releaseState(state string) *string { return &state }

// Producer waves and registered consumers share the existing exact-copy custody.
// Existing-parent dependency validation keeps the traversal finite.
func (q *Queue) cancellationMembers(j *Job) []*Job {
	if j.FanOut == nil {
		return nil
	}
	if q.cancellationMembersByParent != nil {
		return q.cancellationMembersByParent[j.ID]
	}
	out := []*Job{}
	for _, id := range j.FanOut.Children {
		if child := q.retainedJob(id); child != nil {
			out = append(out, child)
		}
	}
	for _, child := range q.s.Jobs {
		if child.Spec.After != nil && child.Spec.After.Job == j.ID {
			out = append(out, child)
		}
	}
	for _, record := range q.s.Submissions {
		child := record.Job
		if child != nil && q.s.Jobs[child.ID] == nil && child.Spec.After != nil && child.Spec.After.Job == j.ID {
			out = append(out, child)
		}
	}
	return out
}

// One status capture projects many retained fan-outs under q.mu. Scanning the
// entire job/submission history for each parent (and again for release checks)
// starved control requests during a producer burst (#343). Batch the same
// relationships once. This view borrows state only while its caller holds q.mu;
// it is discarded after projection, so mutations need no new index lifecycle.
func (q *Queue) cancellationObservation() *Queue {
	view := &Queue{s: q.s, cancellationMembersByParent: map[string][]*Job{}}
	retained := make(map[string]*Job, len(q.s.Jobs))
	dependents := map[string][]*Job{}
	addDependent := func(j *Job) {
		if j.Spec.After != nil {
			parent := j.Spec.After.Job
			dependents[parent] = append(dependents[parent], j)
		}
	}
	for id, j := range q.s.Jobs {
		retained[id] = j
		addDependent(j)
	}
	for _, r := range q.s.Submissions {
		if j := r.Job; j != nil && q.s.Jobs[j.ID] == nil {
			addDependent(j)
			if r.Kind == submissionBinding {
				retained[r.JobID] = j
			}
		}
	}
	for id, j := range retained {
		if j.FanOut == nil {
			continue
		}
		members := make([]*Job, 0, len(j.FanOut.Children)+len(dependents[id]))
		for _, childID := range j.FanOut.Children {
			if child := retained[childID]; child != nil {
				members = append(members, child)
			}
		}
		view.cancellationMembersByParent[id] = append(members, dependents[id]...)
	}
	return view
}

func (q *Queue) jobReleasePending(j *Job) bool {
	if j.Attempt > 0 && len(j.Copies) == 0 && !j.ReleaseConfirmed {
		return true
	}
	for _, copy := range j.Copies {
		if !copy.ReleaseConfirmed && !(copy.Attempt == j.Attempt && j.ReleaseConfirmed) {
			return true
		}
	}
	for _, r := range q.s.HostReleases {
		if r.Job == j.ID {
			return true
		}
	}
	for _, child := range q.cancellationMembers(j) {
		if q.jobReleasePending(child) {
			return true
		}
	}
	return false
}
