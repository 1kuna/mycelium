package myce

import (
	"context"
	"errors"
	"net/url"
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

func (q *Queue) CancelJob(id string) (Cancellation, error) {
	var out Cancellation
	err := q.update(func() error {
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
			out = q.cancellation(j, q.s.Jobs[id] == nil)
			return nil
		}
		return &HTTPError{Status: 404, Code: "job_unknown", Message: "job is unknown", JobID: id}
	})
	return out, err
}

func (q *Queue) CancelSubmission(project, key string) (Cancellation, error) {
	if err := validateSubmissionKey(key); err != nil {
		return Cancellation{}, err
	}
	if project == "" {
		return Cancellation{}, errors.New("project is required")
	}
	var out Cancellation
	err := q.update(func() error {
		q.maintainSubmissions()
		if q.s.Submissions == nil {
			q.s.Submissions = map[string]*submissionRecord{}
		}
		digest := submissionDigest(project, key)
		record := q.s.Submissions[digest]
		if record == nil {
			now := q.clock.Now()
			record = &submissionRecord{Kind: submissionReservation, Project: project, Key: key, Created: &now, CancelledAt: &now}
			q.s.Submissions[digest] = record
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
		j := q.s.Jobs[record.JobID]
		removed := j == nil
		if removed {
			j = record.Job
		}
		if j == nil {
			return errors.New("submission binding has no retained job")
		}
		q.cancelJob(j)
		out = q.cancellation(j, removed)
		return nil
	})
	return out, err
}

// Called only under update. Keep terminal state/results, but persist exclusion
// even for failed jobs that an operator loss refund could otherwise requeue.
func (q *Queue) cancelJob(j *Job) {
	if j.CancelledAt == nil {
		now := q.clock.Now()
		j.CancelledAt = &now
	}
	for i := range j.Copies {
		j.Copies[i].Recoverable = false
	}
	if j.State == "queued" {
		q.finishCancellation(j)
	} else if !terminal(j.State) {
		if j.State == "running" {
			q.stop(j, "cancel requested")
		}
		j.Reason = "cancel requested"
	}
}

func (q *Queue) finishCancellation(j *Job) {
	j.State, j.StopAt = "cancelled", nil
	j.Reason = "cancel requested"
	if j.FinishedAt == nil {
		now := q.clock.Now()
		j.FinishedAt = &now
	}
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
	if j.Attempt > 0 {
		out.Release = releaseState("confirmed")
		if len(j.Copies) == 0 && !j.ReleaseConfirmed {
			out.Release = releaseState("pending")
		}
		for _, copy := range j.Copies {
			if !copy.ReleaseConfirmed && !(copy.Attempt == j.Attempt && j.ReleaseConfirmed) {
				out.Release = releaseState("pending")
			}
		}
	}
	for _, r := range q.s.HostReleases {
		if r.Job == j.ID {
			out.Release = releaseState("pending")
		}
	}
	// Do not return pointers into mutable queue state after unlocking.
	id := j.ID
	out.JobID = &id
	return out
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
			c.ReleaseConfirmed = true
			c.ReleaseReason = "execution_acknowledged"
			c.Recoverable = false
		}
	}
	if j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
		j.ReleaseConfirmed = true
	}
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
	if j := q.s.Jobs[id]; j != nil {
		return j
	}
	for _, record := range q.s.Submissions {
		if record.Kind == submissionBinding && record.JobID == id {
			return record.Job
		}
	}
	return nil
}

func releaseState(state string) *string { return &state }
