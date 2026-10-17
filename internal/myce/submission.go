package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const submissionRetention = 30 * 24 * time.Hour
const (
	submissionBinding     = "binding"
	submissionReservation = "reservation"
	submissionExpired     = "expired"
)

// Stored in the queue state, in the same SQLite transaction as Jobs. The map
// key is a digest of the project/key tuple, including for expired records. An
// expired record contains nothing else and is never removed. Missing fields in
// pre-key queue states need no rewrite or separate schema migration.
type submissionRecord struct {
	Kind        string     `json:"kind"`
	Project     string     `json:"project,omitempty"`
	Key         string     `json:"key,omitempty"`
	Fingerprint string     `json:"fingerprint,omitempty"`
	JobID       string     `json:"job_id,omitempty"`
	Created     *time.Time `json:"created,omitempty"`
	RemovedAt   *time.Time `json:"removed_at,omitempty"`
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`
	// Last durable answer, retained if the job is removed. There is currently no
	// job-pruning API; any future removal must run through Queue.update.
	Job *Job `json:"job,omitempty"`
}

func submissionDigest(project, key string) string {
	data, _ := json.Marshal([2]string{project, key})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func submissionFingerprint(s Spec) (string, error) {
	s.SubmissionKey = ""
	// encoding/json sorts object keys; Spec contains only submitted fields. This
	// runs before resolution, learned needs and the default production deadline.
	data, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func submissionFailure(expired bool) *HTTPError {
	if expired {
		return &HTTPError{Status: 410, Code: "submission_key_expired", Message: "submission key has expired and cannot create a job"}
	}
	return &HTTPError{Status: 409, Code: "submission_key_conflict", Message: "submission key is already bound to a different job spec"}
}

// Called under the update lock before lookup, from maintenance, and before
// persistence so the retained answer includes every committed job mutation.
func (q *Queue) maintainSubmissions() {
	now := q.clock.Now()
	for digest, record := range q.s.Submissions {
		var expires time.Time
		switch record.Kind {
		case submissionReservation:
			expires = record.Created.Add(submissionRetention)
		case submissionBinding:
			if j := q.s.Jobs[record.JobID]; j != nil {
				record.Job = j
				continue // The binding lives at least as long as the job.
			}
			if record.RemovedAt == nil {
				removed := now
				record.RemovedAt = &removed
			}
			expires = record.RemovedAt.Add(submissionRetention)
		default:
			continue
		}
		// The full record is retained on [creation/removal, expiry).
		if !now.Before(expires) {
			q.s.Submissions[digest] = &submissionRecord{Kind: submissionExpired}
		}
	}
}

// The caller holds the queue's update lock. CancelledAt remains set after a
// reservation binds, so subsequent POSTs retain its spec-independent precedence.
func (q *Queue) existingSubmission(s Spec, fingerprint string) (*Job, error) {
	if s.SubmissionKey == "" {
		return nil, nil
	}
	record := q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)]
	if record == nil {
		return nil, nil
	}
	if record.Kind == submissionExpired {
		return nil, submissionFailure(true)
	}
	if record.Kind == submissionReservation {
		cancelled := *record.Created
		j := &Job{ID: ID(), Spec: Spec{Project: record.Project, SubmissionKey: record.Key}, State: "cancelled", CancelledAt: &cancelled, Queued: cancelled, FinishedAt: &cancelled, Reason: "cancelled before submission"}
		q.s.Jobs[j.ID] = j
		record.Kind = submissionBinding
		record.JobID = j.ID
		record.Fingerprint = fingerprint
		record.CancelledAt = &cancelled
		record.Job = j
	}
	if record.CancelledAt == nil && record.Fingerprint != fingerprint {
		return nil, submissionFailure(false)
	}
	j := record.Job
	if live := q.s.Jobs[record.JobID]; live != nil {
		j = live
	}
	copy := cloneJob(j)
	return &copy, nil
}
