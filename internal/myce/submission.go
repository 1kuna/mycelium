package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"time"
)

const submissionRetention = 30 * 24 * time.Hour

var submissionKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

func validateSubmissionKey(key string) error {
	if !submissionKeyPattern.MatchString(key) {
		return &HTTPError{Status: 400, Code: "submission_key_invalid", Message: "submission key must be 1-128 characters from A-Z, a-z, 0-9, dot, underscore, colon or hyphen"}
	}
	return nil
}

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
	if q.records == nil {
		q.rebuildRecords()
	}
	now := q.clock.Now()
	for id := range q.records.jobs {
		if digest := q.records.bindings[id]; digest != "" {
			record := q.touchSubmission(digest)
			if j := q.s.Jobs[id]; j != nil {
				record.Job = j
				record.RemovedAt = nil
			} else if record.RemovedAt == nil {
				removed := now
				record.RemovedAt = &removed
				q.records.keys.set(digest, removed.Add(submissionRetention))
			}
		}
	}
	for _, digest := range q.records.keys.due(now) {
		record := q.s.Submissions[digest]
		if record == nil {
			continue
		}
		if record.Kind == submissionBinding && q.s.Jobs[record.JobID] != nil {
			continue
		}
		q.putSubmission(digest, &submissionRecord{Kind: submissionExpired})
	}

}

// The caller holds the queue's update lock. CancelledAt remains set after a
// reservation binds, so subsequent POSTs retain its spec-independent precedence.
func (q *Queue) existingSubmission(s Spec, fingerprint string) (*Job, error) {
	if s.SubmissionKey == "" {
		return nil, nil
	}
	record := q.touchSubmission(submissionDigest(s.Project, s.SubmissionKey))
	if record == nil {
		return nil, nil
	}
	if record.Kind == submissionExpired {
		return nil, submissionFailure(true)
	}
	if record.Kind == submissionReservation {
		cancelled := *record.Created
		j := &Job{ID: ID(), Spec: Spec{Project: record.Project, SubmissionKey: record.Key}, State: "cancelled", CancelledAt: &cancelled, Queued: cancelled, FinishedAt: &cancelled, Reason: "cancelled before submission"}
		q.putJob(j.ID, j)
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
