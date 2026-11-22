package myce

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	checkpointInputExpired     = "checkpoint_input_expired"
	checkpointInputExpiryDue   = "checkpoint_input_expiry_due"
	checkpointInputReplacement = "import the bytes with myce file put or use a retained NAS path, then submit with the updated input and a new key if replacing an accepted job"
)

const predicateCheckpointInputExpiry AdmissionPredicate = checkpointInputExpiryDue

// CheckpointInputExpiry is queue evidence for a declared shared input backed by
// a settled NAS checkpoint. The deadline is the earliest permitted removal.
// ExpiredAt is the time the queue accepted a worker's removal report.
type CheckpointInputExpiry struct {
	Field     string    `json:"field"`
	Deadline  time.Time `json:"deadline,omitempty,omitzero"`
	ExpiredAt time.Time `json:"expired_at,omitempty,omitzero"`
}

func (e CheckpointInputExpiry) removedMessage() string {
	return fmt.Sprintf("%s references a NAS checkpoint whose removal was confirmed at %s", e.Field, e.ExpiredAt.UTC().Format(time.RFC3339))
}

func (e CheckpointInputExpiry) dueMessage() string {
	return fmt.Sprintf("%s references a NAS checkpoint eligible for removal since %s", e.Field, e.Deadline.UTC().Format(time.RFC3339))
}

func CheckpointInputAdvisories(j *Job) []string {
	var messages []string
	for _, e := range j.CheckpointInputExpiries {
		if e.ExpiredAt.IsZero() {
			messages = append(messages, fmt.Sprintf("%s references a NAS checkpoint eligible for removal at %s; for a durable input, %s", e.Field, e.Deadline.UTC().Format(time.RFC3339), checkpointInputReplacement))
		}
	}
	return messages
}

// Only the two supported folder layouts can name a checkpoint. Match the
// recorded folder as well as the ID to avoid classifying unrelated NAS paths.
// All operations are lexical or read in-memory records under the queue lock.
func (q *Queue) checkpointInputSource(nas string) *Job {
	first, rest, _ := strings.Cut(nas, "/")
	if j := q.checkpointInputJob(first, first); j != nil {
		return j
	}
	id, _, _ := strings.Cut(rest, "/")
	if id != "" && checkpointShard(id) == first {
		return q.checkpointInputJob(id, first+"/"+id)
	}
	return nil
}

func (q *Queue) checkpointInputJob(id, folder string) *Job {
	j := q.s.Jobs[id]
	if j == nil && q.records != nil {
		if r := q.s.Submissions[q.records.bindings[id]]; r != nil {
			j = r.Job
		}
	}
	if j != nil && j.ID == id {
		if recorded, ok := checkpointRecordFolder(q.root, j); ok && recorded == folder {
			return j
		}
	}
	return nil
}

func (q *Queue) checkpointInputExpiries(s Spec) []CheckpointInputExpiry {
	if len(s.SharedInputs) == 0 {
		return nil
	}
	var expiries []CheckpointInputExpiry
	add := func(field, nas string) {
		if nas == "" {
			return
		}
		source := q.checkpointInputSource(nas)
		if source == nil {
			return
		}
		e := CheckpointInputExpiry{Field: field}
		if source.CheckpointExpired != nil {
			e.ExpiredAt = *source.CheckpointExpired
		} else if _, ok := checkpointExpirable(q.root, source); q.fileCheckpoints || !ok {
			return
		}
		if end, ok := checkpointEndTime(source); ok {
			e.Deadline = end.Add(q.expiryGrace())
		}
		if !e.Deadline.IsZero() || !e.ExpiredAt.IsZero() {
			expiries = append(expiries, e)
		}
	}
	for name, input := range s.SharedInputs {
		field := "shared_inputs." + name
		add(field+".nas", input.NAS)
		add(field+".manifest_file", input.ManifestFile)
	}
	slices.SortFunc(expiries, func(a, b CheckpointInputExpiry) int {
		return strings.Compare(a.Field, b.Field)
	})
	return expiries
}

func (q *Queue) maintainCheckpointInputs(j *Job) {
	if (j.State != "queued" && j.State != "paused") || len(j.Spec.SharedInputs) == 0 {
		return
	}
	expiries := q.checkpointInputExpiries(j.Spec)
	if !slices.Equal(j.CheckpointInputExpiries, expiries) {
		q.touchJob(j.ID).CheckpointInputExpiries = expiries
	}
	for _, e := range expiries {
		if e.ExpiredAt.IsZero() {
			continue
		}
		if j.FanOut != nil {
			// Fully dispatched parents settle through their children. Let
			// already assigned waves finish before failing undispatched work.
			if j.FanOut.Next >= len(j.Spec.FanOut.Pieces) {
				return
			}
			for _, id := range j.FanOut.Children {
				if child := q.s.Jobs[id]; child != nil && !terminal(child.State) {
					return
				}
			}
		}
		now := q.clock.Now()
		q.touchJob(j.ID)
		q.closeAdmissionWait(j, "failed")
		j.State, j.DiagnosticCode, j.Reason, j.FinishedAt = "failed", checkpointInputExpired, e.removedMessage(), &now
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
		return
	}
}

func (q *Queue) checkpointInputHold(j *Job) *Hold {
	if j.State != "queued" || len(j.CheckpointInputExpiries) == 0 {
		return nil
	}
	now := q.clock.Now()
	for _, e := range j.CheckpointInputExpiries {
		if !e.ExpiredAt.IsZero() {
			return newHold(checkpointInputExpiryDue, e.removedMessage())
		}
		if !e.Deadline.IsZero() && !now.Before(e.Deadline) {
			return newHold(checkpointInputExpiryDue, e.dueMessage())
		}
	}
	return nil
}
