package myce

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Finished job records expire 14 days after they finish (MY-49: the
// orchestrator's slimming choice, changeable on evidence). Files go after 3
// days (MY-47), so by then a record holds only history; 14 days keeps two
// weeks of inspectable failures. No retention setting exists, so this is a
// constant. What stays is a one-line summary per job and everything learned
// from it, in the job_summaries table.
const jobRecordRetention = 14 * 24 * time.Hour

// A due record that is still blocked (files, logs, checkpoints, a dependent
// job) is looked at again after this delay instead of each maintenance pass.
// It only delays expiry of a record that is already past its window.
const jobExpiryRecheck = time.Hour

// Records expired per maintenance pass. Each costs one small row delete in
// the pass's commit (its summary is written before the lock), so a backlog of
// thousands drains within minutes without one long hold of q.mu.
const jobExpiryBatch = 64

// JobSummary is what remains of a job record after expiry.
type JobSummary struct {
	ID              string    `json:"id"`
	Project         string    `json:"project,omitempty"`
	Name            string    `json:"name,omitempty"`
	State           string    `json:"state"`
	FinishedAt      time.Time `json:"finished_at"`
	Host            string    `json:"host,omitempty"`
	DurationSeconds float64   `json:"duration_seconds,omitempty"`
	PeakMemoryGB    float64   `json:"peak_memory_gb,omitempty"`
}

func (s JobSummary) String() string {
	return fmt.Sprintf("%s %s/%s %s finished %s on %s, %.0fs, peak %.2f GB", s.ID, s.Project, s.Name, s.State, s.FinishedAt.UTC().Format(time.RFC3339), s.Host, s.DurationSeconds, s.PeakMemoryGB)
}

// expiredJobLearning keeps every input that learning reads from job records:
// the retained memory lower bound and recovery evidence, and which work
// observations its ledger learned (they stay pinned against evidence expiry).
// Durations and envelopes are already learned outside job records.
type expiredJobLearning struct {
	Memory       *learnedMemoryFact `json:"memory,omitempty"`
	WorkLearning []string           `json:"work_learning,omitempty"`
}

type expiredJobRow struct {
	id       string
	finished time.Time
	summary  []byte
	learned  []byte
	learning expiredJobLearning
}

const jobSummariesSchema = `CREATE TABLE IF NOT EXISTS job_summaries (id TEXT PRIMARY KEY, finished INTEGER NOT NULL, summary BLOB NOT NULL, learned BLOB)`

func summarizeJob(q *Queue, j *Job) JobSummary {
	s := JobSummary{ID: j.ID, Project: j.Spec.Project, Name: j.Spec.Name, State: j.State, Host: j.Machine}
	if m := q.s.Machines[j.Machine]; m != nil {
		s.Host = hostID(m)
	}
	if j.FinishedAt != nil {
		s.FinishedAt = *j.FinishedAt
		if j.StartedAt != nil && j.FinishedAt.After(*j.StartedAt) {
			s.DurationSeconds = j.FinishedAt.Sub(*j.StartedAt).Seconds()
		}
	}
	for _, facts := range j.MemoryAttempts {
		if p := facts.MemoryPeak; p != nil && p.HostGB != nil && validRate(*p.HostGB) {
			s.PeakMemoryGB = max(s.PeakMemoryGB, hostMemoryPeakGB(p))
		}
	}
	return s
}

// expiryBlocked names why a due record must stay. Running, queued and held
// work is never terminal; custody, files, logs, checkpoints, qualification
// and after: parents stay until they no longer hold anything.
func (q *Queue) expiryBlocked(id string, j *Job, now time.Time) bool {
	return j == nil || !terminal(j.State) || mutableJob(j) || j.Pause != nil || j.FinishedAt == nil ||
		now.Sub(*j.FinishedAt) < jobRecordRetention ||
		j.Spec.ExtensionCheck != nil || // Native qualification reads the accepted check job.
		hasJobFiles(j) && (!j.FilesExpired || j.FileReclamation == nil) ||
		!q.records.logs.scheduled[id].IsZero() ||
		q.records.checkpointExpiry[id].path != "" || q.records.checkpointUndated[id] ||
		q.records.dependents[id] > 0
}

// pruneJobHistory expires a bounded batch per maintenance pass. Encoding and
// the summary writes run outside q.mu; the deletion transaction rechecks each
// record under the lock and removes only records unchanged since the snapshot.
func (q *Queue) pruneJobHistory(requestContexts ...context.Context) error {
	ctx := requestContext(requestContexts)
	if q.simulation || q.db == nil {
		return nil
	}
	unlock := requestLock(ctx, &q.mu)
	if q.records == nil {
		q.rebuildRecords()
	}
	now := q.clock.Now()
	type candidate struct {
		job *Job
		row expiredJobRow
	}
	var candidates []candidate
	var unstamped []string
	for _, id := range q.records.history.dueN(now, jobExpiryBatch) {
		j := q.s.Jobs[id]
		if j != nil && terminal(j.State) && j.FinishedAt == nil && !mutableJob(j) {
			// Its window starts now; a restart must not restart it again.
			unstamped = append(unstamped, id)
			continue
		}
		if q.expiryBlocked(id, j, now) {
			if j != nil && terminal(j.State) {
				q.records.history.set(id, now.Add(jobExpiryRecheck))
			}
			continue
		}
		// Ownership of the deadline stays here until the deletion commits.
		q.records.history.set(id, now.Add(jobExpiryRecheck))
		row := expiredJobRow{id: id, finished: *j.FinishedAt}
		row.learning.Memory = learnedMemoryFactOf(j)
		for _, e := range j.LedgerEntries {
			if e.WorkLearning != nil {
				row.learning.WorkLearning = append(row.learning.WorkLearning, identityHash(e.WorkLearning.Key))
			}
		}
		summary := summarizeJob(q, j)
		var err error
		if row.summary, err = json.Marshal(summary); err != nil {
			unlock()
			return err
		}
		candidates = append(candidates, candidate{j, row})
	}
	unlock()
	if len(unstamped) > 0 {
		if err := q.updateRequest(ctx, func() error {
			for _, id := range unstamped {
				if j := q.s.Jobs[id]; j != nil && terminal(j.State) && j.FinishedAt == nil {
					ended := q.clock.Now()
					q.touchJob(id).FinishedAt = &ended
				}
			}
			return nil
		}, nil); err != nil {
			return err
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	// Summaries and learned inputs are durable before any record goes. A row
	// whose record then survives (a recheck failed) is ignored on load.
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for i := range candidates {
		row := &candidates[i].row
		if row.learning.Memory != nil || len(row.learning.WorkLearning) > 0 {
			if row.learned, err = json.Marshal(row.learning); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if _, err = tx.Exec("INSERT INTO job_summaries(id,finished,summary,learned) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET finished=excluded.finished, summary=excluded.summary, learned=excluded.learned", row.id, row.finished.UnixNano(), row.summary, row.learned); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	var expired []expiredJobRow
	err = q.updateTransaction(ctx, func() error {
		now := q.clock.Now()
		expired = expired[:0]
		for _, c := range candidates {
			id := c.row.id
			if q.s.Jobs[id] != c.job || q.expiryBlocked(id, c.job, now) {
				continue
			}
			if c.job.FileReclamation != nil {
				for _, hash := range c.job.FileReclamation.Pending {
					if q.s.RetiredJobFiles == nil {
						q.s.RetiredJobFiles = map[string]bool{}
					}
					q.s.RetiredJobFiles[hash] = true
				}
			}
			// A keyed answer keeps replaying the same job ID and final state,
			// not the full record it would otherwise retain.
			if digest := q.records.bindings[id]; digest != "" {
				if r := q.touchSubmission(digest); r != nil && r.Kind == submissionBinding {
					r.Job = replayJob(c.job)
				}
			}
			q.deleteJob(id)
			expired = append(expired, c.row)
		}
		return nil
	}, nil, false, func() {
		for _, row := range expired {
			q.learnExpired(row.learning)
		}
		if len(expired) > 0 {
			q.invalidateRetainedMemory()
		}
	})
	if err != nil {
		return err
	}
	// Log rows belong only to the record; delete them outside q.mu in
	// bounded statements, like log retention.
	for _, row := range expired {
		for _, table := range []string{"logs", "log_offsets", "environment_log_offsets"} {
			for {
				r, e := q.db.ExecContext(ctx, "DELETE FROM "+table+" WHERE rowid IN (SELECT rowid FROM "+table+" WHERE job=? LIMIT 512)", row.id)
				if e != nil {
					err = errors.Join(err, e)
					break
				}
				if n, e := r.RowsAffected(); e != nil || n < 512 {
					err = errors.Join(err, e)
					break
				}
			}
		}
	}
	return err
}

// replayJob is the keyed answer for an expired record: identity and outcome.
func replayJob(j *Job) *Job {
	return &Job{ID: j.ID, Spec: j.Spec, State: j.State, Attempt: j.Attempt, Machine: j.Machine, Queued: j.Queued,
		StartedAt: j.StartedAt, FinishedAt: j.FinishedAt, CancelledAt: j.CancelledAt, Reason: j.Reason}
}

// learnExpired installs a durable expired record's learning. Facts are
// appended to a fresh slice so snapshots taken outside q.mu stay valid.
func (q *Queue) learnExpired(l expiredJobLearning) {
	if l.Memory != nil {
		q.expiredMemory = append(q.expiredMemory[:len(q.expiredMemory):len(q.expiredMemory)], *l.Memory)
	}
	if len(l.WorkLearning) > 0 {
		if q.evidenceLearned == nil {
			q.evidenceLearned = map[string]bool{}
			q.evidenceLedgerCounts = map[string]int{}
		}
		for _, hash := range l.WorkLearning {
			q.evidenceLearned[hash] = true
		}
	}
}

// loadExpiredLearning restores learning from expired records at open. Rows
// whose record still exists (an interrupted expiry) are skipped: the record
// itself is the input.
func (q *Queue) loadExpiredLearning() error {
	rows, err := q.db.Query("SELECT id, learned FROM job_summaries WHERE learned IS NOT NULL")
	if err != nil {
		return err
	}
	defer rows.Close()
	q.expiredMemory = nil
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return err
		}
		if q.s.Jobs[id] != nil {
			continue
		}
		var l expiredJobLearning
		if err := json.Unmarshal(data, &l); err != nil {
			return err
		}
		q.learnExpired(l)
	}
	q.invalidateRetainedMemory()
	return rows.Err()
}

// expiredJobSummary answers lookups for an expired record. It reads one row
// by primary key and never takes q.mu.
func (q *Queue) expiredJobSummary(ctx context.Context, id string) (*JobSummary, error) {
	if q.db == nil {
		return nil, nil
	}
	var data []byte
	err := q.db.QueryRowContext(ctx, "SELECT summary FROM job_summaries WHERE id=?", id).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s JobSummary
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// expiredJobError replaces job_unknown for an expired record's exact ID.
func (q *Queue) expiredJobError(ctx context.Context, err error) error {
	var unknown *HTTPError
	if !errors.As(err, &unknown) || unknown.Code != "job_unknown" || unknown.JobID == "" {
		return err
	}
	s, lookup := q.expiredJobSummary(ctx, unknown.JobID)
	if lookup != nil || s == nil {
		return err
	}
	return &HTTPError{Status: 410, Code: "job_expired", JobID: s.ID, Message: "job record expired 14 days after it finished; summary: " + s.String()}
}
