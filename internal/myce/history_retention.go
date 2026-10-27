package myce

import "time"

// Match the default result window; key replay continues for submissionRetention
// after removal. Longer configured file/log windows extend job retention.
const jobHistoryRetention = 168 * time.Hour

// All changes use the existing undo journal and one FULL-synchronous commit.
// Deadlines avoid scanning history; each Tick retires at most fileReclaimBatch
// jobs. File reclamation and bounded log deletion finish before their owner goes.
func (q *Queue) pruneJobHistory() error {
	if q.simulation {
		return nil
	}
	return q.update(func() error {
		now := q.clock.Now()
		for _, id := range q.records.history.due(now) {
			j := q.touchJob(id)
			if j == nil || !terminal(j.State) {
				continue
			}
			if j.FinishedAt == nil {
				ended := now
				j.FinishedAt = &ended
				continue
			}
			if mutableJob(j) || hasJobFiles(j) && (!j.FilesExpired || j.FileReclamation == nil || len(j.FileReclamation.Pending) > 0) || !q.records.logs.scheduled[id].IsZero() {
				continue
			}
			q.deleteJob(id)
		}
		return nil
	})
}
