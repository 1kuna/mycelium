package myce

import (
	"context"
	"time"
)

// Match the default result window; key replay continues for submissionRetention
// after removal. Longer configured file/log windows extend job retention.
const jobHistoryRetention = 168 * time.Hour

// All changes use the existing undo journal and one FULL-synchronous commit.
// Deadlines avoid scanning history; each Tick retires at most fileReclaimBatch
// jobs. Pending file cleanup transfers to a compact, durable hash set.
func (q *Queue) pruneJobHistory(requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	if q.simulation {
		return nil
	}
	return q.updateRequest(requestCtx, func() error {
		now := q.clock.Now()
		for _, id := range q.records.history.due(now) {
			j := q.touchJob(id)
			if j == nil || !terminal(j.State) || len(j.RecoveredResults) > 0 {
				continue
			}
			if j.FinishedAt == nil {
				ended := now
				j.FinishedAt = &ended
				continue
			}
			if deliveryPending(j) || mutableJob(j) || hasJobFiles(j) && (!j.FilesExpired || j.FileReclamation == nil) || !q.records.logs.scheduled[id].IsZero() {
				continue
			}
			if j.FileReclamation != nil {
				for _, hash := range j.FileReclamation.Pending {
					if q.s.RetiredJobFiles == nil {
						q.s.RetiredJobFiles = map[string]bool{}
					}
					q.s.RetiredJobFiles[hash] = true
				}
			}
			q.deleteJob(id)
		}
		return nil
	}, nil)
}
