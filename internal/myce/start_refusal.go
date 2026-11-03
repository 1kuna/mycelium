package myce

import "log"

// The ledger binds the refusal to exact attempt custody and persists it in the
// same transaction as the fence decision. A later observation cannot erase it.
func (q *Queue) recordStartRefusal(j *Job, predicate string) {
	if j == nil || j.State != "running" || j.StartedAt != nil || j.ReleaseConfirmed || cancelled(j) {
		return
	}
	log.Printf("event=start_fence_rejected job=%s attempt=%d predicate=%s", j.ID, j.Attempt, predicate)
	if recordedStartRefusal(j) != predicate {
		q.appendJobLedger(j, ledgerAttempt(j, "start_refused", predicate))
	}
}

func recordedStartRefusal(j *Job) string {
	for i := len(j.LedgerEntries) - 1; i >= 0; i-- {
		e := j.LedgerEntries[i]
		if e.Attempt != j.Attempt || e.Machine != j.Machine || e.Session != j.Session {
			continue // Late releases from older attempts may be interleaved.
		}
		switch e.Kind {
		case "start_refused":
			return e.Reason
		case "assigned", "start_authorized":
			return ""
		}
	}
	return ""
}

// A successful retry of the fence supersedes its refusal. A real failure after
// that permission must still spend the normal loss/retry budget.
func (q *Queue) clearStartRefusal(j *Job) {
	if recordedStartRefusal(j) != "" {
		q.appendJobLedger(j, ledgerAttempt(j, "start_authorized", "start_fence_passed"))
	}
}
