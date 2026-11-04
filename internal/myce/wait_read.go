package myce

import "context"

// waitJobSnapshot avoids diagnostic capture, placement and memory projections
// for pending batch jobs. Exact IDs are one map lookup and a scalar copy under
// q.mu. Prefixes use the same ambiguity rules as ordinary job GETs. Terminal
// jobs retain the full public reply (including cancellation/release details).
// Model calls use their ordinary projection, which derives state separately.
func (q *Queue) waitJobSnapshot(id string, ctx context.Context) (*Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	unlock := requestLock(ctx, &q.mu)
	j, err := q.findObservationJob(id)
	if err != nil {
		unlock()
		return nil, err
	}
	out := &Job{ID: j.ID, State: j.State, Reason: j.Reason}
	full := terminal(j.State) || q.calls[j.ID] != nil
	unlock()
	if full {
		return q.jobSnapshot(out.ID, ctx)
	}
	// Reuse the public Job sanitizer on our detached scalar copy. Worker
	// availability errors can otherwise expose private details in Reason.
	out.Reason = statusJobValue(out).Reason
	return out, nil
}
