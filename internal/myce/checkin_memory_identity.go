package myce

import "context"

type checkinMemoryIdentities struct {
	revision uint64
	values   map[*Job]string
}

// Plain recipe identities include every input reference. Snapshot under the
// mutex, hash outside it, then accept only if no transaction committed meanwhile.
// A stale snapshot falls back to the ordinary read-only planner's computation.
func (q *Queue) snapshotCheckinMemoryIdentities(ctx context.Context) checkinMemoryIdentities {
	unlock := requestLock(ctx, &q.mu)
	snapshot := checkinMemoryIdentities{revision: q.fileReferenceRevision}
	jobs := map[*Job]Job{}
	if ctx.Err() == nil {
		for _, j := range q.liveJobs() {
			if j.State == "queued" && plainMemoryWork(j.Spec) && j.Spec.FanOut == nil {
				jobs[j] = cloneJob(j)
			}
		}
	}
	unlock()
	snapshot.values = make(map[*Job]string, len(jobs))
	for original, copy := range jobs {
		snapshot.values[original] = profileMemoryIdentity(&copy)
	}
	return snapshot
}

// Caller holds q.mu. Pointer keys also prevent a replacement/rollback from
// lending an identity to a different job object with the same logical ID.
func (s checkinMemoryIdentities) current(q *Queue) map[*Job]string {
	if s.revision != q.fileReferenceRevision {
		return nil
	}
	return s.values
}
