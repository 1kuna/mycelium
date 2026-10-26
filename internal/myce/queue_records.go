package myce

import (
	"container/heap"
	"encoding/json"
	"strings"
	"time"
)

// Retained answers remain addressable in s.Jobs/s.Submissions. Only live work
// participates in ordinary transactions and scheduling. Cold mutations must
// enter through touchJob/touchSubmission; insertions use putJob/putSubmission.
// This is an in-memory index, rebuilt from the same SQLite records on startup.
type queueRecords struct {
	catalog                   map[string]bool
	removed                   map[string]*Job
	references                map[string]bool
	referenceAdds             map[string]bool
	live                      map[string]*Job
	bindings                  map[string]string // retained job ID -> submission digest
	jobs                      map[string]*Job   // transaction undo; nil means absent before insertion
	submissions               map[string]*submissionRecord
	files                     recordDeadlines
	keys                      recordDeadlines
	jobCount, submissionCount int
	updating                  bool
}

type recordDeadline struct {
	id string
	at time.Time
}
type deadlineHeap []recordDeadline

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h deadlineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *deadlineHeap) Push(v any)        { *h = append(*h, v.(recordDeadline)) }
func (h *deadlineHeap) Pop() any          { a := *h; v := a[len(a)-1]; *h = a[:len(a)-1]; return v }

type recordDeadlines struct {
	heap      deadlineHeap
	scheduled map[string]time.Time
}

func (d *recordDeadlines) set(id string, at time.Time) {
	if d.scheduled == nil {
		d.scheduled = map[string]time.Time{}
	}
	if d.scheduled[id] == at {
		return
	}
	if at.IsZero() {
		delete(d.scheduled, id)
		return
	}
	d.scheduled[id] = at
	heap.Push(&d.heap, recordDeadline{id, at})
}
func (d *recordDeadlines) due(now time.Time) []string {
	var ids []string
	// Reuse the existing reclamation budget so equal-expiry producer waves do
	// not monopolize the queue lock. Exact key lookups expire their own record.
	for len(d.heap) > 0 && !d.heap[0].at.After(now) && len(ids) < fileReclaimBatch {
		v := heap.Pop(&d.heap).(recordDeadline)
		if d.scheduled[v.id] == v.at {
			delete(d.scheduled, v.id)
			ids = append(ids, v.id)
		}
	}
	return ids
}

func mutableJob(j *Job) bool {
	if !terminal(j.State) || j.Attempt > 0 && !j.ReleaseConfirmed {
		return true
	}
	for _, c := range j.Copies {
		if !c.ReleaseConfirmed {
			return true
		}
	}
	return false
}
func (q *Queue) indexJob(id string, j *Job) {
	if j == nil {
		delete(q.records.live, id)
		q.records.files.set(id, time.Time{})
		return
	}
	if mutableJob(j) {
		q.records.live[id] = j
	} else {
		delete(q.records.live, id)
	}
	var at time.Time
	if terminal(j.State) && hasJobFiles(j) && !j.FilesExpired {
		at = q.clock.Now()
		if j.FinishedAt != nil {
			at = j.FinishedAt.Add(q.fileRetention)
		}
	}
	q.records.files.set(id, at)
}
func (q *Queue) indexSubmission(id string, r *submissionRecord) {
	var at time.Time
	if r != nil {
		if r.JobID != "" {
			q.records.bindings[r.JobID] = id
		}
		if r.Kind == submissionReservation && r.Created != nil {
			at = r.Created.Add(submissionRetention)
		}
		if r.Kind == submissionBinding && r.RemovedAt != nil {
			at = r.RemovedAt.Add(submissionRetention)
		}
	}
	q.records.keys.set(id, at)
}
func (q *Queue) rebuildRecords() {
	q.records = &queueRecords{catalog: map[string]bool{}, removed: map[string]*Job{}, live: map[string]*Job{}, bindings: map[string]string{}, jobCount: len(q.s.Jobs), submissionCount: len(q.s.Submissions)}
	for id, j := range q.s.Jobs {
		q.indexJob(id, j)
	}
	for key := range q.persisted {
		if key == "state" || strings.HasPrefix(key, "extensions/") || strings.HasPrefix(key, "work_observations/") {
			q.records.catalog[key] = true
		}
	}
	for id, r := range q.s.Submissions {
		if j := q.s.Jobs[r.JobID]; j != nil {
			r.Job = j
		}
		q.indexSubmission(id, r)
		if q.s.Jobs[r.JobID] == nil && r.Job != nil && mutableJob(r.Job) {
			q.records.removed[r.JobID] = r.Job
		}
	}
	_, _, q.records.references = q.fileReferences(false)
}
func (q *Queue) liveJobs() map[string]*Job {
	if q.records == nil || q.simulation || !q.records.updating {
		return q.s.Jobs
	}
	return q.records.live
}
func (q *Queue) touchJob(id string) *Job {
	j := q.s.Jobs[id]
	if q.records != nil && q.records.updating {
		if _, ok := q.records.jobs[id]; !ok {
			var old *Job
			if j != nil {
				v := cloneJob(j)
				old = &v
			}
			q.records.jobs[id] = old
		}
	}
	return j
}
func (q *Queue) putJob(id string, j *Job) {
	q.touchJob(id)
	if q.s.Jobs[id] == nil && q.records != nil {
		q.records.jobCount++
	}
	q.s.Jobs[id] = j
	if q.records != nil {
		q.records.live[id] = j
	} // finish transitions retire at commit
}
func (q *Queue) deleteJob(id string) {
	q.touchJob(id)
	if q.s.Jobs[id] != nil && q.records != nil {
		q.records.jobCount--
	}
	delete(q.s.Jobs, id)
	if q.records != nil {
		delete(q.records.live, id)
	}
}
func (q *Queue) touchSubmission(id string) *submissionRecord {
	r := q.s.Submissions[id]
	if q.records != nil && q.records.updating {
		if _, ok := q.records.submissions[id]; !ok {
			var old *submissionRecord
			if r != nil {
				data, _ := json.Marshal(r)
				_ = json.Unmarshal(data, &old)
			}
			q.records.submissions[id] = old
		}
	}
	return r
}
func (q *Queue) putSubmission(id string, r *submissionRecord) {
	q.touchSubmission(id)
	if q.s.Submissions == nil {
		q.s.Submissions = map[string]*submissionRecord{}
	}
	if q.s.Submissions[id] == nil && q.records != nil {
		q.records.submissionCount++
	}
	q.s.Submissions[id] = r
	if q.records != nil && r.JobID != "" {
		q.records.bindings[r.JobID] = id
	}
}

func (q *Queue) beginRecords() ([]byte, error) {
	// Counts also let offline fixture/import code invalidate the derived index.
	imported := q.records != nil && (q.records.jobCount != len(q.s.Jobs) || q.records.submissionCount != len(q.s.Submissions))
	if q.records == nil || imported {
		q.rebuildRecords()
	}
	base := q.s
	base.Jobs = nil
	base.Submissions = nil
	old, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}
	q.records.updating = true
	q.records.jobs = map[string]*Job{}
	q.records.submissions = map[string]*submissionRecord{}
	if imported {
		for id := range q.s.Jobs {
			q.touchJob(id)
		}
		for id := range q.s.Submissions {
			q.touchSubmission(id)
		}
	}
	for id := range q.records.live {
		q.touchJob(id)
	}
	for id := range q.records.removed {
		q.touchSubmission(q.records.bindings[id])
	}
	return old, nil
}
func (q *Queue) rollbackRecords(old []byte) {
	jobs, submissions := q.s.Jobs, q.s.Submissions
	var base state
	_ = json.Unmarshal(old, &base)
	base.Jobs, base.Submissions = jobs, submissions
	q.s = base
	for id, j := range q.records.jobs {
		if j == nil {
			delete(jobs, id)
		} else {
			jobs[id] = j
		}
	}
	for id, r := range q.records.submissions {
		current := submissions[id]
		if current != nil && current.JobID != "" {
			delete(q.records.bindings, current.JobID)
			delete(q.records.removed, current.JobID)
		}
		if r == nil {
			delete(submissions, id)
		} else {
			submissions[id] = r
		}
	}
	q.records.jobCount, q.records.submissionCount = len(q.s.Jobs), len(q.s.Submissions)
	q.records.referenceAdds = nil
	q.commitRecords()
}
func (q *Queue) commitRecords() {
	for id := range q.records.jobs {
		q.indexJob(id, q.s.Jobs[id])
	}
	for id, old := range q.records.submissions {
		r := q.s.Submissions[id]
		if r != nil {
			if j := q.s.Jobs[r.JobID]; j != nil {
				r.Job = j
			}
		}
		if old != nil && old.JobID != "" && (r == nil || r.JobID != old.JobID) {
			delete(q.records.bindings, old.JobID)
			delete(q.records.removed, old.JobID)
		}
		q.indexSubmission(id, r)
		if r != nil && r.JobID != "" {
			delete(q.records.removed, r.JobID)
			if q.s.Jobs[r.JobID] == nil && r.Job != nil && mutableJob(r.Job) {
				q.records.removed[r.JobID] = r.Job
			}
		}
	}
	for hash := range q.records.referenceAdds {
		q.records.references[hash] = true
	}
	q.records.referenceAdds = nil
	q.records.updating = false
	q.records.jobs = nil
	q.records.submissions = nil
}

func (q *Queue) touchedJobs() map[string]*Job {
	out := map[string]*Job{}
	for id := range q.records.jobs {
		out[id] = q.s.Jobs[id]
	}
	for id := range q.records.submissions {
		if r := q.s.Submissions[id]; r != nil && r.Job != nil {
			out[r.JobID] = r.Job
		}
	}
	return out
}
