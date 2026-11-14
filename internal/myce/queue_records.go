package myce

import (
	"container/heap"
	"reflect"
	"time"
)

// Set only by test builds; the product path has no full-diff checker.
var auditQueueRecords func(*Queue) func() error

// Retained answers remain addressable in s.Jobs/s.Submissions. Only live work
// participates in ordinary transactions and scheduling. Cold mutations must
// enter through touchJob/touchSubmission; insertions use putJob/putSubmission.
// Touch before the first nested mutation so both rollback and the durable dirty
// set own the original decision. Derived diagnostics use their view journals.
// This is an in-memory index, rebuilt from the same SQLite records on startup.
type queueRecords struct {
	healthCount               int
	healthChanged             map[int]bool
	statusHistory             map[string]statusHistoryEntry
	catalogUndo               map[string]catalogPrevious
	machines                  map[string]*Machine
	reasons                   map[string]string
	staleness                 map[string]string
	catalogCounts             [4]int
	machineCount              int
	removed                   map[string]*Job
	references                map[string]bool
	referenceAdds             map[string]bool
	live                      map[string]*Job
	bindings                  map[string]string // retained job ID -> submission digest
	jobs                      map[string]*Job   // transaction undo; nil means absent before insertion
	submissions               map[string]*submissionRecord
	history                   recordDeadlines
	logs                      recordDeadlines
	logsDone                  map[string]bool
	files                     recordDeadlines
	keys                      recordDeadlines
	jobCount, submissionCount int
	updating                  bool
	checkpointExpiry          map[string]checkpointExpiryEntry // Settled job ID -> folder and earliest expiry (#894).
	checkpointUndated         map[string]bool                  // Settled jobs with no timestamp to start a grace period.
	after                     map[string]string                // Job ID -> its after: parent.
	dependents                map[string]int                   // Parent ID -> retained jobs naming it in after:.
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
	// Reuse the existing reclamation budget so equal-expiry producer waves do
	// not monopolize the queue lock. Exact key lookups expire their own record.
	return d.dueN(now, fileReclaimBatch)
}
func (d *recordDeadlines) dueN(now time.Time, limit int) []string {
	var ids []string
	for len(d.heap) > 0 && !d.heap[0].at.After(now) && len(ids) < limit {
		v := heap.Pop(&d.heap).(recordDeadline)
		if d.scheduled[v.id] == v.at {
			delete(d.scheduled, v.id)
			ids = append(ids, v.id)
		}
	}
	return ids
}

func mutableJob(j *Job) bool {
	// Unresolved parked custody cannot be retired by ordinary history/key expiry.
	if parkedStorageCustody(j) {
		return true
	}
	return deliveryPending(j) || !terminal(j.State) || attemptReleasePending(j)
}
func (q *Queue) indexJob(id string, j *Job) {
	if j == nil || q.records.live[id] == nil || !mutableJob(j) {
		q.invalidateRetainedMemory()
	}
	if q.evidenceLearned == nil {
		q.evidenceLearned = map[string]bool{}
		q.evidenceLedgerCounts = map[string]int{}
	}
	if j != nil {
		for _, e := range j.LedgerEntries[min(len(j.LedgerEntries), q.evidenceLedgerCounts[id]):] {
			if e.WorkLearning != nil {
				q.evidenceLearned[identityHash(e.WorkLearning.Key)] = true
			}
		}
		q.evidenceLedgerCounts[id] = len(j.LedgerEntries)
	}
	if j == nil || terminal(j.State) {
		for key := range q.evidenceDeferred {
			q.enqueueEvidence(key)
		}
		q.evidenceDeferred = nil
	}

	if j == nil || terminal(j.State) {
		q.evidenceGeneration++
	}
	q.indexCheckpointExpiry(id, j)
	q.indexDependency(id, j)
	if j == nil {
		delete(q.records.statusHistory, id)
		delete(q.records.live, id)
		q.records.files.set(id, time.Time{})
		q.records.history.set(id, time.Time{})
		q.records.logs.set(id, time.Time{})
		delete(q.records.logsDone, id)
		return
	}
	if mutableJob(j) {
		q.records.live[id] = j
		delete(q.records.statusHistory, id)
	} else {
		delete(q.records.live, id)
		q.indexStatusHistory(id, j)
	}
	q.records.files.set(id, q.fileExpiryDeadline(j))
	var history, logs time.Time
	if terminal(j.State) && !mutableJob(j) {
		ended := q.clock.Now()
		if j.FinishedAt != nil {
			ended = *j.FinishedAt
		}
		// pruneJobHistory rechecks every blocker when the record comes due.
		history = ended.Add(jobRecordRetention)
		if q.logRetention > 0 && !q.records.logsDone[id] {
			logs = ended.Add(q.logRetention)
		}
	}
	q.records.history.set(id, history)
	q.records.logs.set(id, logs)
}

// indexDependency counts after: references so expiry keeps every parent a
// retained job names, without scanning history.
func (q *Queue) indexDependency(id string, j *Job) {
	if q.records.after == nil {
		q.records.after, q.records.dependents = map[string]string{}, map[string]int{}
	}
	parent := ""
	if j != nil && j.Spec.After != nil {
		parent = j.Spec.After.Job
	}
	old, had := q.records.after[id]
	if had && old == parent {
		return
	}
	if had {
		if q.records.dependents[old]--; q.records.dependents[old] <= 0 {
			delete(q.records.dependents, old)
		}
		delete(q.records.after, id)
	}
	if parent != "" {
		q.records.after[id] = parent
		q.records.dependents[parent]++
	}
}
func (q *Queue) indexSubmission(id string, r *submissionRecord) {
	var at time.Time
	if r != nil {
		if r.JobID != "" {
			q.records.bindings[r.JobID] = id
		}
		if r.Kind == submissionReservation && r.Created != nil {
			at = time.Time{}
		}
		if r.Kind == submissionBinding && r.RemovedAt != nil {
			at = time.Time{}
		}
	}
	q.records.keys.set(id, at)
}
func (q *Queue) rebuildRecords() {
	q.invalidateRetainedMemory()
	q.records = &queueRecords{logsDone: map[string]bool{}, removed: map[string]*Job{}, live: map[string]*Job{}, bindings: map[string]string{}, jobCount: len(q.s.Jobs), submissionCount: len(q.s.Submissions)}
	for id, j := range q.s.Jobs {
		q.indexJob(id, j)
	}
	q.records.catalogCounts = q.catalogCounts()
	q.records.machineCount = len(q.s.Machines)
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
	if q.observationLive != nil {
		return q.observationLive
	}
	if q.records == nil || q.simulation || !q.records.updating {
		return q.s.Jobs
	}
	return q.records.live
}
func (q *Queue) touchJob(id string) *Job {
	q.enqueueMaintenance(id, false)
	j := q.s.Jobs[id]
	if q.records != nil && q.records.live[id] == nil {
		q.invalidateRetainedMemory()
	}
	if q.records != nil && q.records.updating {
		if _, ok := q.records.jobs[id]; !ok {
			var old *Job
			if j != nil {
				// Admission evidence is immutable and replaced on publication or
				// closure. The undo journal can share it, like immutable ledger
				// entries, instead of JSON-copying every retained explanation on
				// each heartbeat. Public job copies still own their nested data.
				input := *j
				input.AdmissionWait = nil
				v := cloneJob(&input)
				v.AdmissionWait = j.AdmissionWait
				if reason, ok := q.records.reasons[id]; ok {
					v.Reason = reason
				}
				old = &v
			}
			q.records.jobs[id] = old
			// Jobs and their retained answers alias the same mutable object.
			// Journal both before exposing either pointer, even if the operation
			// aborts before submission maintenance runs.
			if key := q.records.bindings[id]; key != "" {
				q.touchSubmission(key)
			}
		}
	}
	return j
}
func (q *Queue) putJob(id string, j *Job) {
	if q.s.Jobs[id] == nil && j.Attempt == 0 {
		j.Ledger = &JobLedgerInfo{Complete: true}
		j.LedgerEntries = nil
	}
	q.touchJob(id)
	if q.s.Jobs[id] == nil && q.records != nil {
		q.records.jobCount++
	}
	q.s.Jobs[id] = j
	q.maintenanceJob(j)
	if q.records != nil {
		q.records.live[id] = j
	} // finish transitions retire at commit
}
func (q *Queue) deleteJob(id string) {
	q.maintenanceRemoved(q.s.Jobs[id])
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
				copy := *r // Metadata timestamps are replaced, never mutated in place.
				copy.Job = nil
				old = &copy
			}
			// Install the entry before touchJob: the two undo paths journal each
			// other. A live job and its keyed answer share one owned snapshot,
			// avoiding a second deep copy on every control transaction.
			q.records.submissions[id] = old
			if r != nil {
				live := q.s.Jobs[r.JobID]
				if live != nil {
					q.touchJob(r.JobID)
				}
				if live != nil && r.Job == live {
					old.Job = q.records.jobs[r.JobID]
				} else if r.Job != nil {
					copy := cloneJob(r.Job)
					old.Job = &copy
				}
			}
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

func (q *Queue) catalogCounts() [4]int {
	return [4]int{len(q.s.Extensions), len(q.s.WorkObservations), len(q.s.StorageObservations), len(q.s.ExternalMemory)}
}
func (q *Queue) beginRecords() (recordUndo, error) {
	imported := q.records != nil && (q.records.jobCount != len(q.s.Jobs) || q.records.submissionCount != len(q.s.Submissions))
	catalogImport := q.records != nil && q.records.catalogCounts != q.catalogCounts()
	machineImport := q.records != nil && q.records.machineCount != len(q.s.Machines)
	if q.records == nil || imported {
		q.rebuildRecords()
	}
	base := queueBase(q.s, false)
	// Diagnostic generations are replaced, never mutated in place. Borrow the
	// baseline for rollback just as observation capture does; copying thousands
	// of histories on every unrelated writer only extends the central hold.
	efficiencyHealth := base.EfficiencyHealth
	base.EfficiencyHealth = nil
	old := copyObservationValue(reflect.ValueOf(base)).Interface().(state)
	old.EfficiencyHealth = efficiencyHealth
	q.records.healthChanged = map[int]bool{}
	q.records.updating = true
	q.records.jobs = map[string]*Job{}
	q.records.submissions = map[string]*submissionRecord{}
	q.records.catalogUndo = map[string]catalogPrevious{}
	q.records.machines = map[string]*Machine{}
	q.records.reasons = map[string]string{}
	q.records.staleness = map[string]string{}
	if imported {
		for id := range q.s.Jobs {
			q.touchJob(id)
		}
		for id := range q.s.Submissions {
			q.touchSubmission(id)
		}
	}
	if catalogImport {
		for id := range q.s.Extensions {
			q.touchCatalog("extensions/" + id)
		}
		for id := range q.s.WorkObservations {
			q.touchCatalog("work_observations/" + id)
		}
		for id := range q.s.StorageObservations {
			q.touchCatalog("storage_observations/" + id)
		}
		for id := range q.s.ExternalMemory {
			q.touchCatalog("external_memory/" + id)
		}
	}
	if machineImport {
		for id := range q.s.Machines {
			q.touchMachine(id)
		}
	}
	for id := range q.records.removed {
		q.touchSubmission(q.records.bindings[id])
	}
	catalogs := state{Extensions: q.s.Extensions, WorkObservations: q.s.WorkObservations, StorageObservations: q.s.StorageObservations, ExternalMemory: q.s.ExternalMemory}
	return recordUndo{base: old, catalogs: catalogs, machines: q.s.Machines, health: q.s.Health.Events, efficiencyJSON: q.efficiencyHealthJSON}, nil
}
func (q *Queue) rollbackRecords(old recordUndo) {
	q.evidenceGeneration++
	q.memoryObservationIndex = nil
	q.invalidateRetainedMemory()
	jobs, submissions := q.s.Jobs, q.s.Submissions
	base := old.base
	base.Extensions, base.WorkObservations, base.StorageObservations = old.catalogs.Extensions, old.catalogs.WorkObservations, old.catalogs.StorageObservations
	base.ExternalMemory = old.catalogs.ExternalMemory
	base.Health.Events = old.health
	base.Jobs, base.Submissions = jobs, submissions
	q.s = base
	q.efficiencyHealthJSON = old.efficiencyJSON
	q.s.Machines = old.machines
	for id, m := range q.records.machines {
		if m == nil {
			delete(q.s.Machines, id)
		} else {
			q.s.Machines[id] = m
		}
	}
	q.restoreCatalogs()

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
	for id, reason := range q.records.reasons {
		if _, durable := q.records.jobs[id]; durable {
			continue
		}
		if j := jobs[id]; j != nil {
			j.Reason = reason
		}
	}
	for id, reason := range q.records.staleness {
		if _, durable := q.records.catalogUndo["work_observations/"+id]; durable {
			continue
		}
		if p, ok := q.s.WorkObservations[id]; ok {
			p.StaleReason = reason
			q.s.WorkObservations[id] = p
		}
	}

	q.records.jobCount, q.records.submissionCount = len(q.s.Jobs), len(q.s.Submissions)
	q.records.referenceAdds = nil
	q.commitRecords()
	q.refreshHostMemory()
}
func (q *Queue) commitRecords() {
	q.records.healthCount = len(q.s.Health.Events)
	q.records.healthChanged = nil
	q.records.catalogCounts = q.catalogCounts()
	q.records.machineCount = len(q.s.Machines)
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
	q.records.catalogUndo = nil
	q.records.machines, q.records.reasons, q.records.staleness = nil, nil, nil
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
