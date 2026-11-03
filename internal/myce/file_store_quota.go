package myce

import (
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"
)

// fileStoreQuotaWindow is the rolling window for max_added_file_bytes_24h.
const fileStoreQuotaWindow = 24 * time.Hour

// ProjectFileStoreUsage is attribution under the shared fleet credential, not
// authorization. Byte counts use manifest sizes, each digest once per project;
// a digest held by two projects counts toward both, but once in UsedBytes.
type ProjectFileStoreUsage struct {
	Project string `json:"project"`
	// Digests protected by the project's unexpired jobs, keyed answers,
	// checkpoints and pending outputs. Released jobs drop out immediately.
	RetainedBytes uint64 `json:"retained_bytes"`
	// Digests first referenced by the project's jobs within the last 24 hours:
	// inputs at submission, results at completion. Release does not refund them.
	AddedBytes24h uint64 `json:"added_bytes_24h"`
	// Declared async result sizes of queued/running jobs not yet reported.
	ReservedOutputBytes  uint64 `json:"reserved_output_bytes,omitempty"`
	MaxRetainedFileBytes uint64 `json:"max_retained_file_bytes,omitempty"`
	MaxAddedFileBytes24h uint64 `json:"max_added_file_bytes_24h,omitempty"`
	ReservedFileBytes    uint64 `json:"reserved_file_bytes,omitempty"`
	// Why new non-production submissions are refused now; empty when admitted.
	Refusal string `json:"refusal,omitempty"`
}

type projectFileLedger struct {
	retained map[string]uint64
	first    map[string]time.Time
	size     map[string]uint64
	reserved uint64
}

type projectFileFact struct {
	project, hash string
	size          uint64
	at            time.Time
}

// Flat facts copied under q.mu: appends only, no maps per digest. Status builds
// the ledgers after unlocking, so the queue lock pays one reference walk (#80).
type projectFileFacts struct {
	retained, added []projectFileFact
	reserved        map[string]uint64
	limits          map[string]ProjectPolicy
}

// Report callback for collectFileReferences. Worker-local custody lives on the
// worker, not in the queue store.
func (f *projectFileFacts) retain(ref FileRef, owner FileKeepingReference) {
	if owner.Project != "" && owner.Kind != "worker_local_custody" {
		f.retained = append(f.retained, projectFileFact{project: owner.Project, hash: ref.SHA256, size: uint64(ref.SizeBytes)})
	}
}

// q.mu held. Inputs count at submission, results at completion.
func (q *Queue) noteProjectFileAdditions(f *projectFileFacts, jobs map[string]*Job) {
	now := q.clock.Now()
	if f.reserved == nil {
		f.reserved = map[string]uint64{}
	}
	f.added = slices.Grow(f.added, 4*len(jobs))
	note := func(project string, at time.Time, files FileManifest) {
		for _, ref := range files {
			f.added = append(f.added, projectFileFact{project: project, hash: ref.SHA256, size: uint64(ref.SizeBytes), at: at})
		}
	}
	for _, j := range jobs {
		if j == nil || j.Spec.Project == "" {
			continue
		}
		project := j.Spec.Project
		// submissionFiles without its per-job map; names are irrelevant here.
		note(project, j.Queued, j.Spec.Files)
		if len(j.Spec.SharedInputs) > 0 {
			note(project, j.Queued, sharedStoredFiles(j.Spec))
		}
		if j.Dependency != nil {
			note(project, j.Queued, j.Dependency.Files)
		}
		finished := now
		if j.FinishedAt != nil {
			finished = *j.FinishedAt
		}
		note(project, finished, j.OutputRefs)
		for _, a := range j.AttemptHistory {
			if a.Diagnostics != nil {
				note(project, finished, a.Diagnostics.Files)
			}
		}
		if j.FanOut != nil {
			for _, r := range j.FanOut.Results {
				note(project, finished, r.OutputRefs)
			}
		}
		if !terminal(j.State) && !cancelled(j) && !j.OutputAdmissionPending && j.Spec.OutputDelivery == asyncNAS && j.PendingOutputRefs == nil && j.Spec.Disk != nil {
			f.reserved[project] = addDiskBytes(f.reserved[project], j.Spec.Disk.OutputBytes)
		}
	}
}

// q.mu held. Keyed answers whose job has left the job table.
func (q *Queue) retiredSubmissionJobs(project string, into map[string]*Job) map[string]*Job {
	for _, r := range q.s.Submissions {
		if r.Job != nil && q.s.Jobs[r.Job.ID] == nil && (project == "" || r.Job.Spec.Project == project) {
			if into == nil {
				into = map[string]*Job{}
			}
			into[r.Job.ID] = r.Job
		}
	}
	return into
}

// q.mu held. One project's jobs, including its retired keyed answers.
func (q *Queue) projectFileJobs(project string) map[string]*Job {
	jobs := map[string]*Job{}
	for id, j := range q.s.Jobs {
		if j != nil && j.Spec.Project == project {
			jobs[id] = j
		}
	}
	return q.retiredSubmissionJobs(project, jobs)
}

// No lock needed: facts are detached copies.
func (f projectFileFacts) ledgers() map[string]*projectFileLedger {
	ledgers := map[string]*projectFileLedger{}
	ledger := func(name string) *projectFileLedger {
		if ledgers[name] == nil {
			ledgers[name] = &projectFileLedger{retained: map[string]uint64{}, first: map[string]time.Time{}, size: map[string]uint64{}}
		}
		return ledgers[name]
	}
	for _, r := range f.retained {
		ledger(r.project).retained[r.hash] = r.size
	}
	for _, a := range f.added {
		l := ledger(a.project)
		if first, ok := l.first[a.hash]; !ok || a.at.Before(first) {
			l.first[a.hash] = a.at
		}
		l.size[a.hash] = a.size
	}
	for name, bytes := range f.reserved {
		ledger(name).reserved = bytes
	}
	return ledgers
}

// Usage after admitting incoming inputs, excluding reserved/declared outputs.
func (l *projectFileLedger) usage(since time.Time, incoming FileManifest) (retained, added uint64) {
	for _, bytes := range l.retained {
		retained = addDiskBytes(retained, bytes)
	}
	for hash, at := range l.first {
		if at.After(since) {
			added = addDiskBytes(added, l.size[hash])
		}
	}
	counted := map[string]bool{}
	for _, ref := range incoming {
		if counted[ref.SHA256] {
			continue
		}
		counted[ref.SHA256] = true
		if _, ok := l.retained[ref.SHA256]; !ok {
			retained = addDiskBytes(retained, uint64(ref.SizeBytes))
		}
		if _, ok := l.first[ref.SHA256]; !ok {
			added = addDiskBytes(added, uint64(ref.SizeBytes))
		}
	}
	return retained, added
}

// The reason is the agent's next step: shrink what the store holds for them.
func fileStoreQuotaReason(project string, p ProjectPolicy, retained, added uint64) string {
	var over string
	if p.MaxRetainedFileBytes > 0 && retained > p.MaxRetainedFileBytes {
		over = fmt.Sprintf("retained=%d bytes would exceed max_retained_file_bytes=%d", retained, p.MaxRetainedFileBytes)
	} else if p.MaxAddedFileBytes24h > 0 && added > p.MaxAddedFileBytes24h {
		over = fmt.Sprintf("added in the last 24h=%d bytes would exceed max_added_file_bytes_24h=%d", added, p.MaxAddedFileBytes24h)
	} else {
		return ""
	}
	return fmt.Sprintf("project %s queue file-store quota: %s (including inputs and declared outputs); non-production submissions are refused until usage drops. Write large outputs to your project's own storage inside the job and return small receipts; release collected results with `myce file release plan --project %s JOB... > PLAN.json` then `myce file release apply --accept-expiry PLAN.json`; the 24h allowance frees as additions age out", project, over, project)
}

// q.mu held. Default off: projects without limits and production work skip
// the metadata walk. Completed results are never refused here; a project that
// overshot is refused at its next non-production submission instead.
func (q *Queue) fileStoreQuotaRefusal(s Spec, incoming FileManifest, declared uint64) error {
	if q.priorityMatch(s).Tier == "production" {
		return nil
	}
	return q.projectFileQuotaRefusal(s.Project, incoming, declared)
}

// q.mu held; the caller has already excluded production.
func (q *Queue) projectFileQuotaRefusal(project string, incoming FileManifest, declared uint64) error {
	p := q.s.Policies[project]
	if p.MaxRetainedFileBytes == 0 && p.MaxAddedFileBytes24h == 0 {
		return nil
	}
	// Walk only this project's jobs: cost follows the project, not the queue.
	jobs := q.projectFileJobs(project)
	var facts projectFileFacts
	q.collectFileReferences(false, nil, facts.retain, jobs)
	q.noteProjectFileAdditions(&facts, jobs)
	q.noteInputClaimFacts(&facts, project)
	l := facts.ledgers()[project]
	if l == nil {
		l = &projectFileLedger{}
	}
	retained, added := l.usage(q.clock.Now().Add(-fileStoreQuotaWindow), incoming)
	pending := addDiskBytes(l.reserved, declared)
	if reason := fileStoreQuotaReason(project, p, addDiskBytes(retained, pending), addDiskBytes(added, pending)); reason != "" {
		return &HTTPError{Status: http.StatusInsufficientStorage, Code: "project_file_store_quota", Message: reason}
	}
	return nil
}

// Status lists projects holding bytes or configured with a limit. Runs after
// q.mu is released, on facts captured with the status reference walk.
func (f projectFileFacts) usage(now time.Time) []ProjectFileStoreUsage {
	ledgers := f.ledgers()
	since := now.Add(-fileStoreQuotaWindow)
	for name := range f.limits {
		if ledgers[name] == nil {
			ledgers[name] = &projectFileLedger{}
		}
	}
	var out []ProjectFileStoreUsage
	for _, name := range slices.Sorted(maps.Keys(ledgers)) {
		l, p := ledgers[name], f.limits[name]
		u := ProjectFileStoreUsage{Project: name, ReservedOutputBytes: l.reserved, MaxRetainedFileBytes: p.MaxRetainedFileBytes, MaxAddedFileBytes24h: p.MaxAddedFileBytes24h}
		u.ReservedFileBytes = p.ReservedFileBytes
		u.RetainedBytes, u.AddedBytes24h = l.usage(since, nil)
		if u.RetainedBytes == 0 && u.AddedBytes24h == 0 && l.reserved == 0 && p.MaxRetainedFileBytes == 0 && p.MaxAddedFileBytes24h == 0 && p.ReservedFileBytes == 0 {
			continue
		}
		u.Refusal = fileStoreQuotaReason(name, p, addDiskBytes(u.RetainedBytes, l.reserved), addDiskBytes(u.AddedBytes24h, l.reserved))
		out = append(out, u)
	}
	return out
}
