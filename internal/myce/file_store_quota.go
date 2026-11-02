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
	// Why new non-production submissions are refused now; empty when admitted.
	Refusal string `json:"refusal,omitempty"`
}

type projectFileLedger struct {
	retained map[string]uint64
	first    map[string]time.Time
	size     map[string]uint64
	reserved uint64
}

// q.mu held. Metadata only: no store inventory, hashing or other I/O.
// project=="" builds every project's ledger (status); otherwise only that one.
func (q *Queue) projectFileLedgers(project string) map[string]*projectFileLedger {
	ledgers := map[string]*projectFileLedger{}
	ledger := func(name string) *projectFileLedger {
		if ledgers[name] == nil {
			ledgers[name] = &projectFileLedger{retained: map[string]uint64{}, first: map[string]time.Time{}, size: map[string]uint64{}}
		}
		return ledgers[name]
	}
	q.collectFileReferences(false, nil, func(ref FileRef, owner FileKeepingReference) {
		// Worker-local custody lives on the worker, not in the queue store.
		if owner.Project == "" || owner.Kind == "worker_local_custody" || project != "" && owner.Project != project {
			return
		}
		ledger(owner.Project).retained[ref.SHA256] = uint64(ref.SizeBytes)
	})
	now := q.clock.Now()
	seen := map[string]bool{}
	visit := func(j *Job) {
		if j == nil || seen[j.ID] || project != "" && j.Spec.Project != project {
			return
		}
		seen[j.ID] = true
		l := ledger(j.Spec.Project)
		note := func(at time.Time, files FileManifest) {
			for _, ref := range files {
				if first, ok := l.first[ref.SHA256]; !ok || at.Before(first) {
					l.first[ref.SHA256] = at
				}
				l.size[ref.SHA256] = uint64(ref.SizeBytes)
			}
		}
		note(j.Queued, submissionFiles(j.Spec))
		if j.Dependency != nil {
			note(j.Queued, j.Dependency.Files)
		}
		finished := now
		if j.FinishedAt != nil {
			finished = *j.FinishedAt
		}
		note(finished, j.OutputRefs)
		for _, a := range j.AttemptHistory {
			if a.Diagnostics != nil {
				note(finished, a.Diagnostics.Files)
			}
		}
		if j.FanOut != nil {
			for _, r := range j.FanOut.Results {
				note(finished, r.OutputRefs)
			}
		}
		if !terminal(j.State) && !cancelled(j) && !j.OutputAdmissionPending && j.Spec.OutputDelivery == asyncNAS && j.PendingOutputRefs == nil && j.Spec.Disk != nil {
			l.reserved = addDiskBytes(l.reserved, j.Spec.Disk.OutputBytes)
		}
	}
	for _, j := range q.s.Jobs {
		visit(j)
	}
	for _, record := range q.s.Submissions {
		visit(record.Job)
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
	return fmt.Sprintf("project %s queue file-store quota: %s (including inputs and declared outputs); non-production submissions are refused until usage drops. Write large outputs to your project's own storage inside the job and return small receipts; release collected results with `myce file release plan --project %s JOB...` then `myce file release apply --accept-expiry PLAN.json`; the 24h allowance frees as additions age out", project, over, project)
}

// q.mu held. Default off: projects without limits and production work skip
// the metadata walk. Completed results are never refused here; a project that
// overshot is refused at its next non-production submission instead.
func (q *Queue) fileStoreQuotaRefusal(s Spec, incoming FileManifest, declared uint64) error {
	p := q.s.Policies[s.Project]
	if p.MaxRetainedFileBytes == 0 && p.MaxAddedFileBytes24h == 0 || q.priorityMatch(s).Tier == "production" {
		return nil
	}
	l := q.projectFileLedgers(s.Project)[s.Project]
	if l == nil {
		l = &projectFileLedger{}
	}
	retained, added := l.usage(q.clock.Now().Add(-fileStoreQuotaWindow), incoming)
	pending := addDiskBytes(l.reserved, declared)
	if reason := fileStoreQuotaReason(s.Project, p, addDiskBytes(retained, pending), addDiskBytes(added, pending)); reason != "" {
		return &HTTPError{Status: http.StatusInsufficientStorage, Code: "project_file_store_quota", Message: reason}
	}
	return nil
}

// q.mu held. Status lists projects holding bytes or configured with a limit.
func (q *Queue) projectFileStoreUsage() []ProjectFileStoreUsage {
	ledgers := q.projectFileLedgers("")
	since := q.clock.Now().Add(-fileStoreQuotaWindow)
	var out []ProjectFileStoreUsage
	for name, p := range q.s.Policies {
		if (p.MaxRetainedFileBytes > 0 || p.MaxAddedFileBytes24h > 0) && ledgers[name] == nil {
			ledgers[name] = &projectFileLedger{}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(ledgers)) {
		l, p := ledgers[name], q.s.Policies[name]
		u := ProjectFileStoreUsage{Project: name, ReservedOutputBytes: l.reserved, MaxRetainedFileBytes: p.MaxRetainedFileBytes, MaxAddedFileBytes24h: p.MaxAddedFileBytes24h}
		u.RetainedBytes, u.AddedBytes24h = l.usage(since, nil)
		if u.RetainedBytes == 0 && u.AddedBytes24h == 0 && l.reserved == 0 && p.MaxRetainedFileBytes == 0 && p.MaxAddedFileBytes24h == 0 {
			continue
		}
		u.Refusal = fileStoreQuotaReason(name, p, addDiskBytes(u.RetainedBytes, l.reserved), addDiskBytes(u.AddedBytes24h, l.reserved))
		out = append(out, u)
	}
	return out
}
