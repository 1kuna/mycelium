package myce

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"
)

const DefaultFileRetention = 7 * 24 * time.Hour

// One minute covers the client's 30-second file request timeout plus multiple
// one-second maintenance ticks between upload chunks and submit/complete. This
// is a publication grace, not an extension of a terminal job's recovery window.
const filePublicationGrace = time.Minute

// Bound filesystem work while draining a large expiry backlog so maintenance
// remains responsive. Durable per-job progress avoids repeating completed work.
const fileReclaimBatch = 64

// Stored on the existing job record, not a separate blob index. A non-nil value
// with no pending hashes means this job's input/result reclamation is complete.
type FileReclamation struct {
	Pending []string `json:"pending,omitempty"`
}

func (q *Queue) ConfigureFileRetention(age time.Duration) error {
	if age <= 0 {
		return errors.New("file-retention must be positive")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.fileRetention = age
	return nil
}

func resultExpired() *HTTPError {
	return &HTTPError{Status: 410, Code: "result_expired", Message: rejectionMessages["result_expired"]}
}

func isResultExpired(err error) bool {
	var he *HTTPError
	return errors.As(err, &he) && he.Status == 410 && he.Code == "result_expired"
}

// Called in the ordinary queue transaction. Expiry is durable, including keyed
// answers; increasing retention later cannot restore deleted bytes.
func (q *Queue) maintainFileExpiry() {
	now := q.clock.Now()
	for _, j := range q.s.Jobs {
		if !terminal(j.State) || len(j.Spec.Files)+len(j.OutputRefs) == 0 {
			continue
		}
		// Some historical terminal paths did not stamp FinishedAt. Start their
		// window when first observed, rather than guessing from submission time.
		if j.FinishedAt == nil {
			ended := now
			j.FinishedAt = &ended
		}
		if !now.Before(j.FinishedAt.Add(q.fileRetention)) {
			j.FilesExpired = true
		}
		if j.FilesExpired && j.FileReclamation == nil {
			j.FileReclamation = &FileReclamation{}
			seen := map[string]bool{}
			for _, files := range []FileManifest{j.Spec.Files, j.OutputRefs} {
				for _, ref := range files {
					if !seen[ref.SHA256] {
						j.FileReclamation.Pending = append(j.FileReclamation.Pending, ref.SHA256)
						seen[ref.SHA256] = true
					}
				}
			}
		}
	}
}

// q.mu must be held. Only expired terminal inputs/results are candidates.
// Everything else with explicit custody is a keeping reference. In particular,
// checkpoint generations and catalog snapshots survive terminal file expiry.
func (q *Queue) fileReferences(pendingOnly bool) (expired, keep map[string]bool) {
	expired, keep = map[string]bool{}, map[string]bool{}
	add := func(to map[string]bool, files FileManifest) {
		for _, ref := range files {
			to[ref.SHA256] = true
		}
	}
	job := func(j *Job) {
		if j == nil {
			return
		}
		if terminal(j.State) && j.FilesExpired {
			if !pendingOnly {
				add(expired, j.Spec.Files)
				add(expired, j.OutputRefs)
			} else if j.FileReclamation != nil {
				for _, hash := range j.FileReclamation.Pending {
					expired[hash] = true
				}
			}
		} else {
			add(keep, j.Spec.Files)
			add(keep, j.OutputRefs)
		}
		for _, save := range j.FileCheckpoints {
			add(keep, save.Files)
		}
		if j.ResumeFiles != nil {
			add(keep, j.ResumeFiles.Files)
		}
		for _, e := range j.Extensions {
			add(keep, e.Assets)
		}
		for _, r := range j.EnvironmentRecipes {
			add(keep, r.Assets)
		}
		for _, recipes := range j.SidecarEnvironmentRecipes {
			for _, r := range recipes {
				add(keep, r.Assets)
			}
		}
	}
	for _, j := range q.s.Jobs {
		job(j)
	}
	for _, record := range q.s.Submissions {
		job(record.Job)
	}
	for _, e := range q.s.Extensions {
		add(keep, e.Assets)
	}
	for _, recipes := range q.environments.Environments {
		for _, r := range recipes {
			add(keep, r.Assets)
		}
	}
	for _, m := range q.models.Models {
		keep[m.SHA256] = true
		for _, f := range m.Files {
			keep[f.SHA256] = true
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slot.EnvironmentRecipe != nil {
			add(keep, slot.EnvironmentRecipe.Assets)
		}
	}
	return expired, keep
}

func (q *Queue) expiredBlob(hash string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	expired, keep := q.fileReferences(false)
	return expired[hash] && !keep[hash]
}

// Tick snapshots candidates, then fences reference publication and rechecks
// custody for each unlink. Neither filesystem access nor waiting for the fence
// holds q.mu. Unreferenced uploads are deliberately outside this collector.
func (q *Queue) reclaimJobFiles() error {
	q.mu.RLock()
	store := q.files
	expired, keep := q.fileReferences(true)
	candidates := expired
	revision := q.fileReferenceRevision
	cutoff := q.clock.Now().Add(-filePublicationGrace)
	q.mu.RUnlock()
	if store == nil {
		return nil
	}
	gone := map[string]bool{}
	checked := 0
	deferred := false
	for hash := range candidates {
		if (FileRef{SHA256: hash}).validate() != nil {
			continue
		}
		if keep[hash] {
			continue
		}
		if checked == fileReclaimBatch {
			break
		}
		checked++
		// The same fence covers tus publication and verify-then-commit callers.
		// Do not queue a writer behind slow hashing/uploads: RWMutex writer
		// preference would then stall unrelated publications. Tick retries later.
		if !q.fileLifecycle.TryLock() {
			deferred = true
			break
		}
		q.mu.RLock()
		// Every queue transaction and catalog change advances this fence. Reuse
		// the reference snapshot only while it still describes current custody.
		if revision != q.fileReferenceRevision {
			expired, keep = q.fileReferences(true)
			revision = q.fileReferenceRevision
		}
		remove := q.files == store && expired[hash] && !keep[hash]
		q.mu.RUnlock()
		var err error
		if remove {
			path := filepath.Join(store.root, "blobs", hash)
			var st os.FileInfo
			st, err = os.Lstat(path)
			// Stat inside the publication fence: a publisher/HEAD may have
			// refreshed mtime since the collection snapshot was taken.
			if err == nil && !st.ModTime().After(cutoff) {
				err = os.Remove(path)
				if err == nil {
					gone[hash] = true
				}
			}
			if errors.Is(err, os.ErrNotExist) {
				gone[hash] = true
				err = nil
			}
		}
		q.fileLifecycle.Unlock()
		if err != nil {
			log.Printf("file retention deferred hash=%s error=%q; retrying next tick", hash, err)
		}
	}
	q.mu.Lock()
	warn := deferred && !q.fileCollectionDeferred
	q.fileCollectionDeferred = deferred
	q.mu.Unlock()
	if warn {
		log.Printf("file retention deferred: publication or reference verification in progress; retrying next tick")
	}
	if len(gone) == 0 {
		return nil
	}
	return q.update(func() error {
		if q.files != store {
			return nil
		}
		mark := func(j *Job) {
			if j == nil || j.FileReclamation == nil {
				return
			}
			pending := j.FileReclamation.Pending[:0]
			for _, hash := range j.FileReclamation.Pending {
				if !gone[hash] {
					pending = append(pending, hash)
				}
			}
			j.FileReclamation.Pending = pending
		}
		for _, j := range q.s.Jobs {
			mark(j)
		}
		for _, record := range q.s.Submissions {
			mark(record.Job)
		}
		return nil
	})
}
