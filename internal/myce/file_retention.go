package myce

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"
)

const DefaultFileRetention = 7 * 24 * time.Hour

// Six hours covers the gap from the first file's publish/HEAD to submit or
// complete: later multi-GB uploads, slow NAS, and a queue outage or deploy can
// delay that reference for hours. Only refreshed bytes receive this publication
// grace; it does not extend a terminal job's result-recovery window.
const filePublicationGrace = 6 * time.Hour

// Bound filesystem work while draining a large expiry backlog so maintenance
// remains responsive. Durable per-job progress avoids repeating completed work.
const fileReclaimBatch = 64

// Stored on the existing job record, not a separate blob index. A non-nil value
// with no pending hashes means this job's input/result reclamation is complete.
type FileReclamation struct {
	Pending []string `json:"pending,omitempty"`
}

const jobInputUploadPurpose = "job-input"

// Creation and publication callers hold fileLifecycle.RLock. Only newly
// created job-input identities acquire eligibility; reusing an unmarked import
// never makes it collectible. A generic import permanently releases the marker.
func (q *Queue) recordFileUpload(hash, purpose string, created bool) error {
	return q.update(func() error {
		if purpose != jobInputUploadPurpose {
			delete(q.s.JobInputUploads, hash)
		} else if published, marked := q.s.JobInputUploads[hash]; marked {
			if !published.IsZero() {
				q.s.JobInputUploads[hash] = q.clock.Now()
			}
		} else if created {
			if q.s.JobInputUploads == nil {
				q.s.JobInputUploads = map[string]time.Time{}
			}
			q.s.JobInputUploads[hash] = time.Time{}
		}
		return nil
	})
}

func (q *Queue) publishFileUpload(hash string) error {
	return q.update(func() error {
		if _, marked := q.s.JobInputUploads[hash]; marked {
			q.s.JobInputUploads[hash] = q.clock.Now()
		}
		return nil
	})
}

// Called before the transaction commits a job/reference. Terminal retention
// takes over once attached; other durable custody also releases orphan eligibility.
func (q *Queue) releaseFileUploadMarkers() {
	if len(q.s.JobInputUploads) == 0 {
		return
	}
	_, _, referenced := q.fileReferences(false)
	for hash := range q.s.JobInputUploads {
		if referenced[hash] {
			delete(q.s.JobInputUploads, hash)
		}
	}
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
func (q *Queue) fileReferences(pendingOnly bool) (expired, keep, referenced map[string]bool) {
	expired, keep, referenced = map[string]bool{}, map[string]bool{}, map[string]bool{}
	add := func(to map[string]bool, files FileManifest) {
		for _, ref := range files {
			to[ref.SHA256] = true
		}
	}
	extension := func(e Extension) {
		add(keep, e.Assets)
		for _, v := range e.Variants {
			for _, r := range v.Environments {
				add(keep, r.Assets)
			}
		}
	}
	job := func(j *Job) {
		if j == nil {
			return
		}
		// Retained identities, including expired answers and completed
		// reclamations, must never be mistaken for pre-submit orphans.
		add(referenced, j.Spec.Files)
		add(referenced, j.OutputRefs)
		if j.FileReclamation != nil {
			for _, hash := range j.FileReclamation.Pending {
				referenced[hash] = true
			}
		}
		if j.FanOut != nil {
			for _, result := range j.FanOut.Results {
				add(referenced, result.OutputRefs)
				if !terminal(j.State) || !j.FilesExpired {
					add(keep, result.OutputRefs)
				}
			}
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
			extension(e)
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
		extension(e)
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
	for hash := range keep {
		referenced[hash] = true
	}
	return expired, keep, referenced
}

func (q *Queue) expiredBlob(hash string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	expired, keep, _ := q.fileReferences(false)
	return expired[hash] && !keep[hash]
}

// Tick snapshots candidates, then fences reference publication and rechecks
// custody for each unlink. Neither filesystem access nor waiting for the fence
// holds q.mu. Only positively marked job-input uploads can be orphans.
func (q *Queue) reclaimJobFiles() error {
	q.mu.RLock()
	store := q.files
	expired, keep, referenced := q.fileReferences(true)
	candidates := expired
	revision := q.fileReferenceRevision
	cutoff := q.clock.Now().Add(-filePublicationGrace)
	orphanCutoff := q.clock.Now().Add(-q.fileRetention)
	orphans := map[string]time.Time{}
	for hash, published := range q.s.JobInputUploads {
		if !published.IsZero() && !published.After(orphanCutoff) && !referenced[hash] {
			orphans[hash] = published
		}
	}
	q.mu.RUnlock()
	if store == nil {
		return nil
	}
	gone := map[string]bool{}
	checked := 0
	deferred := false
	reclaim := func(hash string, orphan bool) {
		// The same fence covers tus publication and verify-then-commit callers.
		// Do not queue a writer behind slow hashing/uploads: RWMutex writer
		// preference would then stall unrelated publications. Tick retries later.
		if !q.fileLifecycle.TryLock() {
			deferred = true
			return
		}
		q.mu.RLock()
		// Every queue transaction and catalog change advances this fence. Reuse
		// the reference snapshot only while it still describes current custody.
		if revision != q.fileReferenceRevision {
			expired, keep, referenced = q.fileReferences(true)
			revision = q.fileReferenceRevision
		}
		remove := q.files == store && expired[hash] && !keep[hash]
		if orphan {
			published, marked := q.s.JobInputUploads[hash]
			remove = q.files == store && marked && published == orphans[hash] && !referenced[hash]
		}
		q.mu.RUnlock()
		var err error
		if remove {
			path := filepath.Join(store.root, "blobs", hash)
			var st os.FileInfo
			st, err = os.Lstat(path)
			// Stat inside the publication fence: a publisher/HEAD may have
			// refreshed mtime since the collection snapshot was taken.
			if err == nil && !st.ModTime().After(cutoff) && (!orphan || st.Mode().IsRegular() && !st.ModTime().After(orphanCutoff)) {
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
	for hash := range candidates {
		if (FileRef{SHA256: hash}).validate() != nil || keep[hash] {
			continue
		}
		if checked == fileReclaimBatch || deferred {
			break
		}
		checked++
		reclaim(hash, false)
	}
	for hash := range orphans {
		if checked == fileReclaimBatch || deferred {
			break
		}
		checked++
		reclaim(hash, true)
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
		for hash := range gone {
			// A re-publication between unlink and this transaction owns a newer
			// marker. Never erase its eligibility/progress with an older result.
			if q.s.JobInputUploads[hash] == orphans[hash] {
				delete(q.s.JobInputUploads, hash)
			}
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
