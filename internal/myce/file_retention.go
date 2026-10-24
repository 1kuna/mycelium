package myce

import (
	"errors"
	"os"
	"path/filepath"
	"time"
)

const DefaultFileRetention = 7 * 24 * time.Hour

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
	}
}

// q.mu must be held. Only expired terminal inputs/results are candidates.
// Everything else with explicit custody is a keeping reference. In particular,
// checkpoint generations and catalog snapshots survive terminal file expiry.
func (q *Queue) fileReferences() (expired, keep map[string]bool) {
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
		to := keep
		if terminal(j.State) && j.FilesExpired {
			to = expired
		}
		add(to, j.Spec.Files)
		add(to, j.OutputRefs)
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
	expired, keep := q.fileReferences()
	return expired[hash] && !keep[hash]
}

// Tick snapshots candidates, then fences reference publication and rechecks
// custody for each unlink. Neither filesystem access nor waiting for the fence
// holds q.mu. Unreferenced uploads are deliberately outside this collector.
func (q *Queue) reclaimJobFiles() error {
	q.mu.RLock()
	store := q.files
	expired, keep := q.fileReferences()
	candidates := expired
	revision := q.fileReferenceRevision
	q.mu.RUnlock()
	if store == nil {
		return nil
	}
	for hash := range candidates {
		if (FileRef{SHA256: hash}).validate() != nil {
			continue
		}
		if keep[hash] {
			continue
		}
		path := filepath.Join(store.root, "blobs", hash)
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		// The same fence covers tus publication and verify-then-commit callers.
		// Do not queue a writer behind slow hashing/uploads: RWMutex writer
		// preference would then stall unrelated publications. Tick retries later.
		if !q.fileLifecycle.TryLock() {
			continue
		}
		q.mu.RLock()
		// Every queue transaction and catalog change advances this fence. Reuse
		// the reference snapshot only while it still describes current custody.
		if revision != q.fileReferenceRevision {
			expired, keep = q.fileReferences()
			revision = q.fileReferenceRevision
		}
		remove := q.files == store && expired[hash] && !keep[hash]
		q.mu.RUnlock()
		var err error
		if remove {
			err = os.Remove(path)
			if errors.Is(err, os.ErrNotExist) {
				err = nil
			}
		}
		q.fileLifecycle.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
