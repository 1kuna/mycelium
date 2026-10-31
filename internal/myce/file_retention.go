package myce

import (
	"context"
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

const attemptOutputUploadPurpose = "attempt-output"
const jobInputUploadPurpose = "job-input"
const fileImportUploadPurpose = "import"

// Persist eligibility before creating internal bytes, so rejected submissions
// and crashes before the reference commit still have a cleanup owner. Existing
// unmarked imports are never enrolled merely because a recipe reuses them.
func (q *Queue) recordBundleUpload(hash string, created bool, requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)
	if !created {
		return nil
	}
	unlockRequest := requestReadLock(requestCtx, &q.mu)
	_, marked := q.s.BundleUploads[hash]
	unlockRequest()
	if marked {
		return nil
	}
	return q.updateRequest(requestCtx, func() error { q.markBundleUpload(hash); return nil }, nil)
}

func (q *Queue) markBundleUpload(hash string) {
	if q.s.BundleUploads == nil {
		q.s.BundleUploads = map[string]time.Time{}
	}
	q.s.BundleUploads[hash] = q.clock.Now()
}

// Creation and publication callers hold fileLifecycle.RLock. Only newly
// created job-input identities acquire eligibility; reusing an unmarked import
// never makes it collectible. A generic import permanently releases the marker.
func (q *Queue) recordFileUpload(hash, purpose string, created bool, requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)
	if purpose == attemptOutputUploadPurpose {
		return q.recordAttemptOutputUpload(hash, created, requestCtx)
	}
	unlockRequest := requestReadLock(requestCtx, &q.mu)
	_, output := q.s.AttemptOutputUploads[hash]
	_, marked := q.s.JobInputUploads[hash]
	_, bundle := q.s.BundleUploads[hash]
	change := purpose != jobInputUploadPurpose && marked || purpose == jobInputUploadPurpose && created && !marked
	change = change || purpose != jobInputUploadPurpose && (bundle || output)
	unlockRequest()
	if !change {
		return nil
	}
	return q.updateRequest(requestCtx, func() error {
		if purpose != jobInputUploadPurpose {
			delete(q.s.JobInputUploads, hash)
			delete(q.s.AttemptOutputUploads, hash)
			delete(q.s.BundleUploads, hash)
		} else if _, marked := q.s.JobInputUploads[hash]; created && !marked {
			if q.s.JobInputUploads == nil {
				q.s.JobInputUploads = map[string]time.Time{}
			}
			q.s.JobInputUploads[hash] = time.Time{}
		}
		return nil
	}, nil)
}

func (q *Queue) publishFileUpload(hash string, requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)
	unlockRequest := requestReadLock(requestCtx, &q.mu)
	published, marked := q.jobUploadMarker(hash)
	unchanged := !marked || published.Equal(q.clock.Now())
	unlockRequest()
	if unchanged {
		return nil
	}
	return q.updateRequest(requestCtx, func() error {
		if _, marked := q.s.JobInputUploads[hash]; marked {
			q.s.JobInputUploads[hash] = q.clock.Now()
		}
		if _, marked := q.s.AttemptOutputUploads[hash]; marked {
			q.s.AttemptOutputUploads[hash] = q.clock.Now()
		}
		return nil
	}, nil)
}

// Called before the transaction commits a job/reference. Terminal retention
// takes over once attached; other durable custody also releases orphan eligibility.
func (q *Queue) releaseFileUploadMarkers() {
	if len(q.s.JobInputUploads) == 0 {
		return
	}
	_, _, referenced := q.fileReferences(false, q.touchedJobs())
	q.records.referenceAdds = referenced
	for hash := range q.s.JobInputUploads {
		if referenced[hash] || q.records.references[hash] {
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
	if q.records != nil {
		q.rebuildRecords()
	}
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
	if q.records == nil {
		q.rebuildRecords()
	}
	for _, id := range q.records.files.due(now) {
		j := q.touchJob(id)
		if j == nil || !terminal(j.State) || !hasJobFiles(j) {
			continue
		}
		if deliveryPending(j) {
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
		enrollFileReclamation(j)
	}
}

func enrollFileReclamation(j *Job) {
	if j.FilesExpired && j.FileReclamation == nil {
		j.FileReclamation = &FileReclamation{}
		seen := map[string]bool{}
		for _, files := range jobFileManifests(j) {
			for _, ref := range files {
				if !seen[ref.SHA256] {
					j.FileReclamation.Pending = append(j.FileReclamation.Pending, ref.SHA256)
					seen[ref.SHA256] = true
				}
			}
		}
	}
}

// q.mu must be held. Only expired terminal inputs/results are candidates.
// Everything else with explicit custody is a keeping reference. In particular,
// checkpoint generations and catalog snapshots survive terminal file expiry.
func (q *Queue) fileReferences(pendingOnly bool, subset ...map[string]*Job) (expired, keep, referenced map[string]bool) {
	return q.collectFileReferences(pendingOnly, nil, nil, subset...)
}

// The release planner uses the collector itself, treating only eligible selected
// jobs as expired. Explanations therefore follow the same protection decisions.
func (q *Queue) collectFileReferences(pendingOnly bool, releasing map[string]bool, report func(string, FileKeepingReference), subset ...map[string]*Job) (expired, keep, referenced map[string]bool) {
	expired, keep, referenced = map[string]bool{}, map[string]bool{}, map[string]bool{}
	if len(subset) == 0 {
		for hash := range q.s.RetiredJobFiles {
			expired[hash], referenced[hash] = true, true
		}
	}
	owner := FileKeepingReference{}
	keepFiles := func(kind string, files FileManifest) {
		for name, ref := range files {
			keep[ref.SHA256] = true
			if report != nil {
				r := owner
				r.Kind, r.Name = kind, name
				report(ref.SHA256, r)
			}
		}
	}
	for hash := range q.bundleReferences(subset...) {
		keepFiles("extension_environment_bundle", FileManifest{"bundle": {SHA256: hash}})
	}
	add := func(to map[string]bool, files FileManifest) {
		for _, ref := range files {
			to[ref.SHA256] = true
		}
	}
	extension := func(e Extension) {
		keepFiles("extension_asset", e.Assets)
		for _, v := range e.Variants {
			for _, r := range v.Environments {
				keepFiles("environment_asset", r.Assets)
			}
		}
	}
	var custody *Queue
	job := func(j *Job) {
		if j == nil {
			return
		}
		if releasing[j.ID] {
			// Project exactly the enrollment that apply would perform without
			// re-enrolling historical manifests after completed reclamation.
			planned := *j
			planned.FilesExpired = true
			enrollFileReclamation(&planned)
			j = &planned
		}
		owner = FileKeepingReference{JobID: j.ID, Project: j.Spec.Project}
		filesExpired := j.FilesExpired
		protected := deliveryPending(j)
		if terminal(j.State) && filesExpired {
			if custody == nil {
				custody = q.cancellationObservation()
			}
			protected = protected || custody.jobReleasePending(j)
		}
		// Retained identities, including expired answers and completed
		// reclamations, must never be mistaken for pre-submit orphans.
		add(referenced, j.Spec.Files)
		add(referenced, sharedStoredFiles(j.Spec))
		add(referenced, j.OutputRefs)
		// Recoveries have their own immutable retention deadline. They can be
		// attached after the original job's ordinary files have already expired.
		for _, r := range j.RecoveredResults {
			add(referenced, r.Files)
			if q.clock.Now().Before(r.ExpiresAt) {
				keepFiles("recovered_result", r.Files)
			} else {
				add(expired, r.Files)
			}
		}
		if !terminal(j.State) {
			keepFiles("pending_output", j.PendingOutputRefs)
		}
		if deliveryPending(j) {
			keepFiles("pending_delivery", j.OutputRefs)
		}
		for _, a := range j.AttemptHistory {
			if a.Diagnostics != nil {
				add(referenced, a.Diagnostics.Files)
			}
		}
		if j.FileReclamation != nil {
			for _, hash := range j.FileReclamation.Pending {
				referenced[hash] = true
			}
		}
		if j.FanOut != nil {
			for _, result := range j.FanOut.Results {
				add(referenced, result.OutputRefs)
				if !terminal(j.State) || !filesExpired || protected {
					keepFiles("fan_out_result", result.OutputRefs)
				}
			}
		}
		if terminal(j.State) && filesExpired && !protected {
			if !pendingOnly {
				for _, files := range jobFileManifests(j) {
					add(expired, files)
				}
			} else if j.FileReclamation != nil {
				for _, hash := range j.FileReclamation.Pending {
					expired[hash] = true
				}
			}
		} else {
			for _, files := range jobFileManifests(j) {
				keepFiles("retained_job_files", files)
			}
		}
		for _, save := range j.FileCheckpoints {
			keepFiles("checkpoint", save.Files)
		}
		if j.ResumeFiles != nil {
			keepFiles("resume_checkpoint", j.ResumeFiles.Files)
		}
		for _, e := range j.Extensions {
			extension(e)
		}
		for _, r := range j.EnvironmentRecipes {
			keepFiles("environment_asset", r.Assets)
		}
		for _, recipes := range j.SidecarEnvironmentRecipes {
			for _, r := range recipes {
				keepFiles("environment_asset", r.Assets)
			}
		}
	}
	jobs := q.s.Jobs
	if len(subset) > 0 {
		jobs = subset[0]
	}
	for _, j := range jobs {
		job(j)
	}
	if len(subset) == 0 {
		for _, record := range q.s.Submissions {
			job(record.Job)
		}
	}
	owner = FileKeepingReference{}
	for _, e := range q.s.Extensions {
		extension(e)
	}
	for _, recipes := range q.environments.Environments {
		for _, r := range recipes {
			keepFiles("environment_asset", r.Assets)
		}
	}
	for _, m := range q.models.Models {
		keepFiles("model_catalog", FileManifest{m.Name: {SHA256: m.SHA256}})
		for _, f := range m.Files {
			keepFiles("model_catalog", FileManifest{"file": {SHA256: f.SHA256}})
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slot.EnvironmentRecipe != nil {
			keepFiles("model_environment_asset", slot.EnvironmentRecipe.Assets)
		}
	}
	for hash := range keep {
		referenced[hash] = true
	}
	return expired, keep, referenced
}

func (q *Queue) expiredBlob(hash string, requestContexts ...context.Context) bool {
	requestCtx := requestContext(requestContexts)
	unlockRequest := requestReadLock(requestCtx, &q.mu)
	defer unlockRequest()
	expired, keep, _ := q.fileReferences(false)
	return expired[hash] && !keep[hash]
}

// Tick snapshots candidates, then fences reference publication and rechecks
// custody for each unlink. Neither filesystem access nor waiting for the fence
// holds q.mu. Only positively marked job-input uploads can be orphans.
func (q *Queue) reclaimJobFiles() error { return q.reclaimJobFilesContext(context.Background()) }

func (q *Queue) reclaimJobFilesContext(requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	return errors.Join(q.reclaimInterruptedUploads(requestCtx), q.reclaimPublishedFilesContext(requestCtx))
}

func (q *Queue) reclaimPublishedFiles() error {
	return q.reclaimPublishedFilesContext(context.Background())
}

func (q *Queue) reclaimPublishedFilesContext(requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestReadLock(requestCtx, &q.mu)
	store := q.files
	expired, keep, referenced := q.fileReferences(true)
	candidates := expired
	revision := q.fileReferenceRevision
	cutoff := q.clock.Now().Add(-filePublicationGrace)
	orphanCutoff := q.clock.Now().Add(-q.fileRetention)
	orphans := map[string]time.Time{}
	bundles := map[string]time.Time{}
	for hash, published := range q.s.BundleUploads {
		if !published.After(orphanCutoff) && !referenced[hash] {
			bundles[hash] = published
		}
	}
	for hash, published := range q.jobUploadMarkers() {
		if !published.IsZero() && !published.After(orphanCutoff) && !referenced[hash] {
			orphans[hash] = published
		}
	}
	requestUnlock1()
	if store == nil {
		return nil
	}
	gone := map[string]bool{}
	checked := 0
	deferred := false
	reclaim := func(hash string, orphan, bundle bool) {
		// The same fence covers tus publication and verify-then-commit callers.
		// Do not queue a writer behind slow hashing/uploads: RWMutex writer
		// preference would then stall unrelated publications. Tick retries later.
		if !q.fileLifecycle.TryLock() {
			deferred = true
			return
		}
		requestUnlock2 := requestReadLock(requestCtx, &q.mu)
		// Every queue transaction and catalog change advances this fence. Reuse
		// the reference snapshot only while it still describes current custody.
		if revision != q.fileReferenceRevision {
			expired, keep, referenced = q.fileReferences(true)
			revision = q.fileReferenceRevision
		}
		remove := q.files == store && expired[hash] && !keep[hash]
		if orphan {
			published, marked := q.jobUploadMarker(hash)
			remove = q.files == store && marked && published == orphans[hash] && !referenced[hash]
		}
		if bundle {
			published, marked := q.s.BundleUploads[hash]
			remove = q.files == store && marked && published == bundles[hash] && !referenced[hash]
		}
		bundleTime, trackedBundle := q.s.BundleUploads[hash]
		// An expired input can share bytes with a newly created, rejected
		// recipe. Its older job window cannot shorten the recipe's retention.
		remove = remove && (!trackedBundle || !bundleTime.After(orphanCutoff))
		requestUnlock2()
		var err error
		if remove {
			path := filepath.Join(store.root, "blobs", hash)
			var st os.FileInfo
			st, err = os.Lstat(path)
			// Stat inside the publication fence: a publisher/HEAD may have
			// refreshed mtime since the collection snapshot was taken.
			if err == nil && filePayloadAgeEligible(st, cutoff, orphanCutoff, orphan || trackedBundle) {
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
		reclaim(hash, false, false)
	}
	for hash := range orphans {
		if checked == fileReclaimBatch || deferred {
			break
		}
		checked++
		reclaim(hash, true, false)
	}
	for hash := range bundles {
		if checked == fileReclaimBatch || deferred {
			break
		}
		checked++
		reclaim(hash, false, true)
	}
	requestUnlock3 := requestLock(requestCtx, &q.mu)
	warn := deferred && !q.fileCollectionDeferred
	q.fileCollectionDeferred = deferred
	requestUnlock3()
	if warn {
		log.Printf("file retention deferred: publication or reference verification in progress; retrying next tick")
	}
	if len(gone) == 0 {
		return nil
	}
	return q.updateRequest(requestCtx, func() error {
		if q.files != store {
			return nil
		}
		for hash := range gone {
			delete(q.s.RetiredJobFiles, hash)
			// A re-publication between unlink and this transaction owns a newer
			// marker. Never erase its eligibility/progress with an older result.
			if q.s.JobInputUploads[hash] == orphans[hash] {
				delete(q.s.JobInputUploads, hash)
			}
			if q.s.AttemptOutputUploads[hash] == orphans[hash] {
				delete(q.s.AttemptOutputUploads, hash)
			}
			if q.s.BundleUploads[hash] == bundles[hash] {
				delete(q.s.BundleUploads, hash)
			}
		}
		mark := func(j *Job) {
			if j == nil || j.FileReclamation == nil {
				return
			}
			intersects := false
			for _, hash := range j.FileReclamation.Pending {
				if gone[hash] {
					intersects = true
					break
				}
			}
			if !intersects {
				return
			}
			q.touchJob(j.ID)
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
	}, nil)
}

// The planner and collector must agree on both publication grace and the
// longer blob-age rule for positively marked orphan/bundle uploads.
func filePayloadAgeEligible(st os.FileInfo, publicationCutoff, orphanCutoff time.Time, marked bool) bool {
	return !st.ModTime().After(publicationCutoff) && (!marked || st.Mode().IsRegular() && !st.ModTime().After(orphanCutoff))
}

func hasJobFiles(j *Job) bool {
	for _, files := range jobFileManifests(j) {
		if len(files) > 0 {
			return true
		}
	}
	return false
}
