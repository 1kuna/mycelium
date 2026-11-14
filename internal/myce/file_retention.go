package myce

import (
	"context"
	"errors"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// Finished jobs' files are kept for 3 days, then reclaimed (MY-47). An explicit
// zero retains unique file bytes until explicit release.
const DefaultFileRetention = 72 * time.Hour

// Publication/reference lifecycle locks and revision fences, rather than an
// elapsed grace, protect concurrent writes. Unclaimed unique bytes stay pinned
// in automatic mode; an explicit retention policy supplies any orphan deadline.
// Bound filesystem work while draining a large expiry backlog so maintenance
// remains responsive. Durable per-job progress avoids repeating completed work.
// One atomic reference/record is the indivisible reclamation unit. Yield the
// control lock between units rather than guessing a fleet-wide batch size.
const fileReclaimBatch = 1

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
	if err := lockUploadContext(requestCtx, q.mu.RLock, q.mu.RUnlock); err != nil {
		return err
	}
	unlockRequest := q.mu.RUnlock
	_, marked := q.s.BundleUploads[hash]
	unlockRequest()
	if marked {
		return nil
	}
	return q.updateUploadRequest(requestCtx, func() error { q.markBundleUpload(hash); return nil }, nil)
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
	if err := lockUploadContext(requestCtx, q.mu.RLock, q.mu.RUnlock); err != nil {
		return err
	}
	unlockRequest := q.mu.RUnlock
	_, output := q.s.AttemptOutputUploads[hash]
	_, marked := q.s.JobInputUploads[hash]
	_, bundle := q.s.BundleUploads[hash]
	change := purpose != jobInputUploadPurpose && marked || purpose == jobInputUploadPurpose && created && !marked
	change = change || purpose != jobInputUploadPurpose && (bundle || output)
	unlockRequest()
	if !change {
		return nil
	}
	return q.updateUploadRequest(requestCtx, func() error {
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
	if err := lockUploadContext(requestCtx, q.mu.RLock, q.mu.RUnlock); err != nil {
		return err
	}
	unlockRequest := q.mu.RUnlock
	published, marked := q.jobUploadMarker(hash)
	unchanged := !marked || published.Equal(q.clock.Now())
	unlockRequest()
	if unchanged {
		return nil
	}
	return q.updateUploadRequest(requestCtx, func() error {
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
	if age < 0 {
		return errors.New("file-retention must be nonnegative")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.fileRetention = age
	if q.records != nil {
		q.rebuildRecords()
	}
	q.republishReads()
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
func (q *Queue) maintainFileExpiry() { q.expireDueFiles(0) }

// Caller is inside a queue transaction. An ordinary pass expires one due record
// (fileReclaimBatch). A capacity-refused write passes the bytes it needs: due
// records keep expiring until their manifests cover that need or none remain.
// Only records whose retention deadline has already passed are expired, so this
// does now what later ordinary passes would do; it never widens what expires.
func (q *Queue) expireDueFiles(need uint64) (expired uint64) {
	now := q.clock.Now()
	if q.records == nil {
		q.rebuildRecords()
	}
	for {
		ids := q.records.files.due(now)
		if len(ids) == 0 {
			return expired
		}
		for _, id := range ids {
			expired = addDiskBytes(expired, q.expireJobFiles(id, now))
		}
		if expired >= need {
			return expired
		}
	}
}

// Returns the manifest bytes of files newly expired by this call.
func (q *Queue) expireJobFiles(id string, now time.Time) uint64 {
	j := q.s.Jobs[id]
	if j == nil || !terminal(j.State) || !hasJobFiles(j) {
		return 0
	}
	if parkedStorageCustody(j) || (deliveryPending(j) || checkpointMirrorsPending(j)) || j.Spec.localFirst() && attemptReleasePending(j) {
		return 0
	}
	if j.Spec.OutputDestination != nil && (j.Delivery == nil || j.Delivery.VerifiedAt == nil || !sameDelivery(j, *j)) {
		return 0
	}
	retention := q.jobFileRetention(j)
	if retention == 0 {
		return 0
	}
	if j.FinishedAt != nil && now.Before(j.FinishedAt.Add(retention)) {
		// A policy change can leave an early index entry; keep the real deadline.
		q.records.files.set(id, j.FinishedAt.Add(retention))
		return 0
	}
	j = q.touchJob(id)
	// Some historical terminal paths did not stamp FinishedAt. Start their
	// window when first observed, rather than guessing from submission time.
	if j.FinishedAt == nil {
		ended := now
		j.FinishedAt = &ended
	}
	newly := !j.FilesExpired && !now.Before(j.FinishedAt.Add(retention))
	if newly {
		j.FilesExpired = true
	}
	enrollFileReclamation(j)
	if !newly {
		return 0
	}
	var bytes uint64
	seen := map[string]bool{}
	for _, files := range jobFileManifests(j) {
		for _, ref := range files {
			if !seen[ref.SHA256] {
				seen[ref.SHA256] = true
				bytes = addDiskBytes(bytes, uint64(ref.SizeBytes))
			}
		}
	}
	return bytes
}

// Validates a project's optional file_retention policy.
func parseProjectFileRetention(v string) (time.Duration, error) {
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, errors.New("project file_retention must be a positive duration such as 72h")
	}
	return d, nil
}

// Caller holds q.mu. A project's policy can only shorten the queue-wide window:
// the project owns its own data, and the operator's window still bounds it.
func (q *Queue) jobFileRetention(j *Job) time.Duration {
	r := q.fileRetention
	if p, _ := parseProjectFileRetention(q.s.Policies[j.Spec.Project].FileRetention); p > 0 && (r == 0 || p < r) {
		r = p
	}
	if j.Spec.localFirst() {
		r = max(r, q.expiryGrace())
	}
	return r
}

// Caller holds q.mu. Zero means no deadline: retained until explicit release.
func (q *Queue) fileExpiryDeadline(j *Job) time.Time {
	retention := q.jobFileRetention(j)
	if retention == 0 || !terminal(j.State) || !hasJobFiles(j) || j.FilesExpired || (deliveryPending(j) || checkpointMirrorsPending(j)) || j.Spec.localFirst() && attemptReleasePending(j) {
		return time.Time{}
	}
	if j.FinishedAt == nil {
		return q.clock.Now()
	}
	return j.FinishedAt.Add(retention)
}

// Runs under q.mu after a committed project-policy change. Re-derives expiry
// deadlines only for projects whose file_retention changed.
func (q *Queue) reindexProjectFileDeadlines(previous map[string]ProjectPolicy) {
	if q.records == nil {
		return
	}
	changed := map[string]bool{}
	for name, p := range q.s.Policies {
		changed[name] = previous[name].FileRetention != p.FileRetention
	}
	for name, p := range previous {
		changed[name] = changed[name] || q.s.Policies[name].FileRetention != p.FileRetention
	}
	if !slices.Contains(slices.Collect(maps.Values(changed)), true) {
		return
	}
	for id, j := range q.s.Jobs {
		if j != nil && changed[j.Spec.Project] {
			q.records.files.set(id, q.fileExpiryDeadline(j))
		}
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
func (q *Queue) collectFileReferences(pendingOnly bool, releasing map[string]bool, report func(FileRef, FileKeepingReference), subset ...map[string]*Job) (expired, keep, referenced map[string]bool) {
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
				report(ref, r)
			}
		}
	}
	for _, claim := range q.s.FileInputClaims {
		if claim.SettledAt.IsZero() {
			owner = FileKeepingReference{Project: claim.Project}
			keepFiles("input_claim", FileManifest{claim.ID: claim.File})
		}
	}
	owner = FileKeepingReference{}
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
		protected := parkedStorageCustody(j) || (deliveryPending(j) || checkpointMirrorsPending(j))
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
		if j.LocalOutputs != nil {
			// Local custody can pin an existing shared identity, but never grants
			// authority to collect an unrelated import with matching content.
			add(referenced, j.LocalOutputs.Files)
			if !terminal(j.State) || !filesExpired || protected {
				keepFiles("worker_local_custody", j.LocalOutputs.Files)
			}
		}
		// Recoveries have their own immutable retention deadline. They can be
		// attached after the original job's ordinary files have already expired.
		for _, r := range j.RecoveredResults {
			add(referenced, r.Files)
			if r.ExpiresAt.IsZero() || q.clock.Now().Before(r.ExpiresAt) {
				keepFiles("recovered_result", r.Files)
			} else {
				add(expired, r.Files)
			}
		}
		if !terminal(j.State) {
			keepFiles("pending_output", j.PendingOutputRefs)
		}
		if parkedStorageCustody(j) || (deliveryPending(j) || checkpointMirrorsPending(j)) || j.Spec.localFirst() && attemptReleasePending(j) {
			keepFiles("pending_delivery", j.OutputRefs)
		}
		for _, a := range j.AttemptHistory {
			if a.Diagnostics != nil {
				add(referenced, a.Diagnostics.Files)
				if j.Spec.OutputDestination != nil && j.FinishedAt != nil && (q.fileRetention == 0 || q.clock.Now().Before(j.FinishedAt.Add(q.fileRetention))) {
					keepFiles("retained_attempt_diagnostics", a.Diagnostics.Files)
				}
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
			if j.Spec.localFirst() && j.FilesExpired {
				continue
			}
			keepFiles("checkpoint", save.Files)
		}
		if j.ResumeFiles != nil && !(j.Spec.localFirst() && j.FilesExpired) {
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
	_, err := q.reclaimPublishedFilesUntil(requestContext(requestContexts), 0)
	return err
}

// Each pass snapshots candidates under a short read lock, then works in
// batches: one publication-fence hold and one short q.mu recheck of current
// custody per batch, unlinks outside q.mu, and one commit transaction at the
// end. Measured control pressure ends the pass after its first batch rather
// than before it, so collection always progresses (#1375). With need zero an
// idle queue keeps draining; a capacity-refused write passes its need instead,
// and collection of already authorized bytes continues until it frees that many.
func (q *Queue) reclaimPublishedFilesUntil(requestCtx context.Context, need uint64) (freed uint64, _ error) {

	requestUnlock1 := requestReadLock(requestCtx, &q.mu)
	store := q.files
	expired, keep, referenced := q.fileReferences(true)
	type candidate struct {
		hash           string
		orphan, bundle bool
	}
	items := make([]candidate, 0, len(expired))
	for hash := range expired {
		if (FileRef{SHA256: hash}).validate() == nil && !keep[hash] {
			items = append(items, candidate{hash: hash})
		}
	}
	revision := q.fileReferenceRevision
	cutoff := q.clock.Now().Add(-q.s.PublicationHorizon)
	expiredCutoff := q.clock.Now().Add(-q.expiredFilePublicationGrace())
	orphanCutoff := q.clock.Now().Add(-q.fileRetention)
	orphans := map[string]time.Time{}
	bundles := map[string]time.Time{}
	markers := q.jobUploadMarkers()
	for hash, published := range markers {
		if q.fileRetention > 0 && !published.IsZero() && !published.After(orphanCutoff) && !referenced[hash] {
			orphans[hash] = published
			items = append(items, candidate{hash: hash, orphan: true})
		}
	}
	for hash, published := range q.s.BundleUploads {
		if q.fileRetention > 0 && !published.After(orphanCutoff) && !referenced[hash] {
			bundles[hash] = published
			items = append(items, candidate{hash: hash, bundle: true})
		}
	}
	batch := fileCollectionBatch(len(referenced) + len(markers) + len(q.s.BundleUploads))
	requestUnlock1()
	if store == nil {
		return 0, nil
	}
	gone := map[string]bool{}
	deferred, yielded := false, false
	var reclaimedBlobs uint64
	// Reports false when the fence is busy and the batch was not attempted.
	reclaimBatch := func(batch []candidate) bool {
		// The same fence covers tus publication and verify-then-commit callers.
		// Do not queue a writer behind slow hashing/uploads: RWMutex writer
		// preference would then stall unrelated publications. Tick retries later.
		if !q.fileLifecycle.TryLock() {
			return false
		}
		defer q.fileLifecycle.Unlock()
		type decision struct {
			remove, marked bool
		}
		decisions := make([]decision, len(batch))
		requestUnlock2 := requestReadLock(requestCtx, &q.mu)
		// Every queue transaction and catalog change advances this fence. Reuse
		// the reference snapshot only while it still describes current custody.
		if revision != q.fileReferenceRevision {
			expired, keep, referenced = q.fileReferences(true)
			revision = q.fileReferenceRevision
		}
		for i, c := range batch {
			remove := q.files == store && expired[c.hash] && !keep[c.hash]
			if c.orphan {
				published, marked := q.jobUploadMarker(c.hash)
				remove = q.files == store && marked && published.Equal(orphans[c.hash]) && !referenced[c.hash]
			}
			if c.bundle {
				published, marked := q.s.BundleUploads[c.hash]
				remove = q.files == store && marked && published.Equal(bundles[c.hash]) && !referenced[c.hash]
			}
			bundleTime, trackedBundle := q.s.BundleUploads[c.hash]
			// An expired input can share bytes with a newly created, rejected
			// recipe. Its older job window cannot shorten the recipe's retention.
			remove = remove && (!trackedBundle || !bundleTime.After(orphanCutoff))
			decisions[i] = decision{remove: remove, marked: c.orphan || trackedBundle}
		}
		requestUnlock2()
		// The fence stays held from the recheck through every unlink below, so
		// no publication can commit a new reference to a hash in this batch.
		for i, c := range batch {
			if need > 0 && freed >= need {
				break
			}
			if !decisions[i].remove || gone[c.hash] {
				continue
			}
			path := filepath.Join(store.root, "blobs", c.hash)
			st, err := os.Lstat(path)
			// Stat inside the publication fence: a publisher/HEAD may have
			// refreshed mtime since the collection snapshot was taken.
			if err == nil && filePayloadAgeEligible(st, cutoff, expiredCutoff, orphanCutoff, decisions[i].marked) {
				err = os.Remove(path)
				if err == nil {
					gone[c.hash] = true
					reclaimedBlobs++
					freed = addDiskBytes(freed, uint64(st.Size()))
				}
			}
			if errors.Is(err, os.ErrNotExist) {
				gone[c.hash] = true
				err = nil
			}
			if err != nil {
				log.Printf("file retention deferred hash=%s error=%q; retrying next tick", c.hash, err)
			}
		}
		return true
	}
	for start := 0; start < len(items); start += batch {
		if requestCtx.Err() != nil || need > 0 && freed >= need {
			break
		}
		if start > 0 && need == 0 && q.fileReclaimUnderPressure() {
			yielded = true
			break
		}
		if !reclaimBatch(items[start:min(start+batch, len(items))]) {
			deferred = true
			break
		}
	}
	left := map[string]bool{}
	for _, c := range items {
		if !gone[c.hash] {
			left[c.hash] = true
		}
	}
	remaining := len(left)
	store.noteCollection(q.clock.Now(), reclaimedBlobs, freed, deferred, yielded, remaining)
	requestUnlock3 := requestLock(requestCtx, &q.mu)
	warn := deferred && !q.fileCollectionDeferred
	q.fileCollectionDeferred = deferred
	requestUnlock3()
	if warn {
		log.Printf("file retention deferred: publication or reference verification in progress; retrying next tick")
	}
	if len(gone) == 0 {
		return 0, nil
	}
	return freed, q.updateRequest(requestCtx, func() error {
		if q.files != store {
			return nil
		}
		for hash := range gone {
			q.forgetFileInputClaims(hash, cutoff)
			delete(q.s.RetiredJobFiles, hash)
			// A re-publication between unlink and this transaction owns a newer
			// marker. Never erase its eligibility/progress with an older result.
			if q.s.JobInputUploads[hash].Equal(orphans[hash]) {
				delete(q.s.JobInputUploads, hash)
			}
			if q.s.AttemptOutputUploads[hash].Equal(orphans[hash]) {
				delete(q.s.AttemptOutputUploads, hash)
			}
			if q.s.BundleUploads[hash].Equal(bundles[hash]) {
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
// longer blob-age rule for positively marked orphan/bundle uploads. An expired
// job's files use expiredCutoff, the grace capped at the file retention.
func filePayloadAgeEligible(st os.FileInfo, publicationCutoff, expiredCutoff, orphanCutoff time.Time, marked bool) bool {
	if !marked {
		return !st.ModTime().After(expiredCutoff)
	}
	return !st.ModTime().After(publicationCutoff) && st.Mode().IsRegular() && !st.ModTime().After(orphanCutoff)
}

func hasJobFiles(j *Job) bool {
	if j.LocalOutputs != nil && len(j.LocalOutputs.Files) > 0 {
		return true
	}
	for _, files := range jobFileManifests(j) {
		if len(files) > 0 {
			return true
		}
	}
	return false
}

// Caller holds q.mu. A publication or HEAD refreshes a blob's mtime, and the
// publication horizon (the longest measured first settlement of an input claim)
// keeps it until the submission that references it can commit. For an expired
// finished job's files that grace never outlasts the file retention: they are
// kept 3 days, then reclaimed (MY-47). Live, a horizon past the retention stopped
// every unlink (#1375). An unsettled claim keeps its bytes as custody anyway.
// Orphan and bundle uploads keep the full horizon. Zero retention keeps it too.
func (q *Queue) expiredFilePublicationGrace() time.Duration {
	if q.fileRetention > 0 {
		return min(q.s.PublicationHorizon, q.fileRetention)
	}
	return q.s.PublicationHorizon
}

// Measured control pressure: a control request has waited on q.mu longer than
// one mean uncontended hold. Collection yields after a batch, never before one.
func (q *Queue) fileReclaimUnderPressure() bool { busy, _ := q.evidencePressure(); return busy }

// One batch costs one fence hold, one q.mu recheck and its share of the pass's
// commit. The recheck already walks every reference, upload marker and bundle
// record under q.mu (n of them), as does the snapshot; a batch of at most n
// candidates therefore keeps the pass's locked work within a constant factor of
// the zero-reclaim pass the queue already runs every tick, whatever the backlog.
func fileCollectionBatch(n int) int { return max(1, n) }
