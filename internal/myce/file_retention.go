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
	Pending                  []string `json:"pending,omitempty"`
	LegacyCheckpointsExpired bool     `json:"legacy_checkpoints_expired,omitempty"`
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

// Callers hold the file lifecycle fence. Creation records new job-input
// identities and interrupted-upload recovery restores verified legacy markers.
// Reusing an unmarked import never enrolls it for published-blob collection.
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
	_, _, referenced := q.collectDurableFileReferences(false, nil, nil, q.touchedJobs())
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
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
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
		if previous[name].FileRetention != p.FileRetention {
			changed[name] = true
		}
	}
	for name, p := range previous {
		if q.s.Policies[name].FileRetention != p.FileRetention {
			changed[name] = true
		}
	}
	if len(changed) == 0 {
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

func visitLegacyCheckpointFiles(j *Job, visit func(string, FileManifest)) {
	if j.Spec.localFirst() {
		return
	}
	for _, save := range j.FileCheckpoints {
		visit("checkpoint", save.Files)
	}
	if j.ResumeFiles != nil {
		visit("resume_checkpoint", j.ResumeFiles.Files)
	}
}

// Explicit terminal-job release may enroll legacy HTTP checkpoint files.
// Clone because the release planner projects expiry from a shallow job copy.
func enrollLegacyCheckpointReclamation(j *Job) {
	if j.Spec.localFirst() || j.FileReclamation.LegacyCheckpointsExpired {
		return
	}
	pending := slices.Clone(j.FileReclamation.Pending)
	seen := make(map[string]bool, len(pending))
	for _, hash := range pending {
		seen[hash] = true
	}
	visitLegacyCheckpointFiles(j, func(_ string, files FileManifest) {
		for _, ref := range files {
			if !seen[ref.SHA256] {
				seen[ref.SHA256] = true
				pending = append(pending, ref.SHA256)
			}
		}
	})
	j.FileReclamation = &FileReclamation{Pending: pending, LegacyCheckpointsExpired: true}
}

func (q *Queue) collectFileReferences(pendingOnly bool, releasing map[string]fileReleaseScope, report func(FileRef, FileKeepingReference), subset ...map[string]*Job) (expired, keep, referenced map[string]bool) {
	expired, keep, referenced = q.collectDurableFileReferences(pendingOnly, releasing, report, subset...)
	q.visitFileInputClaimKeepers(func(claim FileInputClaim, owner fileKeeper) {
		hash := claim.File.SHA256
		keep[hash], referenced[hash] = true, true
		if report != nil {
			report(claim.File, owner.FileKeepingReference)
		}
	})
	return expired, keep, referenced
}

type fileReferenceFlags uint8

const (
	fileReferenced fileReferenceFlags = 1 << iota
	fileExpired
	fileKept
)

// Registry identity is used for ownership deduplication. The existing public
// reference describes the asset path and deliberately omits this identity.
type fileKeeper struct {
	FileKeepingReference
	registry fileRegistryOwner
}

type fileRegistryOwner struct {
	kind, name, variant string
}

type fileDigestReferences struct {
	flags   fileReferenceFlags
	keepers map[fileKeeper]uint64
}

type fileReferenceView map[string]fileDigestReferences

func (v fileReferenceView) note(ref FileRef, flags fileReferenceFlags, owner fileKeeper, names bool) {
	hash := ref.SHA256
	entry := v[hash]
	entry.flags |= flags
	if names && flags&fileKept != 0 {
		if entry.keepers == nil {
			entry.keepers = map[fileKeeper]uint64{}
		}
		entry.keepers[owner] = max(entry.keepers[owner], uint64(ref.SizeBytes))
	}
	v[hash] = entry
}

func (r fileDigestReferences) collectionEligible() bool {
	return r.flags&(fileExpired|fileKept) == fileExpired
}

// q.mu held. Claims protect published blobs without granting durable
// custody over interrupted uploads or retiring their cleanup markers.
func (q *Queue) visitFileReferences(pendingOnly bool, releasing map[string]fileReleaseScope, note func(FileRef, fileReferenceFlags, fileKeeper)) {
	q.visitDurableFileReferences(pendingOnly, releasing, note)
	q.visitFileInputClaimKeepers(func(claim FileInputClaim, owner fileKeeper) {
		note(claim.File, fileKept|fileReferenced, owner)
	})
}

// q.mu held. With no filter the collector needs only the retention flags;
// a selected release also captures the keeper identities for its preview.
func (q *Queue) captureFileReferences(pendingOnly bool, releasing map[string]fileReleaseScope, hashes map[string]bool, keepers bool) fileReferenceView {
	view := make(fileReferenceView, len(hashes))
	q.visitFileReferences(pendingOnly, releasing, func(ref FileRef, flags fileReferenceFlags, owner fileKeeper) {
		if hashes == nil || hashes[ref.SHA256] {
			view.note(ref, flags, owner, keepers)
		}
	})
	return view
}

// A claim protects a published blob but does not retire a cleanup marker or
// give an interrupted transfer durable job or catalog custody.
func (q *Queue) collectDurableFileReferences(pendingOnly bool, releasing map[string]fileReleaseScope, report func(FileRef, FileKeepingReference), subset ...map[string]*Job) (expired, keep, referenced map[string]bool) {
	expired, keep, referenced = map[string]bool{}, map[string]bool{}, map[string]bool{}
	q.visitDurableFileReferences(pendingOnly, releasing, func(ref FileRef, flags fileReferenceFlags, owner fileKeeper) {
		hash := ref.SHA256
		if flags&fileExpired != 0 {
			expired[hash] = true
		}
		if flags&fileReferenced != 0 {
			referenced[hash] = true
		}
		if flags&fileKept != 0 {
			keep[hash] = true
			if report != nil {
				report(ref, owner.FileKeepingReference)
			}
		}
	}, subset...)
	return expired, keep, referenced
}

// q.mu held. This walk only reads committed identities. Callers can bound
// retained facts by digest or project before inspecting the store.
func (q *Queue) visitDurableFileReferences(pendingOnly bool, releasing map[string]fileReleaseScope, note func(FileRef, fileReferenceFlags, fileKeeper), subset ...map[string]*Job) {
	if len(subset) == 0 {
		for hash := range q.s.RetiredJobFiles {
			note(FileRef{SHA256: hash}, fileExpired|fileReferenced, fileKeeper{})
		}
	}
	owner := fileKeeper{}
	keep := func(kind, name string, ref FileRef) {
		r := owner
		r.Kind, r.Name = kind, name
		note(ref, fileKept|fileReferenced, r)
	}
	keepFiles := func(kind string, files FileManifest) {
		for name, ref := range files {
			keep(kind, name, ref)
		}
	}
	add := func(flags fileReferenceFlags, files FileManifest) {
		for _, ref := range files {
			note(ref, flags, fileKeeper{})
		}
	}
	bundle := func(ref *FileRef) {
		if ref != nil {
			keep("extension_environment_bundle", "bundle", *ref)
		}
	}
	recipe := func(kind string, r EnvironmentRecipe) {
		keepFiles(kind, r.Assets)
		bundle(r.FilesRef)
	}
	model := func(kind string, m Model) {
		keep(kind, m.Name, FileRef{SHA256: m.SHA256, SizeBytes: m.SizeBytes})
		for _, f := range m.Files {
			keep(kind, f.Path, FileRef{SHA256: f.SHA256, SizeBytes: f.SizeBytes})
		}
	}
	extension := func(e Extension) {
		keepFiles("extension_asset", e.Assets)
		bundle(e.FilesRef)
		for _, v := range e.Variants {
			for _, r := range v.Environments {
				recipe("environment_asset", r)
			}
		}
	}
	var custody *Queue
	job := func(j *Job) {
		if j == nil {
			return
		}
		if scope, ok := releasing[j.ID]; ok {
			// Project exactly the enrollment that apply would perform without
			// re-enrolling historical manifests after completed reclamation.
			planned := *j
			planned.FilesExpired = true
			enrollFileReclamation(&planned)
			if scope.checkpoints {
				enrollLegacyCheckpointReclamation(&planned)
			}
			j = &planned
		}
		owner = fileKeeper{FileKeepingReference: FileKeepingReference{JobID: j.ID, Project: j.Spec.Project}}
		deliveryProtected := parkedStorageCustody(j) || deliveryPending(j) || checkpointMirrorsPending(j)
		protected := deliveryProtected
		if terminal(j.State) && j.FilesExpired {
			if custody == nil {
				custody = q.cancellationObservation()
			}
			protected = protected || custody.jobReleasePending(j)
		}
		expired := terminal(j.State) && j.FilesExpired && !protected
		if j.LocalOutputs != nil {
			// Local custody can pin an existing shared identity but only an
			// ordinary expiry can authorize collection of the queue copy.
			if expired {
				add(fileReferenced, j.LocalOutputs.Files)
			} else {
				keepFiles("worker_local_custody", j.LocalOutputs.Files)
			}
		}
		// Recoveries have their own retention deadline, even after the
		// original job's ordinary files have expired.
		for _, r := range j.RecoveredResults {
			if r.ExpiresAt.IsZero() || q.clock.Now().Before(r.ExpiresAt) {
				keepFiles("recovered_result", r.Files)
			} else {
				add(fileReferenced|fileExpired, r.Files)
			}
		}
		if !terminal(j.State) {
			keepFiles("pending_output", j.PendingOutputRefs)
		}
		if deliveryProtected || j.Spec.localFirst() && attemptReleasePending(j) {
			keepFiles("pending_delivery", j.OutputRefs)
		}
		if j.Spec.OutputDestination != nil && j.FinishedAt != nil && (q.fileRetention == 0 || q.clock.Now().Before(j.FinishedAt.Add(q.fileRetention))) {
			for _, a := range j.AttemptHistory {
				if a.Diagnostics != nil {
					keepFiles("retained_attempt_diagnostics", a.Diagnostics.Files)
				}
			}
		}
		if j.FileReclamation != nil {
			flags := fileReferenced
			if expired {
				flags |= fileExpired
			}
			for _, hash := range j.FileReclamation.Pending {
				note(FileRef{SHA256: hash}, flags, fileKeeper{})
			}
		}
		if expired {
			visitJobFileManifests(j, func(kind string, files FileManifest) {
				// Retained identities must never become pre-submit orphans.
				// Pending enrollment owns checkpoint and dependency expiry.
				var flags fileReferenceFlags
				switch kind {
				case "input", "shared_input", "result", "diagnostic", "fan_out_result":
					flags = fileReferenced
				}
				if !pendingOnly {
					flags |= fileExpired
				}
				if flags != 0 {
					add(flags, files)
				}
			})
		} else {
			visitJobFileManifests(j, keepFiles)
		}
		legacyCheckpointsExpired := j.FileReclamation != nil && j.FileReclamation.LegacyCheckpointsExpired
		if legacyCheckpointsExpired {
			visitLegacyCheckpointFiles(j, func(_ string, files FileManifest) {
				add(fileReferenced, files)
				if !pendingOnly && terminal(j.State) && !protected {
					add(fileExpired, files)
				}
			})
		}
		checkpointsExpired := j.Spec.localFirst() && j.FilesExpired || legacyCheckpointsExpired
		if !checkpointsExpired {
			for _, save := range j.FileCheckpoints {
				keepFiles("checkpoint", save.Files)
			}
			if j.ResumeFiles != nil {
				keepFiles("resume_checkpoint", j.ResumeFiles.Files)
			}
		}
		for _, e := range j.Extensions {
			extension(e)
		}
		if r := j.Spec.Resident; r != nil && r.Engine != nil {
			for _, env := range r.Engine.Environments {
				keepFiles("resident_environment", env.Assets)
			}
			model("resident_model", r.Engine.Model)
		}
		for _, r := range j.EnvironmentRecipes {
			recipe("environment_asset", r)
		}
		for _, recipes := range j.SidecarEnvironmentRecipes {
			for _, r := range recipes {
				recipe("environment_asset", r)
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
			// Pruned answers retain the submitted spec for replay, never file
			// custody. The history transaction transfers pending collection to
			// RetiredJobFiles before storing the skeletal answer.
			if record.Job != nil && !record.FileCustodyPruned && jobs[record.Job.ID] == nil {
				job(record.Job)
			}
		}
	}
	for key, e := range q.s.Extensions {
		owner = fileKeeper{registry: fileRegistryOwner{kind: "extension", name: key}}
		extension(e)
	}
	for key, recipes := range q.environments.Environments {
		for variant, r := range recipes {
			owner = fileKeeper{registry: fileRegistryOwner{kind: "environment", name: key, variant: variant}}
			recipe("environment_asset", r)
		}
	}
	for _, m := range q.models.Models {
		owner = fileKeeper{registry: fileRegistryOwner{kind: "model", name: m.Name}}
		model("model_catalog", m)
	}
	for key, slot := range q.s.ModelSlots {
		owner = fileKeeper{registry: fileRegistryOwner{kind: "model_slot", name: key}}
		if slot.Resident != nil {
			for _, recipe := range slot.Resident.Environments {
				keepFiles("resident_environment", recipe.Assets)
			}
			model("resident_model", slot.Resident.Model)
		}
		if slot.EnvironmentRecipe != nil {
			recipe("model_environment_asset", *slot.EnvironmentRecipe)
		}
	}
}

func (q *Queue) expiredBlob(hash string, requestContexts ...context.Context) bool {
	requestCtx := requestContext(requestContexts)
	unlockRequest := requestReadLock(requestCtx, &q.mu)
	defer unlockRequest()
	var refs fileDigestReferences
	q.visitFileReferences(false, nil, func(ref FileRef, flags fileReferenceFlags, _ fileKeeper) {
		if ref.SHA256 == hash {
			refs.flags |= flags
		}
	})
	return refs.collectionEligible()
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
	if store == nil {
		requestUnlock1()
		return 0, nil
	}
	refs := q.captureFileReferences(true, nil, nil, false)
	revision := q.fileReferenceRevision
	age := q.filePublicationAge(q.clock.Now())
	claimCutoff := age.now.Add(-q.s.PublicationHorizon)
	markers, bundles := q.jobUploadMarkers(), maps.Clone(q.s.BundleUploads)
	requestUnlock1()

	orphanCutoff := age.now.Add(-age.retention)
	type candidate struct {
		hash           string
		orphan, bundle bool
	}
	var items []candidate
	for hash, ref := range refs {
		if ref.collectionEligible() && (FileRef{SHA256: hash}).validate() == nil {
			items = append(items, candidate{hash: hash})
		}
	}
	orphans := map[string]time.Time{}
	for hash, published := range markers {
		if age.retention > 0 && !published.IsZero() && !published.After(orphanCutoff) && refs[hash].flags&fileReferenced == 0 {
			orphans[hash] = published
			items = append(items, candidate{hash: hash, orphan: true})
		}
	}
	for hash, published := range bundles {
		if age.retention > 0 && !published.After(orphanCutoff) && refs[hash].flags&fileReferenced == 0 {
			items = append(items, candidate{hash: hash, bundle: true})
		}
	}
	batch := fileCollectionBatch(len(refs) + len(markers) + len(bundles))
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
			remove, marked, expired bool
		}
		decisions := make([]decision, len(batch))
		requestUnlock2 := requestReadLock(requestCtx, &q.mu)
		// Every queue transaction and catalog change advances this fence. Reuse
		// the reference snapshot only while it still describes current custody.
		if revision != q.fileReferenceRevision {
			refs = q.captureFileReferences(true, nil, nil, false)
			revision = q.fileReferenceRevision
			age = q.filePublicationAge(q.clock.Now())
		}
		for i, c := range batch {
			ref := refs[c.hash]
			expired, referenced := ref.flags&fileExpired != 0, ref.flags&fileReferenced != 0
			remove := q.files == store && ref.collectionEligible()
			if c.orphan {
				published, marked := q.jobUploadMarker(c.hash)
				remove = q.files == store && marked && published.Equal(orphans[c.hash]) && !referenced
			}
			if c.bundle {
				published, marked := q.s.BundleUploads[c.hash]
				remove = q.files == store && marked && published.Equal(bundles[c.hash]) && !referenced
			}
			bundleTime, trackedBundle := q.s.BundleUploads[c.hash]
			// An expired/released job owns reclamation of its bundle bytes.
			// Other custody still wins above; refreshed publication keeps its
			// ordinary grace below. Only unclaimed bundles need the orphan window.
			remove = remove && (!trackedBundle || expired || !bundleTime.After(orphanCutoff))
			decisions[i] = decision{remove: remove, marked: c.orphan || trackedBundle, expired: expired}
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
			if err == nil && age.payloadEligible(st, decisions[i].marked, decisions[i].expired) {
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
			q.forgetFileInputClaims(hash, claimCutoff)
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
		removed := func(hash string) bool { return gone[hash] }
		mark := func(j *Job) {
			if j != nil && j.FileReclamation != nil && slices.ContainsFunc(j.FileReclamation.Pending, removed) {
				q.touchJob(j.ID)
				j.FileReclamation.Pending = slices.DeleteFunc(j.FileReclamation.Pending, removed)
			}
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
// longer blob-age rule for positively marked orphan uploads. Expired/released
// bundles keep publication grace but no second retention window from upload.
// Other expired job files use the grace capped at the queue's file retention.
func (a filePublicationAge) payloadEligible(st os.FileInfo, marked, expired bool) bool {
	if !marked {
		return !a.protects(st.ModTime(), true)
	}
	return !a.protects(st.ModTime(), false) && st.Mode().IsRegular() && (expired || !st.ModTime().After(a.now.Add(-a.retention)))
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

type filePublicationAge struct {
	now, measuredSince        time.Time
	retention, legacy, recent time.Duration
}

// Caller holds q.mu. A first settlement after upgrade establishes the clock
// boundary for new evidence. Earlier publications retain their previous grace.
func (q *Queue) filePublicationAge(now time.Time) filePublicationAge {
	age := filePublicationAge{now: now, retention: q.fileRetention, legacy: q.s.PublicationHorizon}
	if timing := q.s.PublicationTimings; timing != nil && len(timing.Peaks) > 0 {
		age.measuredSince, age.legacy = timing.MeasuredSince, timing.LegacyGrace
		age.recent = timing.Peaks[len(timing.Peaks)-1].Delay
		for _, sample := range timing.Peaks {
			if sample.recent(now, q.fileRetention) {
				age.recent = max(age.recent, sample.Delay)
			}
		}
	}
	return age
}

func (a filePublicationAge) grace(modified time.Time) time.Duration {
	grace := a.legacy
	if !a.measuredSince.IsZero() && modified.After(a.measuredSince) {
		grace = a.recent
	}
	return grace
}

func (a filePublicationAge) protects(modified time.Time, expiredJobFile bool) bool {
	grace := a.grace(modified)
	if expiredJobFile && a.retention > 0 {
		grace = min(grace, a.retention)
	}
	return modified.After(a.now.Add(-grace))
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
