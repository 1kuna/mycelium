package myce

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/tus/tusd/v2/pkg/filestore"
	tus "github.com/tus/tusd/v2/pkg/handler"
)

// A queue-issued claim binds a pre-submission transfer to its project and exact
// bytes. The fleet still authenticates one operator, not separate project users.
type FileInputClaim struct {
	ID           string    `json:"id"`
	Project      string    `json:"project"`
	Class        string    `json:"class,omitempty"`
	File         FileRef   `json:"file"`
	At           time.Time `json:"at"`
	LastIssuedAt time.Time `json:"last_issued_at,omitzero"`
	Allocated    bool      `json:"allocated,omitempty"`
	SettledAt    time.Time `json:"settled_at,omitempty"`
	Released     bool      `json:"released,omitempty"`
}

// Rollback and reopen round-trip timestamps through JSON. Compare instants,
// not time.Time's location pointers or process-local monotonic readings.
func (c FileInputClaim) equal(other FileInputClaim) bool {
	return c.sameIdentity(other) && c.Allocated == other.Allocated && c.Released == other.Released &&
		c.LastIssuedAt.Equal(other.LastIssuedAt) && c.SettledAt.Equal(other.SettledAt)
}

func (c FileInputClaim) sameIdentity(other FileInputClaim) bool {
	return c.ID == other.ID && c.Project == other.Project && c.Class == other.Class &&
		c.File == other.File && c.At.Equal(other.At)
}

func (c FileInputClaim) lastIssuance() time.Time {
	if c.LastIssuedAt.After(c.At) {
		return c.LastIssuedAt
	}
	return c.At
}

type fileInputClaimKey struct{}
type fileDeclaredOutputKey struct{}

func (q *Queue) inputClaimsRequired() bool {
	for _, p := range q.s.Policies {
		if p.MaxRetainedFileBytes > 0 || p.MaxAddedFileBytes24h > 0 || p.ReservedFileBytes > 0 {
			return true
		}
	}
	return false
}

func (q *Queue) claimFileInput(ctx context.Context, s Spec, ref FileRef) (FileInputClaim, error) {
	var out FileInputClaim
	if !validFileProject(s.Project) {
		return out, errors.New("input claim requires project")
	}
	if err := ref.validate(); err != nil {
		return out, err
	}
	if s.Class == "" {
		if err := lockUploadContext(ctx, q.mu.RLock, q.mu.RUnlock); err != nil {
			return out, err
		}
		s.Class = q.s.Policies[s.Project].DefaultCallPriority
		q.mu.RUnlock()
	}
	if err := normalizeJobClass(&s); err != nil {
		return out, err
	}
	// Serialize receipt issuance with removal of the matching interrupted upload.
	if err := lockUploadContext(ctx, q.fileLifecycle.RLock, q.fileLifecycle.RUnlock); err != nil {
		return out, err
	}
	defer q.fileLifecycle.RUnlock()
	// Settlement transfers quota attribution to the job, but concurrent uploads
	// may still hold this receipt. An unrelated claim grants no revocation
	// authority. Interrupted upload collection retires a claim with its transfer.
	err := q.updateUploadRequest(ctx, func() error {
		now := q.clock.Now()
		cutoff := now.Add(-fileStoreQuotaWindow)
		for id, claim := range q.s.FileInputClaims {
			if claim.Released {
				if !claim.SettledAt.After(cutoff) {
					delete(q.editInputClaims(), id)
				}
				continue
			}
			if claim.Project == s.Project && claim.Class == s.Class && claim.File == ref {
				if claim.SettledAt.IsZero() && now.After(claim.lastIssuance()) {
					claim.LastIssuedAt = now
					q.editInputClaims()[id] = claim
				}
				out = claim
				return nil
			}
		}
		if err := q.fileStoreQuotaRefusal(s, FileManifest{"input": ref}, 0); err != nil {
			return err
		}
		out = FileInputClaim{ID: ID(), Project: s.Project, Class: s.Class, File: ref, At: now}
		q.editInputClaims()[out.ID] = out
		return nil
	}, nil)
	return out, err
}

func inputClaimError() error {
	return &HTTPError{Status: http.StatusConflict, Code: "file_input_claim_required", Message: "input upload requires a queue-issued claim for this project and file; use a current submit/profile client or POST /api/files/input-claims before uploading"}
}

// Clone once on the first edit so unrelated transactions can borrow the
// original map for rollback. Every write invalidates the prepared encoding.
func (q *Queue) editInputClaims() map[string]FileInputClaim {
	if q.records != nil && q.records.updating && !q.records.editingInputClaims {
		q.s.FileInputClaims = maps.Clone(q.s.FileInputClaims)
		q.records.editingInputClaims = true
	}
	if q.s.FileInputClaims == nil {
		q.s.FileInputClaims = map[string]FileInputClaim{}
	}
	q.fileInputClaimsJSON = nil
	return q.s.FileInputClaims
}

func (q *Queue) forgetFileInputClaims(hash string, before time.Time) {
	for id, claim := range q.s.FileInputClaims {
		if !claim.Released && claim.File.SHA256 == hash && !claim.lastIssuance().After(before) {
			delete(q.editInputClaims(), id)
		}
	}
}

// Caller holds q.mu. Snapshot the receipts separately from committed file
// references so an idle, unpublished transfer can release its quota charge.
func (q *Queue) unsettledInputClaims() map[string][]FileInputClaim {
	claims := map[string][]FileInputClaim{}
	for _, claim := range q.s.FileInputClaims {
		if claim.SettledAt.IsZero() {
			hash := claim.File.SHA256
			claims[hash] = append(claims[hash], claim)
		}
	}
	return claims
}

func allocatedInputClaim(claims []FileInputClaim, size int64) bool {
	for _, claim := range claims {
		if claim.Allocated && claim.File.SizeBytes == size {
			return true
		}
	}
	return false
}

// No disk work: the immutable server record, never a mutable project header,
// controls quota attribution and resume. Call before opening a transfer.
func (q *Queue) inputClaimContext(ctx context.Context, ref FileRef, allocate bool) (context.Context, error) {
	id, _ := ctx.Value(fileInputClaimKey{}).(string)
	if err := lockUploadContext(ctx, q.mu.RLock, q.mu.RUnlock); err != nil {
		return ctx, err
	}
	claim, found := q.s.FileInputClaims[id]
	required := q.inputClaimsRequired()
	if _, ok := ctx.Value(fileStoreAttemptKey{}).(fileStoreAttempt); ok {
		required = false
	}
	valid := found && !claim.Released && (ref.SHA256 == "" || claim.File == ref || ref.SizeBytes == -1 && claim.File.SHA256 == ref.SHA256)
	declaredOutput := false
	if ctx.Value(fileStoreOrdinaryKey{}) != nil {
		required = false
	}
	// Legacy async workers declare immutable output identities before transfer.
	// Keep this completed-output route outside input quota/claim admission.
	if id == "" && required {
		for _, j := range q.liveJobs() {
			if terminal(j.State) || cancelled(j) || j.OutputAdmissionPending {
				continue
			}
			for _, pending := range j.PendingOutputRefs {
				if pending.SHA256 == ref.SHA256 && (ref.SizeBytes == -1 || pending == ref) {
					required = false
					declaredOutput = true
				}
			}
		}
	}
	q.mu.RUnlock()
	if id == "" {
		if required {
			return ctx, inputClaimError()
		}
		if declaredOutput {
			ctx = context.WithValue(ctx, fileDeclaredOutputKey{}, true)
		}
		return ctx, nil
	}
	if !valid {
		return ctx, inputClaimError()
	}
	if s, ok := ctx.Value(fileStoreSubmissionKey{}).(Spec); ok && (s.Project != claim.Project || s.Class != "" && s.Class != claim.Class) {
		return ctx, inputClaimError()
	}
	if allocate && !claim.Allocated {
		if err := q.updateUploadRequest(ctx, func() error {
			current, ok := q.s.FileInputClaims[id]
			if ok && !current.Released && (current.Allocated || !current.SettledAt.IsZero()) {
				return nil
			}
			if !ok || current.Released || !current.sameIdentity(claim) {
				return inputClaimError()
			}
			current.Allocated = true
			q.editInputClaims()[id] = current
			return nil
		}, nil); err != nil {
			return ctx, err
		}
	}
	return context.WithValue(ctx, fileStoreSubmissionKey{}, Spec{Project: claim.Project, Class: claim.Class}), nil
}

func (q *Queue) noteInputClaimFacts(f *projectFileFacts, project string) {
	for _, claim := range q.s.FileInputClaims {
		if project != "" && claim.Project != project {
			continue
		}
		if claim.SettledAt.IsZero() || claim.Released {
			fact := projectFileFact{project: claim.Project, hash: claim.File.SHA256, size: uint64(claim.File.SizeBytes), at: claim.At}
			f.added = append(f.added, fact)
			if !claim.Released {
				f.retained = append(f.retained, fact)
			}
		}
	}
}

// The job's ordinary reference now carries attribution/retention. This runs in
// the same transaction as submission and never removes payload bytes.
func (q *Queue) settleFileInputClaims(s Spec) {
	now := q.clock.Now()
	var delay time.Duration
	for _, ref := range submissionFiles(s) {
		for id, claim := range q.s.FileInputClaims {
			if !claim.Released && claim.Project == s.Project && claim.File == ref {
				// A settled claim is reused by later submissions of the same bytes.
				// Its age then measures reuse, not a publication delay: live, one
				// reused claim lifted the horizon past file retention (#1375).
				if claim.SettledAt.IsZero() {
					delay = max(delay, now.Sub(claim.At))
				}
				claim.SettledAt = now
				q.editInputClaims()[id] = claim
			}
		}
	}
	if delay > 0 {
		q.recordPublicationTiming(now, delay)
	}
}

type FileInputClaimRelease struct {
	Project      string `json:"project"`
	ClaimID      string `json:"claim_id"`
	AcceptExpiry bool   `json:"accept_expiry"`
}

type FileInputClaimReleaseResult struct {
	ClaimID    string    `json:"claim_id"`
	ReleasedAt time.Time `json:"released_at"`
}

func (q *Queue) ReleaseFileInputClaim(ctx context.Context, req FileInputClaimRelease) (FileInputClaimReleaseResult, error) {
	var result FileInputClaimReleaseResult
	if !validFileProject(req.Project) || req.ClaimID == "" || !req.AcceptExpiry {
		return result, fileReleaseInvalid("claim release requires a project, one claim ID and explicit acceptance")
	}
	// Revoke only outside active transfer/verification intervals. TryLock keeps
	// an owner release from queuing a writer ahead of unrelated publications.
	if !q.fileLifecycle.TryLock() {
		return result, fileReleaseError("file_release_busy", "file publication is active; retry claim release")
	}
	defer q.fileLifecycle.Unlock()
	unlock := requestReadLock(ctx, &q.mu)
	claim, found := q.s.FileInputClaims[req.ClaimID]
	store := q.files
	_, marked := q.jobUploadMarker(claim.File.SHA256)
	_, bundle := q.s.BundleUploads[claim.File.SHA256]
	unlock()
	if !found || claim.Project != req.Project {
		return result, fileReleaseInvalid("claim is unavailable or belongs to another project")
	}
	if claim.Released {
		return FileInputClaimReleaseResult{claim.ID, claim.SettledAt}, nil
	}
	if !claim.SettledAt.IsZero() {
		return result, fileReleaseError("file_release_protected", "claim settled into a job; release the finished job")
	}
	if store == nil {
		return result, &HTTPError{Status: http.StatusServiceUnavailable, Code: "file_store_unavailable", Message: "queue file store inventory unavailable"}
	}
	lastActivity, err := store.inputClaimActivity(claim)
	if err != nil {
		return result, fileReleaseError("file_release_busy", "cannot read input transfer activity; retry claim release")
	}
	// A legacy queue sometimes lost the upload marker. Preserve ordinary idle
	// collection authority only for an exact legacy partial transfer. A claim
	// reusing unmarked published content cannot authorize collecting it.
	markPartial := false
	if claim.Allocated && !marked && !bundle {
		var err error
		markPartial, err = store.legacyPartialInput(ctx, claim.File)
		if err != nil {
			return result, fileReleaseError("file_release_busy", "cannot read input transfer custody; retry claim release")
		}
	}
	err = q.updateRequest(ctx, func() error {
		current, found := q.s.FileInputClaims[req.ClaimID]
		if q.files != store || !found || !current.equal(claim) {
			return fileReleaseError("file_release_stale", "claim custody changed; inspect holdings again")
		}
		// The configured interrupted-upload idle window and measured
		// upload-to-submission grace both have to elapse. Lifecycle exclusion
		// alone would cover each request but miss the gaps between them.
		if !q.filePublicationAge(q.clock.Now()).interruptedUploadIdle(lastActivity, nil) {
			return fileReleaseError("file_release_protected", "input claim is within the upload idle window or publication grace")
		}
		if markPartial {
			if _, marked := q.jobUploadMarker(claim.File.SHA256); !marked {
				if q.s.JobInputUploads == nil {
					q.s.JobInputUploads = map[string]time.Time{}
				}
				q.s.JobInputUploads[claim.File.SHA256] = time.Time{}
			}
		}
		claim.SettledAt, claim.Released = q.clock.Now(), true
		q.editInputClaims()[claim.ID] = claim
		result = FileInputClaimReleaseResult{claim.ID, claim.SettledAt}
		return nil
	}, nil)
	return result, err
}

// Read only metadata for this digest. The caller holds the lifecycle write
// fence so upload writes, publication and claim issuance cannot race the check.
func (s *FileStore) inputClaimActivity(claim FileInputClaim) (time.Time, error) {
	if err := claim.File.validate(); err != nil {
		return time.Time{}, err
	}
	at := claim.lastIssuance()
	upload := filepath.Join(s.root, "uploads", claim.File.SHA256)
	for _, path := range []string{s.blob(claim.File), upload, upload + ".info"} {
		st, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return time.Time{}, err
		}
		if !st.Mode().IsRegular() {
			return time.Time{}, errors.New("invalid input claim payload")
		}
		if st.ModTime().After(at) {
			at = st.ModTime()
		}
	}
	return at, nil
}

func (s *FileStore) legacyPartialInput(ctx context.Context, ref FileRef) (bool, error) {
	if err := ref.validate(); err != nil {
		return false, err
	}
	if _, err := os.Lstat(s.blob(ref)); !os.IsNotExist(err) {
		return false, err
	}
	upload, err := filestore.New(filepath.Join(s.root, "uploads")).GetUpload(ctx, ref.SHA256)
	if errors.Is(err, tus.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	info, err := upload.GetInfo(ctx)
	return err == nil && jobInputUpload(info, ref.SHA256) && info.Size == ref.SizeBytes, err
}

type filePublicationSample struct {
	At    time.Time     `json:"at"`
	Delay time.Duration `json:"delay"`
}

type filePublicationTimings struct {
	MeasuredSince time.Time               `json:"measured_since"`
	LegacyGrace   time.Duration           `json:"legacy_grace"`
	Peaks         []filePublicationSample `json:"peaks"`
}

// Keep a longer observation while it describes the retention window or the
// measured delay itself. Once newer observations cover that period, an old peak
// cannot govern collection forever. With no new evidence, keep the latest.
func (s filePublicationSample) recent(now time.Time, retention time.Duration) bool {
	return now.Sub(s.At) < max(s.Delay, retention)
}

// The timing snapshot is immutable for queue transaction rollback. A newer
// equal or longer sample dominates an older one for both delay and freshness.
func (q *Queue) recordPublicationTiming(now time.Time, delay time.Duration) {
	next := filePublicationTimings{MeasuredSince: now, LegacyGrace: q.s.PublicationHorizon}
	if previous := q.s.PublicationTimings; previous != nil {
		next = *previous
		next.Peaks = nil
		for _, sample := range previous.Peaks {
			if sample.Delay > delay && sample.recent(now, q.fileRetention) {
				next.Peaks = append(next.Peaks, sample)
			}
		}
	}
	next.Peaks = append(next.Peaks, filePublicationSample{At: now, Delay: delay})
	q.s.PublicationTimings = &next
	// An adjacent older release still reads this field on rollback.
	q.s.PublicationHorizon = max(q.s.PublicationHorizon, delay)
}

// Admission pools reserve only each other project's unused entitlement. No
// eviction or retention change; exact worker output transfers remain exempt.
func (q *Queue) projectFileReservationRefusal(ctx context.Context, status FileStoreStatus, payloads map[string]storedPayload, need, growth uint64) error {
	if growth == 0 || ctx.Value(fileStoreAttemptKey{}) != nil || ctx.Value(fileDeclaredOutputKey{}) != nil {
		return nil
	}
	s, identified := ctx.Value(fileStoreSubmissionKey{}).(Spec)
	if identified && q.priorityMatch(s).Tier == "production" {
		return nil
	}
	var withheld uint64
	for project, policy := range q.s.Policies {
		if policy.ReservedFileBytes == 0 || identified && project == s.Project {
			continue
		}
		jobs := q.projectFileJobs(project)
		var facts projectFileFacts
		q.collectFileReferences(false, nil, facts.retain, jobs)
		q.noteProjectFileAdditions(&facts, jobs)
		q.noteInputClaimFacts(&facts, project)
		covered := map[string]uint64{}
		used := uint64(0)
		if l := facts.ledgers()[project]; l != nil {
			for hash, size := range l.retained {
				if p, ok := payloads[hash]; ok {
					covered[hash] = min(size, p.length)
				}
			}
			used = addDiskBytes(used, l.reserved)
		}
		// Exact missing output identities already consume the global need.
		// Count them against their owner's entitlement even before allocation.
		for _, j := range jobs {
			if terminal(j.State) || cancelled(j) || j.OutputAdmissionPending || j.Spec.OutputDelivery != asyncNAS {
				continue
			}
			for _, ref := range j.PendingOutputRefs {
				covered[ref.SHA256] = max(covered[ref.SHA256], uint64(ref.SizeBytes))
			}
		}
		for _, size := range covered {
			used = addDiskBytes(used, size)
		}
		withheld = addDiskBytes(withheld, policy.ReservedFileBytes-min(policy.ReservedFileBytes, used))
	}
	available := status.FreeBytes - min(status.FreeBytes, withheld)
	if need > available {
		cause := q.capacityCause(status, fileCapacityProject, need, growth, 0, false)
		cause.FileCapacity.AvailableBytes, cause.FileCapacity.ProjectWithheldBytes = available, withheld
		return &HTTPError{Cause: cause, Status: http.StatusInsufficientStorage, Code: "file_store_capacity", Message: fmt.Sprintf("queue-file-store: need=%d bytes available=%d bytes; other projects' reserved_file_bytes=%d protect capacity; release collected results or reduce new input bytes", need, available, withheld)}
	}
	return nil
}
