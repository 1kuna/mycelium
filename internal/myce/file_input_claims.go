package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// A queue-issued claim binds a pre-submission transfer to its project and exact
// bytes. The fleet still authenticates one operator, not separate project users.
type FileInputClaim struct {
	ID        string    `json:"id"`
	Project   string    `json:"project"`
	Class     string    `json:"class,omitempty"`
	File      FileRef   `json:"file"`
	At        time.Time `json:"at"`
	Allocated bool      `json:"allocated,omitempty"`
	SettledAt time.Time `json:"settled_at,omitempty"`
}

// Rollback and reopen round-trip timestamps through JSON. Compare instants,
// not time.Time's location pointers or process-local monotonic readings.
func (c FileInputClaim) equal(other FileInputClaim) bool {
	return c.ID == other.ID && c.Project == other.Project && c.Class == other.Class &&
		c.File == other.File && c.Allocated == other.Allocated &&
		c.At.Equal(other.At) && c.SettledAt.Equal(other.SettledAt)
}

// A claim is explicit upload/submission intent. Elapsed time cannot release
// its bytes or reservation; settlement transfers accounting to the job.
func (c FileInputClaim) valid(_ time.Time) bool { return true }

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
	if strings.TrimSpace(s.Project) == "" || strings.ContainsAny(s.Project, "\r\n\x00") {
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
	// Settlement transfers quota attribution to the job, but concurrent uploads
	// may still hold this receipt. An unrelated claim grants no revocation
	// authority. Payload collection forgets receipts only when their bytes leave.
	err := q.updateUploadRequest(ctx, func() error {
		for _, claim := range q.s.FileInputClaims {
			if claim.Project == s.Project && claim.Class == s.Class && claim.File == ref && claim.valid(q.clock.Now()) {
				out = claim
				return nil
			}
		}
		if err := q.fileStoreQuotaRefusal(s, FileManifest{"input": ref}, 0); err != nil {
			return err
		}
		out = FileInputClaim{ID: ID(), Project: s.Project, Class: s.Class, File: ref, At: q.clock.Now()}
		if q.s.FileInputClaims == nil {
			q.s.FileInputClaims = map[string]FileInputClaim{}
		}
		q.s.FileInputClaims[out.ID] = out
		return nil
	}, nil)
	return out, err
}

func inputClaimError() error {
	return &HTTPError{Status: http.StatusConflict, Code: "file_input_claim_required", Message: "input upload requires a queue-issued claim for this project and file; use a current submit/profile client or POST /api/files/input-claims before uploading"}
}

func (q *Queue) forgetFileInputClaims(hash string, before time.Time) {
	for id, claim := range q.s.FileInputClaims {
		if claim.File.SHA256 == hash && !claim.At.After(before) {
			delete(q.s.FileInputClaims, id)
		}
	}
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
	valid := found && (ref.SHA256 == "" || claim.File == ref || ref.SizeBytes == -1 && claim.File.SHA256 == ref.SHA256) && claim.valid(q.clock.Now())
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
			if ok && current.valid(q.clock.Now()) && (current.Allocated || !current.SettledAt.IsZero()) {
				return nil
			}
			if !ok || !current.equal(claim) || !current.valid(q.clock.Now()) {
				return inputClaimError()
			}
			current.Allocated = true
			q.s.FileInputClaims[id] = current
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
		if !claim.SettledAt.IsZero() || !claim.valid(q.clock.Now()) {
			continue
		}
		fact := projectFileFact{project: claim.Project, hash: claim.File.SHA256, size: uint64(claim.File.SizeBytes), at: claim.At}
		f.retained = append(f.retained, fact)
		f.added = append(f.added, fact)
	}
}

// The job's ordinary reference now carries attribution/retention. This runs in
// the same transaction as submission and never removes payload bytes.
func (q *Queue) settleFileInputClaims(s Spec) {
	for _, ref := range submissionFiles(s) {
		for id, claim := range q.s.FileInputClaims {
			if claim.Project == s.Project && claim.File == ref {
				// A settled claim is reused by later submissions of the same bytes.
				// Its age then measures reuse, not a publication delay: live, one
				// reused claim lifted the horizon past file retention (#1375).
				if claim.SettledAt.IsZero() {
					q.s.PublicationHorizon = max(q.s.PublicationHorizon, q.clock.Now().Sub(claim.At))
				}
				claim.SettledAt = q.clock.Now()
				q.s.FileInputClaims[id] = claim
			}
		}
	}
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
		return &HTTPError{Status: http.StatusInsufficientStorage, Code: "file_store_capacity", Message: fmt.Sprintf("queue-file-store: need=%d bytes available=%d bytes; other projects' reserved_file_bytes=%d protect capacity; release collected results or reduce new input bytes", need, available, withheld)}
	}
	return nil
}
