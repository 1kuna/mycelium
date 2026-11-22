package myce

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// ProjectFileHoldings explains the same deduplicated attribution as status.
// StoredBytes measures observed payload, including the written portion of
// uploads. RetainedBytes includes the declared sizes of claims and pending
// outputs even when their transfers are incomplete.
type ProjectFileHoldings struct {
	Project       string                      `json:"project"`
	CapturedAt    time.Time                   `json:"captured_at"`
	RetainedBytes uint64                      `json:"retained_bytes"`
	StoredBytes   uint64                      `json:"stored_bytes"`
	SharedBytes   uint64                      `json:"shared_bytes"`
	ByKind        map[string]FileHoldingUsage `json:"by_kind"`
	Files         []ProjectHeldFile           `json:"files"`
}

type FileHoldingUsage struct {
	RetainedBytes uint64 `json:"retained_bytes"`
	StoredBytes   uint64 `json:"stored_bytes"`
	FileCount     int    `json:"file_count"`
}

type ProjectHeldFile struct {
	SHA256           string                 `json:"sha256"`
	RetainedBytes    uint64                 `json:"retained_bytes"`
	StoredBytes      uint64                 `json:"stored_bytes"`
	State            string                 `json:"state"` // published, uploading, absent, or unknown
	OtherHolderKinds []string               `json:"other_holder_kinds,omitempty"`
	Shared           bool                   `json:"shared"`          // held by other projects or queue-wide custody
	DurableCustody   bool                   `json:"durable_custody"` // a keeping reference independent of input claims
	Holders          []FileKeepingReference `json:"holders"`
	InputClaims      []FileHoldingClaim     `json:"input_claims,omitempty"`
}

type FileHoldingClaim struct {
	ClaimID      string    `json:"claim_id"`
	LastActivity time.Time `json:"last_activity,omitzero"`
	IdleSeconds  *float64  `json:"idle_seconds,omitempty"`
	Eligibility  string    `json:"release_eligibility"` // eligible, protected, or unknown
	Reason       string    `json:"reason"`
	EligibleAt   time.Time `json:"eligible_at,omitzero"`
}

type fileHoldingFact struct {
	file  FileRef
	owner FileKeepingReference
}

type fileHoldingPayload struct {
	claimPayloadObservation
	err error
}

type fileHoldingsSnapshot struct {
	at       time.Time
	facts    []fileHoldingFact
	payloads map[string]fileHoldingPayload
	claims   map[string]FileHoldingClaim
}

func (q *Queue) ProjectFileHoldings(ctx context.Context, project string) (ProjectFileHoldings, error) {
	if !validFileProject(project) {
		return ProjectFileHoldings{}, &HTTPError{Status: http.StatusBadRequest, Code: "file_holdings_invalid", Message: "holdings requires a project"}
	}
	snapshot, err := q.captureProjectFileHoldings(ctx, project)
	if err != nil {
		return ProjectFileHoldings{}, err
	}
	// Check the same nonblocking fence a release would need, after dropping
	// this read's own shared lock. A busy fence has no predictable deadline.
	if len(snapshot.claims) != 0 {
		if q.fileLifecycle.TryLock() {
			q.fileLifecycle.Unlock()
		} else {
			for id, detail := range snapshot.claims {
				if detail.Eligibility == "eligible" {
					detail.Eligibility, detail.Reason = "unknown", "publication_busy"
					snapshot.claims[id] = detail
				}
			}
		}
	}
	return summarizeProjectFileHoldings(project, snapshot), nil
}

func (q *Queue) captureProjectFileHoldings(ctx context.Context, project string) (fileHoldingsSnapshot, error) {
	// The lifecycle fence prevents collection between the custody and payload
	// reads. Publication can proceed. Only copy references under the queue lock.
	if err := lockUploadContext(ctx, q.fileLifecycle.RLock, q.fileLifecycle.RUnlock); err != nil {
		return fileHoldingsSnapshot{}, err
	}
	defer q.fileLifecycle.RUnlock()
	unlock, err := requestObservationLock(ctx, &q.mu)
	if err != nil {
		return fileHoldingsSnapshot{}, err
	}
	store, at := q.files, q.clock.Now()
	if store == nil {
		unlock()
		return fileHoldingsSnapshot{}, fileHoldingsUnavailable()
	}
	type claimSnapshot struct {
		claim       FileInputClaim
		needsLegacy bool
	}
	claims := map[string]claimSnapshot{}
	snapshot := fileHoldingsSnapshot{
		at: at, payloads: map[string]fileHoldingPayload{}, claims: map[string]FileHoldingClaim{},
	}
	age := q.filePublicationAge(at)
	q.collectFileReferences(true, nil, func(file FileRef, owner FileKeepingReference) {
		if file.SHA256 != "" {
			snapshot.facts = append(snapshot.facts, fileHoldingFact{file, owner})
			if owner.Project == project && owner.Kind == "input_claim" {
				claim := q.s.FileInputClaims[owner.Name]
				claims[owner.Name] = claimSnapshot{claim, q.inputClaimNeedsLegacyCustody(claim)}
			}
		}
	})
	unlock()

	// A missing parent is a store observation failure, not an absent digest.
	var storeErr error
	for _, dir := range []string{"blobs", "uploads"} {
		st, err := os.Stat(filepath.Join(store.root, dir))
		if err != nil || !st.IsDir() {
			storeErr = errors.New("file store directory unavailable")
			break
		}
	}
	for _, fact := range snapshot.facts {
		if fact.owner.Project != project || fact.owner.Kind == "worker_local_custody" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return fileHoldingsSnapshot{}, err
		}
		hash := fact.file.SHA256
		if _, observed := snapshot.payloads[hash]; !observed {
			payload, err := claimPayloadObservation{}, storeErr
			if err == nil {
				payload, err = store.observeClaimPayload(FileRef{SHA256: hash})
			}
			snapshot.payloads[hash] = fileHoldingPayload{payload, err}
		}
	}
	for id, saved := range claims {
		if err := ctx.Err(); err != nil {
			return fileHoldingsSnapshot{}, err
		}
		payload := snapshot.payloads[saved.claim.File.SHA256]
		detail := FileHoldingClaim{ClaimID: id, Eligibility: "unknown"}
		switch activity := payload.lastActivity(saved.claim); {
		case payload.err != nil || activity.IsZero():
			detail.Reason = "activity_unavailable"
		case activity.After(at):
			detail.Reason = "activity_after_snapshot"
		default:
			idle := at.Sub(activity).Seconds()
			detail.LastActivity, detail.IdleSeconds = activity, &idle
			detail.Reason, detail.EligibleAt = age.inputClaimReleaseAge(activity)
			detail.Eligibility = "protected"
			if detail.Reason == claimReleaseAgeElapsed {
				detail.Eligibility = "eligible"
			}
			if saved.needsLegacy {
				if _, err := store.legacyPartialInput(ctx, saved.claim.File); err != nil {
					detail.Eligibility, detail.Reason, detail.EligibleAt = "unknown", "custody_unavailable", time.Time{}
				}
			}
		}
		snapshot.claims[id] = detail
	}
	// Check the observation fences again. Uploads may run under the shared
	// lifecycle lock and a job may settle a claim while metadata is sampled.
	unlock = requestReadLock(ctx, &q.mu)
	current := q.files == store
	for id, saved := range claims {
		if claim, ok := q.s.FileInputClaims[id]; !ok || !claim.equal(saved.claim) ||
			q.inputClaimNeedsLegacyCustody(claim) != saved.needsLegacy {
			snapshot.claims[id] = FileHoldingClaim{ClaimID: id, Eligibility: "unknown", Reason: "custody_changed"}
		}
	}
	unlock()
	if !current {
		return fileHoldingsSnapshot{}, fileHoldingsUnavailable()
	}
	return snapshot, nil
}

func fileHoldingsUnavailable() error {
	return &HTTPError{Status: http.StatusServiceUnavailable, Code: "file_holdings_unavailable", Message: "file store custody observation unavailable; retry the read"}
}

func validFileProject(project string) bool {
	return strings.TrimSpace(project) != "" && !strings.ContainsAny(project, "\r\n\x00")
}

func summarizeProjectFileHoldings(project string, snapshot fileHoldingsSnapshot) ProjectFileHoldings {
	out := ProjectFileHoldings{Project: project, CapturedAt: snapshot.at, ByKind: map[string]FileHoldingUsage{}, Files: []ProjectHeldFile{}}
	type heldFile struct {
		ProjectHeldFile
		holders map[FileKeepingReference]struct{}
		kinds   map[string]uint64
	}
	files := map[string]*heldFile{}
	for _, f := range snapshot.facts {
		if f.owner.Project != project || f.owner.Kind == "worker_local_custody" {
			continue
		}
		hash := f.file.SHA256
		item := files[hash]
		if item == nil {
			item = &heldFile{ProjectHeldFile: ProjectHeldFile{SHA256: hash, State: "absent"}, holders: map[FileKeepingReference]struct{}{}, kinds: map[string]uint64{}}
			p := snapshot.payloads[hash]
			if p.err != nil {
				item.State = "unknown"
			} else if p.present {
				item.StoredBytes, item.State = p.payload.size, "published"
				if p.payload.upload {
					item.State = "uploading"
				}
			}
			files[hash] = item
		}
		var charge uint64
		if projectFileCharge(f.owner) {
			charge = uint64(f.file.SizeBytes)
		}
		item.RetainedBytes = max(item.RetainedBytes, charge)
		item.kinds[f.owner.Kind] = max(item.kinds[f.owner.Kind], charge)
		// Exclude duplicated provenance (for example a checkpoint in both the
		// locally accepted manifest and the resume pointer).
		item.holders[f.owner] = struct{}{}
	}
	for _, f := range snapshot.facts {
		if item := files[f.file.SHA256]; item != nil {
			item.DurableCustody = item.DurableCustody || f.owner.Kind != "input_claim"
			if f.owner.Project != project {
				item.Shared = true
				item.OtherHolderKinds = append(item.OtherHolderKinds, f.owner.Kind)
			}
		}
	}
	for _, item := range files {
		for holder := range item.holders {
			item.Holders = append(item.Holders, holder)
			if detail, ok := snapshot.claims[holder.Name]; ok && holder.Kind == "input_claim" {
				item.InputClaims = append(item.InputClaims, detail)
			}
		}
		slices.SortFunc(item.Holders, compareFileHolders)
		slices.SortFunc(item.InputClaims, func(a, b FileHoldingClaim) int { return strings.Compare(a.ClaimID, b.ClaimID) })
		slices.Sort(item.OtherHolderKinds)
		item.OtherHolderKinds = slices.Compact(item.OtherHolderKinds)
		for kind, charge := range item.kinds {
			s := out.ByKind[kind]
			s.RetainedBytes = addDiskBytes(s.RetainedBytes, charge)
			s.StoredBytes = addDiskBytes(s.StoredBytes, item.StoredBytes)
			s.FileCount++
			out.ByKind[kind] = s
		}
		out.RetainedBytes = addDiskBytes(out.RetainedBytes, item.RetainedBytes)
		out.StoredBytes = addDiskBytes(out.StoredBytes, item.StoredBytes)
		if item.Shared {
			out.SharedBytes = addDiskBytes(out.SharedBytes, item.StoredBytes)
		}
		out.Files = append(out.Files, item.ProjectHeldFile)
	}
	slices.SortFunc(out.Files, func(a, b ProjectHeldFile) int { return strings.Compare(a.SHA256, b.SHA256) })
	return out
}

func compareFileHolders(a, b FileKeepingReference) int {
	if c := strings.Compare(a.Kind, b.Kind); c != 0 {
		return c
	}
	if c := strings.Compare(a.JobID, b.JobID); c != 0 {
		return c
	}
	return strings.Compare(a.Name, b.Name)
}
