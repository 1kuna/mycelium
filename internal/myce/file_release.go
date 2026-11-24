package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode"
)

// Project is attribution under the shared fleet credential, not authorization.
// Explicit job IDs define the owner's completion boundary (including any parents,
// children and downstream consumers). There is deliberately no project-wide glob.
type FileReleaseSelection struct {
	Project            string   `json:"project"`
	Jobs               []string `json:"jobs"`
	IncludeCheckpoints bool     `json:"include_checkpoints,omitempty"`
}

type fileReleaseScope struct {
	checkpoints bool
}

type FileKeepingReference struct {
	Kind    string `json:"kind"`
	JobID   string `json:"job_id,omitempty"`
	Project string `json:"project,omitempty"`
	Name    string `json:"name,omitempty"`
}

type FileReleaseFile struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	FileRef
}

type FileReleaseJob struct {
	ID                 string                      `json:"id"`
	Project            string                      `json:"project"`
	State              string                      `json:"state"`
	Release            string                      `json:"release"`
	CustodySHA256      string                      `json:"custody_sha256"`
	FilesExpired       bool                        `json:"files_expired"`
	ExpiresAt          *time.Time                  `json:"expires_at,omitempty"`
	Files              []FileReleaseFile           `json:"files"`
	Blockers           []string                    `json:"blockers"`
	CustodyBlockers    []FileReleaseCustodyBlocker `json:"custody_blockers,omitempty"`
	DeliveryState      string                      `json:"delivery_state,omitempty"`
	DeliveryManifest   string                      `json:"delivery_manifest,omitempty"`
	DeliveryVerifiedAt *time.Time                  `json:"delivery_verified_at,omitempty"`
}

type FileReleaseBlob struct {
	SHA256               string                 `json:"sha256"`
	SizeBytes            int64                  `json:"size_bytes"`
	Present              bool                   `json:"present"`
	ModifiedAt           time.Time              `json:"modified_at"`
	CollectionEligible   bool                   `json:"collection_eligible"`
	ReclaimableBytes     int64                  `json:"reclaimable_bytes"`
	References           []FileKeepingReference `json:"references"`
	BundleTracked        bool                   `json:"bundle_tracked"`
	BundleProtected      bool                   `json:"bundle_protected"`
	PublicationProtected bool                   `json:"publication_protected"`
}

type FileReleasePlan struct {
	ID               string               `json:"id"`
	GeneratedAt      time.Time            `json:"generated_at"`
	Selection        FileReleaseSelection `json:"selection"`
	Jobs             []FileReleaseJob     `json:"jobs"`
	Blobs            []FileReleaseBlob    `json:"blobs"`
	ReclaimableBytes int64                `json:"reclaimable_bytes"`
	// One fingerprint per eligible blob, carried back on apply to verify
	// that live collection is still within the reviewed scope.
	CollectionCandidates []string `json:"collection_candidates,omitempty"`
}

// Acceptance receipts use the existing retained job ledger, including keyed
// answers. Overlapping selections cannot overwrite a previous apply receipt.
const fileReleaseLedgerKind = "file_expiry_accepted"

// Both explicit owner release and authored delivery retention enroll the same
// durable collector transaction. Neither path deletes bytes or bypasses pins.
func (q *Queue) acceptFileExpiry(j *Job, reason, selectionSHA256 string) {
	e := ledgerAttempt(j, fileReleaseLedgerKind, reason)
	e.Release = q.observedRelease(j)
	e.FileReleaseSelectionSHA256 = selectionSHA256
	q.appendJobLedger(j, e)
	j.FilesExpired = true
	enrollFileReclamation(j)
}

func fileReleaseEntry(j *Job, planID string) *JobLedgerEntry {
	for i := len(j.LedgerEntries) - 1; i >= 0; i-- {
		e := &j.LedgerEntries[i]
		if e.Kind == fileReleaseLedgerKind && e.Reason == planID {
			return e
		}
	}
	return nil
}

// q.mu held. Validate the complete group before acknowledging a replay.
func (q *Queue) fileReleaseReplay(selection FileReleaseSelection, planID, selectionSHA256 string) (FileReleaseResult, error) {
	var result FileReleaseResult
	for _, id := range selection.Jobs {
		j := q.retainedJob(id)
		if j == nil || j.Spec.Project != selection.Project {
			return FileReleaseResult{}, nil
		}
		e := fileReleaseEntry(j, planID)
		if e == nil {
			return FileReleaseResult{}, nil
		}
		if e.FileReleaseSelectionSHA256 != selectionSHA256 {
			return FileReleaseResult{}, fileReleaseError("file_release_stale", "release receipt does not bind this selection; generate and review a new plan")
		}
		if result.PlanID == "" {
			result = FileReleaseResult{PlanID: planID, AcceptedAt: e.At, Collection: "scheduled"}
		}
	}
	return result, nil
}

type FileReleaseApply struct {
	Selection            FileReleaseSelection `json:"selection"`
	PlanID               string               `json:"plan_id"`
	AcceptExpiry         bool                 `json:"accept_expiry"`
	CollectionCandidates []string             `json:"collection_candidates,omitempty"`
}

type FileReleaseResult struct {
	PlanID     string    `json:"plan_id"`
	AcceptedAt time.Time `json:"accepted_at"`
	// Collection is asynchronous through ordinary maintenance, not NAS delivery or
	// synchronous deletion. Pending hashes remain durable across restart.
	Collection string `json:"collection"`
}

func fileReleaseError(code, message string) error {
	return &HTTPError{Status: http.StatusConflict, Code: code, Message: message}
}

func fileReleaseInvalid(message string) error {
	return &HTTPError{Status: http.StatusBadRequest, Code: "file_release_invalid", Message: message}
}

func normalizeFileRelease(s FileReleaseSelection) (FileReleaseSelection, error) {
	s.Jobs = slices.Clone(s.Jobs)
	slices.Sort(s.Jobs)
	s.Jobs = slices.Compact(s.Jobs)
	if s.Project == "" || len(s.Jobs) == 0 || slices.Contains(s.Jobs, "") {
		return s, fileReleaseInvalid("release requires a project and explicit job IDs")
	}
	for _, id := range s.Jobs {
		if strings.IndexFunc(id, unicode.IsSpace) >= 0 {
			return s, fileReleaseInvalid("pass each selected job ID separately; job IDs cannot contain whitespace")
		}
	}
	return s, nil
}

func fileReleaseDigest(v any) string {
	data, _ := json.Marshal(v)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func fileReleasePlanID(base string, candidates []string) string {
	return fileReleaseDigest(struct {
		Base       string
		Candidates []string
	}{base, candidates})
}

// q.mu held. Only metadata work here; no blob reads, stats or NAS access.
func (q *Queue) fileReleaseMetadata(selection FileReleaseSelection, includeReferences bool) (FileReleasePlan, error) {
	plan := FileReleasePlan{Selection: selection, Jobs: []FileReleaseJob{}, Blobs: []FileReleaseBlob{}}
	releasing := map[string]fileReleaseScope{}
	hashes := map[string]bool{}
	for _, id := range selection.Jobs {
		j := q.retainedJob(id)
		if j == nil || j.Spec.Project != selection.Project {
			return plan, fileReleaseInvalid("selected job is unavailable or belongs to another project")
		}
		item := FileReleaseJob{ID: j.ID, Project: j.Spec.Project, State: j.State, Release: q.observedRelease(j), FilesExpired: j.FilesExpired, Files: []FileReleaseFile{}, Blockers: []string{}}
		item.CustodySHA256 = fileReleaseDigest(struct {
			Attempt          int
			Machine, Session string
			Confirmed        bool
			Copies           []Copy
		}{j.Attempt, j.Machine, j.Session, j.ReleaseConfirmed, j.Copies})
		if j.FinishedAt != nil {
			at := j.FinishedAt.Add(q.jobFileRetention(j))
			item.ExpiresAt = &at
		}
		if parkedStorageCustody(j) {
			item.Blockers = append(item.Blockers, "parked_storage_custody")
		}
		if !terminal(j.State) {
			item.Blockers = append(item.Blockers, "live_job")
		}
		if q.jobReleasePending(j) {
			item.Blockers = append(item.Blockers, "physical_custody")
			item.CustodyBlockers = q.fileReleaseCustodyBlockers(j, selection.Project)
		}
		if deliveryPending(j) || checkpointMirrorsPending(j) {
			item.Blockers = append(item.Blockers, "pending_delivery")
		}
		if j.Spec.localFirst() && !j.FilesExpired && (j.FinishedAt == nil || q.clock.Now().Before(j.FinishedAt.Add(q.jobFileRetention(j)))) {
			item.Blockers = append(item.Blockers, "checkpoint_retention")
		}
		if d := j.Delivery; d != nil {
			item.DeliveryState, item.DeliveryManifest = d.State, d.ManifestSHA256
			if d.VerifiedAt != nil {
				at := *d.VerifiedAt
				item.DeliveryVerifiedAt = &at
			}
			// Required preservation uses the queue's supported verified delivery evidence,
			// bound to the exact accepted execution and manifest, never an owner assertion.
			if d.State == "verified" && (d.VerifiedAt == nil || j.Spec.OutputDelivery != asyncNAS && j.Spec.OutputDestination == nil || d.Execution != (Execution{ID: j.ID, Attempt: j.Attempt}) || !sameDelivery(j, *j)) {
				item.Blockers = append(item.Blockers, "delivery_evidence_mismatch")
			}
		} else if j.Spec.OutputDelivery != "" && len(j.OutputRefs) > 0 {
			item.Blockers = append(item.Blockers, "delivery_evidence_missing")
		}
		add := func(kind string, files FileManifest) {
			for name, ref := range files {
				item.Files = append(item.Files, FileReleaseFile{Kind: kind, Name: name, FileRef: ref})
				hashes[ref.SHA256] = true
			}
		}
		add("input", j.Spec.Files)
		add("shared_input", sharedStoredFiles(j.Spec))
		add("result", j.OutputRefs)
		if j.LocalOutputs != nil {
			add("worker_local_result", j.LocalOutputs.Files)
		}
		if j.Dependency != nil {
			add("dependency_input", j.Dependency.Files)
		}
		for _, a := range j.AttemptHistory {
			if a.Diagnostics != nil {
				add("diagnostic", a.Diagnostics.Files)
			}
		}
		if j.FanOut != nil {
			for key, r := range j.FanOut.Results {
				add("fan_out_result:"+key, r.OutputRefs)
			}
		}
		if selection.IncludeCheckpoints {
			visitLegacyCheckpointFiles(j, add)
		}
		slices.SortFunc(item.Files, func(a, b FileReleaseFile) int { return compareReleaseJSON(a, b) })
		plan.Jobs = append(plan.Jobs, item)
		if len(item.Blockers) == 0 {
			releasing[id] = fileReleaseScope{checkpoints: selection.IncludeCheckpoints}
		}
	}
	refs := q.captureFileReferences(true, releasing, hashes, includeReferences)
	for hash := range hashes {
		if (FileRef{SHA256: hash}).validate() != nil {
			return plan, fileReleaseInvalid("invalid retained content identity")
		}
		entry := refs[hash]
		var keepers []FileKeepingReference
		for owner := range entry.keepers {
			keepers = append(keepers, owner.FileKeepingReference)
		}
		slices.SortFunc(keepers, func(a, b FileKeepingReference) int { return compareReleaseJSON(a, b) })
		keepers = slices.Compact(keepers)
		at, marked := q.s.BundleUploads[hash]
		plan.Blobs = append(plan.Blobs, FileReleaseBlob{
			SHA256: hash, CollectionEligible: entry.collectionEligible(), References: keepers,
			BundleTracked: marked, BundleProtected: marked && entry.flags&fileExpired == 0 && at.After(q.clock.Now().Add(-q.fileRetention)),
		})
	}
	slices.SortFunc(plan.Blobs, func(a, b FileReleaseBlob) int { return compareReleaseJSON(a.SHA256, b.SHA256) })
	return plan, nil
}

func compareReleaseJSON(a, b any) int {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return slices.Compare(aa, bb)
}

type fileReleaseSnapshot struct {
	plan       FileReleasePlan
	store      *FileStore
	baseDigest string
	jobsDigest string
}

// Caller holds fileLifecycle.RLock; this prevents collection, while publications
// may proceed. Apply checks metadata again in its final transaction. A later
// HEAD/publication still receives the collector's ordinary publication grace.
func (q *Queue) fileReleasePlan(ctx context.Context, selection FileReleaseSelection) (fileReleaseSnapshot, error) {
	unlock := requestReadLock(ctx, &q.mu)
	plan, err := q.fileReleaseMetadata(selection, true)
	store, age := q.files, q.filePublicationAge(q.clock.Now())
	unlock()
	if err != nil {
		return fileReleaseSnapshot{}, err
	}
	if store == nil {
		return fileReleaseSnapshot{}, fileReleaseInvalid("queue file store is not configured")
	}
	// An unavailable directory cannot establish that selected blobs are absent.
	if st, err := os.Stat(filepath.Join(store.root, "blobs")); err != nil || !st.IsDir() {
		return fileReleaseSnapshot{}, fileReleaseInvalid("cannot read stored release content metadata")
	}
	jobs := fileReleaseDigest(plan.Jobs)
	// Preserve presence and size for every selected identity. Collection
	// fingerprints also bind publication time and bundle tracking for eligible
	// blobs. Grace and individual reference explanations are display fields.
	type storedContent struct {
		SHA256    string
		Present   bool
		SizeBytes int64
	}
	stored := make([]storedContent, len(plan.Blobs))
	for i := range plan.Blobs {
		if err := ctx.Err(); err != nil {
			return fileReleaseSnapshot{}, err
		}
		b := &plan.Blobs[i]
		st, err := os.Lstat(store.blob(FileRef{SHA256: b.SHA256}))
		if !os.IsNotExist(err) {
			if err != nil {
				return fileReleaseSnapshot{}, fileReleaseInvalid("cannot read stored release content metadata")
			}
			if !st.Mode().IsRegular() {
				return fileReleaseSnapshot{}, fileReleaseInvalid("stored release content is not a regular file")
			}
			b.Present, b.SizeBytes, b.ModifiedAt = true, st.Size(), st.ModTime()
			b.PublicationProtected = age.protects(b.ModifiedAt, !b.BundleTracked)
			b.BundleProtected = b.BundleProtected || b.BundleTracked && !b.CollectionEligible && b.ModifiedAt.After(age.now.Add(-age.retention))
			if b.CollectionEligible && !b.BundleProtected && age.payloadEligible(st, b.BundleTracked, b.CollectionEligible) {
				b.ReclaimableBytes = b.SizeBytes
				plan.ReclaimableBytes += b.SizeBytes
			}
		}
		stored[i] = storedContent{b.SHA256, b.Present, b.SizeBytes}
		if b.CollectionEligible {
			plan.CollectionCandidates = append(plan.CollectionCandidates, fileReleaseDigest(struct {
				SHA256        string
				ModifiedAt    time.Time
				BundleTracked bool
			}{b.SHA256, b.ModifiedAt, b.BundleTracked}))
		}
	}
	slices.Sort(plan.CollectionCandidates)
	base := fileReleaseDigest(struct {
		Selection FileReleaseSelection
		Jobs      string
		Stored    []storedContent
	}{selection, jobs, stored})
	plan.ID = fileReleasePlanID(base, plan.CollectionCandidates)
	plan.GeneratedAt = age.now
	return fileReleaseSnapshot{plan: plan, store: store, baseDigest: base, jobsDigest: jobs}, nil
}

func (q *Queue) PlanFileRelease(ctx context.Context, selection FileReleaseSelection) (FileReleasePlan, error) {
	selection, err := normalizeFileRelease(selection)
	if err != nil {
		return FileReleasePlan{}, err
	}
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	snapshot, err := q.fileReleasePlan(ctx, selection)
	return snapshot.plan, err
}

func (q *Queue) ApplyFileRelease(ctx context.Context, req FileReleaseApply) (FileReleaseResult, error) {
	var result FileReleaseResult
	selection, err := normalizeFileRelease(req.Selection)
	if err != nil {
		return result, err
	}
	if !req.AcceptExpiry || req.PlanID == "" {
		return result, fileReleaseInvalid("review a plan and explicitly accept expiry before applying")
	}
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	// Replay is checked before re-planning because collection may already have run.
	selectionSHA256 := fileReleaseDigest(selection)
	unlock := requestReadLock(ctx, &q.mu)
	result, err = q.fileReleaseReplay(selection, req.PlanID, selectionSHA256)
	unlock()
	if err != nil || result.PlanID != "" {
		return result, err
	}
	snapshot, err := q.fileReleasePlan(ctx, selection)
	if err != nil {
		return result, err
	}
	plan := snapshot.plan
	if plan.ID != req.PlanID {
		// The live stable fields and reviewed candidates must reproduce the
		// saved ID before accepting a strict subset of those candidates.
		stale := fileReleaseError("file_release_stale", "release plan changed; generate and review a new plan")
		if len(req.CollectionCandidates) == 0 || fileReleasePlanID(snapshot.baseDigest, req.CollectionCandidates) != req.PlanID {
			return result, stale
		}
		for _, candidate := range plan.CollectionCandidates {
			if _, found := slices.BinarySearch(req.CollectionCandidates, candidate); !found {
				return result, stale
			}
		}
	}
	for _, j := range plan.Jobs {
		if len(j.Blockers) > 0 {
			return result, fileReleaseError("file_release_protected", "selected job has live work, pending delivery or unresolved custody")
		}
	}
	err = q.updateRequest(ctx, func() error {
		receipt, replayErr := q.fileReleaseReplay(selection, req.PlanID, selectionSHA256)
		if replayErr != nil || receipt.PlanID != "" {
			result = receipt
			return replayErr
		}
		current, e := q.fileReleaseMetadata(selection, false)
		stale := fileReleaseError("file_release_stale", "release collection scope or custody changed; generate and review a new plan")
		if e != nil || q.files != snapshot.store || fileReleaseDigest(current.Jobs) != snapshot.jobsDigest {
			return stale
		}
		// Equal job manifests yield the same sorted blob identities. This
		// transaction can narrow the inspected scope while it holds q.mu.
		for i, b := range current.Blobs {
			if b.CollectionEligible && (!plan.Blobs[i].CollectionEligible || b.BundleTracked != plan.Blobs[i].BundleTracked) {
				return stale
			}
		}
		for _, id := range selection.Jobs {
			j := q.retainedJob(id)
			q.acceptFileExpiry(j, req.PlanID, selectionSHA256)
			if selection.IncludeCheckpoints {
				enrollLegacyCheckpointReclamation(j)
			}
		}
		result, replayErr = q.fileReleaseReplay(selection, req.PlanID, selectionSHA256)
		return replayErr
	}, nil)
	return result, err
}

func (a API) registerFileRelease(mux *requestMux) {
	mux.HandleFunc("GET /api/files/holdings", func(w http.ResponseWriter, r *http.Request) {
		report, err := a.Queue.ProjectFileHoldings(r.Context(), r.URL.Query().Get("project"))
		respond(w, report, err)
	})
	mux.HandleFunc("POST /api/files/release/plan", func(w http.ResponseWriter, r *http.Request) {
		var s FileReleaseSelection
		if !decode(w, r, &s) {
			return
		}
		plan, err := a.Queue.PlanFileRelease(r.Context(), s)
		respond(w, plan, err)
	})
	mux.HandleFunc("POST /api/files/release/claim", func(w http.ResponseWriter, r *http.Request) {
		var req FileInputClaimRelease
		if !decode(w, r, &req) {
			return
		}
		result, err := a.Queue.ReleaseFileInputClaim(r.Context(), req)
		respondOperation(w, result, err, "file_release_invalid")
	})
	mux.HandleFunc("POST /api/files/release/apply", func(w http.ResponseWriter, r *http.Request) {
		var req FileReleaseApply
		if !decode(w, r, &req) {
			return
		}
		result, err := a.Queue.ApplyFileRelease(r.Context(), req)
		respondOperation(w, result, err, "file_release_invalid")
	})
}
