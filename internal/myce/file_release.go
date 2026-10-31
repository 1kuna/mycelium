package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"slices"
	"time"
)

// Project is attribution under the shared fleet credential, not authorization.
// Explicit job IDs define the owner's completion boundary (including any parents,
// children and downstream consumers). There is deliberately no project-wide glob.
type FileReleaseSelection struct {
	Project string   `json:"project"`
	Jobs    []string `json:"jobs"`
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
	ID                 string            `json:"id"`
	Project            string            `json:"project"`
	State              string            `json:"state"`
	Release            string            `json:"release"`
	CustodySHA256      string            `json:"custody_sha256"`
	FilesExpired       bool              `json:"files_expired"`
	ExpiresAt          *time.Time        `json:"expires_at,omitempty"`
	Files              []FileReleaseFile `json:"files"`
	Blockers           []string          `json:"blockers"`
	DeliveryState      string            `json:"delivery_state,omitempty"`
	DeliveryManifest   string            `json:"delivery_manifest,omitempty"`
	DeliveryVerifiedAt *time.Time        `json:"delivery_verified_at,omitempty"`
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
}

// Acceptance receipts use the existing retained job ledger, including keyed
// answers. Overlapping selections cannot overwrite a previous apply receipt.
const fileReleaseLedgerKind = "file_expiry_accepted"

// Both explicit owner release and authored delivery retention enroll the same
// durable collector transaction. Neither path deletes bytes or bypasses pins.
func (q *Queue) acceptFileExpiry(j *Job, reason string) {
	e := ledgerAttempt(j, fileReleaseLedgerKind, reason)
	e.Release = q.observedRelease(j)
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
func (q *Queue) fileReleaseReplay(selection FileReleaseSelection, planID string) (FileReleaseResult, bool) {
	var result FileReleaseResult
	for _, id := range selection.Jobs {
		j := q.retainedJob(id)
		if j == nil || j.Spec.Project != selection.Project {
			return result, false
		}
		e := fileReleaseEntry(j, planID)
		if e == nil {
			return result, false
		}
		if result.PlanID == "" {
			result = FileReleaseResult{PlanID: planID, AcceptedAt: e.At, Collection: "scheduled"}
		}
	}
	return result, true
}

type FileReleaseApply struct {
	Selection    FileReleaseSelection `json:"selection"`
	PlanID       string               `json:"plan_id"`
	AcceptExpiry bool                 `json:"accept_expiry"`
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

func normalizeFileRelease(s FileReleaseSelection) (FileReleaseSelection, error) {
	s.Jobs = slices.Clone(s.Jobs)
	slices.Sort(s.Jobs)
	s.Jobs = slices.Compact(s.Jobs)
	if s.Project == "" || len(s.Jobs) == 0 || slices.Contains(s.Jobs, "") {
		return s, errors.New("release requires a project and explicit job IDs")
	}
	return s, nil
}

func fileReleaseDigest(v any) string {
	data, _ := json.Marshal(v)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// q.mu held. Only metadata work here; no blob reads, stats or NAS access.
func (q *Queue) fileReleaseMetadata(selection FileReleaseSelection) (FileReleasePlan, error) {
	plan := FileReleasePlan{Selection: selection, Jobs: []FileReleaseJob{}, Blobs: []FileReleaseBlob{}}
	releasing := map[string]bool{}
	hashes := map[string]bool{}
	for _, id := range selection.Jobs {
		j := q.retainedJob(id)
		if j == nil || j.Spec.Project != selection.Project {
			return plan, errors.New("selected job is unavailable or belongs to another project")
		}
		item := FileReleaseJob{ID: j.ID, Project: j.Spec.Project, State: j.State, Release: q.observedRelease(j), FilesExpired: j.FilesExpired, Files: []FileReleaseFile{}, Blockers: []string{}}
		item.CustodySHA256 = fileReleaseDigest(struct {
			Attempt          int
			Machine, Session string
			Confirmed        bool
			Copies           []Copy
		}{j.Attempt, j.Machine, j.Session, j.ReleaseConfirmed, j.Copies})
		if j.FinishedAt != nil {
			at := j.FinishedAt.Add(q.fileRetention)
			item.ExpiresAt = &at
		}
		if !terminal(j.State) {
			item.Blockers = append(item.Blockers, "live_job")
		}
		if q.jobReleasePending(j) {
			item.Blockers = append(item.Blockers, "physical_custody")
		}
		if deliveryPending(j) {
			item.Blockers = append(item.Blockers, "pending_delivery")
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
		slices.SortFunc(item.Files, func(a, b FileReleaseFile) int { return compareReleaseJSON(a, b) })
		plan.Jobs = append(plan.Jobs, item)
		releasing[id] = len(item.Blockers) == 0
	}
	refs := map[string][]FileKeepingReference{}
	pending, keep, _ := q.collectFileReferences(true, releasing, func(hash string, ref FileKeepingReference) {
		if hashes[hash] {
			refs[hash] = append(refs[hash], ref)
		}
	})
	for hash := range hashes {
		if (FileRef{SHA256: hash}).validate() != nil {
			return plan, errors.New("invalid retained content identity")
		}
		r := refs[hash]
		slices.SortFunc(r, func(a, b FileKeepingReference) int { return compareReleaseJSON(a, b) })
		r = slices.Compact(r) // Job and keyed answer may alias the same reference.
		at, marked := q.s.BundleUploads[hash]
		plan.Blobs = append(plan.Blobs, FileReleaseBlob{SHA256: hash, CollectionEligible: pending[hash] && !keep[hash], References: r, BundleTracked: marked, BundleProtected: marked && at.After(q.clock.Now().Add(-q.fileRetention))})
	}
	slices.SortFunc(plan.Blobs, func(a, b FileReleaseBlob) int { return compareReleaseJSON(a.SHA256, b.SHA256) })
	return plan, nil
}

func compareReleaseJSON(a, b any) int {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return slices.Compare(aa, bb)
}

// Caller holds fileLifecycle.RLock; this prevents collection, while publications
// may proceed. Apply checks metadata again in its final transaction. A later
// HEAD/publication still receives the collector's ordinary publication grace.
func (q *Queue) fileReleasePlan(ctx context.Context, selection FileReleaseSelection) (FileReleasePlan, string, *FileStore, error) {
	unlock := requestReadLock(ctx, &q.mu)
	plan, err := q.fileReleaseMetadata(selection)
	store, now, retention := q.files, q.clock.Now(), q.fileRetention
	unlock()
	metadata := fileReleaseDigest(plan)
	if err != nil {
		return plan, metadata, store, err
	}
	if store == nil {
		return plan, metadata, store, errors.New("queue file store is not configured")
	}
	for i := range plan.Blobs {
		if err := ctx.Err(); err != nil {
			return plan, metadata, store, err
		}
		b := &plan.Blobs[i]
		st, err := os.Lstat(store.blob(FileRef{SHA256: b.SHA256}))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || !st.Mode().IsRegular() {
			return plan, metadata, store, errors.New("cannot inspect release content")
		}
		b.Present, b.SizeBytes, b.ModifiedAt = true, st.Size(), st.ModTime()
		publicationCutoff, orphanCutoff := now.Add(-filePublicationGrace), now.Add(-retention)
		b.PublicationProtected = st.ModTime().After(publicationCutoff)
		b.BundleProtected = b.BundleProtected || b.BundleTracked && st.ModTime().After(orphanCutoff)
		if b.CollectionEligible && !b.BundleProtected && filePayloadAgeEligible(st, publicationCutoff, orphanCutoff, b.BundleTracked) {
			b.ReclaimableBytes = b.SizeBytes
			plan.ReclaimableBytes += b.SizeBytes
		}
	}
	plan.ID = fileReleaseDigest(plan)
	plan.GeneratedAt = now
	return plan, metadata, store, nil
}

func (q *Queue) PlanFileRelease(ctx context.Context, selection FileReleaseSelection) (FileReleasePlan, error) {
	selection, err := normalizeFileRelease(selection)
	if err != nil {
		return FileReleasePlan{}, err
	}
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	plan, _, _, err := q.fileReleasePlan(ctx, selection)
	return plan, err
}

func (q *Queue) ApplyFileRelease(ctx context.Context, req FileReleaseApply) (FileReleaseResult, error) {
	var result FileReleaseResult
	selection, err := normalizeFileRelease(req.Selection)
	if err != nil {
		return result, err
	}
	if !req.AcceptExpiry || req.PlanID == "" {
		return result, errors.New("review a plan and explicitly accept expiry before applying")
	}
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	// Replay is checked before re-planning because collection may already have run.
	unlock := requestReadLock(ctx, &q.mu)
	result, replay := q.fileReleaseReplay(selection, req.PlanID)
	unlock()
	if replay {
		return result, nil
	}
	plan, metadata, store, err := q.fileReleasePlan(ctx, selection)
	if err != nil {
		return result, err
	}
	if plan.ID != req.PlanID {
		return result, fileReleaseError("file_release_stale", "release plan changed; generate and review a new plan")
	}
	for _, j := range plan.Jobs {
		if len(j.Blockers) > 0 {
			return result, fileReleaseError("file_release_protected", "selected job has live work, pending delivery or unresolved custody")
		}
	}
	err = q.updateRequest(ctx, func() error {
		if receipt, replay := q.fileReleaseReplay(selection, req.PlanID); replay {
			result = receipt
			return nil
		}
		current, e := q.fileReleaseMetadata(selection)
		if e != nil || q.files != store || fileReleaseDigest(current) != metadata {
			return fileReleaseError("file_release_stale", "release references or custody changed; generate a new plan")
		}
		for _, id := range selection.Jobs {
			j := q.retainedJob(id)
			q.acceptFileExpiry(j, req.PlanID)
		}
		result, _ = q.fileReleaseReplay(selection, req.PlanID)
		return nil
	}, nil)
	return result, err
}

func (a API) registerFileRelease(mux *requestMux) {
	mux.HandleFunc("POST /api/files/release/plan", func(w http.ResponseWriter, r *http.Request) {
		var s FileReleaseSelection
		if !decode(w, r, &s) {
			return
		}
		plan, err := a.Queue.PlanFileRelease(r.Context(), s)
		respondOperation(w, plan, err, "file_release_invalid")
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
