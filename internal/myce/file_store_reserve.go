package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type fileStoreSubmissionKey struct{}
type fileStoreAttemptKey struct{}
type fileStoreOrdinaryKey struct{}
type fileStoreAttempt struct {
	Recovery bool `json:"recovery,omitempty"`
	Execution
	Machine string `json:"machine"`
	Session string `json:"session"`
}

// Input hints may narrow the queue's project policy; they never promote it.
func WithFileStoreSubmission(ctx context.Context, s Spec) context.Context {
	return context.WithValue(ctx, fileStoreSubmissionKey{}, Spec{Project: s.Project, Class: jobClass(s)})
}
func withFileStoreAttempt(ctx context.Context, j Job) context.Context {
	return context.WithValue(ctx, fileStoreAttemptKey{}, fileStoreAttempt{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session})
}

// WithFileStoreRecovery fences publication to the captured failed execution.
func WithFileStoreRecovery(ctx context.Context, p RecoveryPlan) context.Context {
	return context.WithValue(ctx, fileStoreAttemptKey{}, fileStoreAttempt{Execution: p.Execution, Machine: p.Machine, Session: p.Session, Recovery: true})
}
func fileStoreSubmissionHeaders(ctx context.Context, r *http.Request) {
	if id, ok := ctx.Value(fileInputClaimKey{}).(string); ok {
		r.Header.Set("X-Myce-Input-Claim", id)
	}
	if s, ok := ctx.Value(fileStoreSubmissionKey{}).(Spec); ok {
		r.Header.Set("X-Myce-Project", s.Project)
		r.Header.Set("X-Myce-Class", s.Class)
	}
	if attempt, ok := ctx.Value(fileStoreAttemptKey{}).(fileStoreAttempt); ok {
		b, _ := json.Marshal(attempt)
		r.Header.Set("X-Myce-File-Attempt", string(b))
	}
}
func fileStoreSubmissionRequest(r *http.Request) (*http.Request, error) {
	ctx := r.Context()
	if id := r.Header.Get("X-Myce-Input-Claim"); id != "" {
		ctx = context.WithValue(ctx, fileInputClaimKey{}, id)
	}
	if raw := r.Header.Get("X-Myce-File-Attempt"); raw != "" {
		var attempt fileStoreAttempt
		if err := json.Unmarshal([]byte(raw), &attempt); err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, fileStoreAttemptKey{}, attempt)
	}
	if r.Header.Get("X-Myce-Project") != "" || r.Header.Get("X-Myce-Class") != "" {
		s := Spec{Project: r.Header.Get("X-Myce-Project"), Class: r.Header.Get("X-Myce-Class")}
		validated := s
		if err := normalizeJobClass(&validated); err != nil {
			return nil, err
		}
		// Preserve an absent class: it does not narrow the project's default.
		ctx = context.WithValue(ctx, fileStoreSubmissionKey{}, s)
	}
	return r.WithContext(ctx), nil
}
func WithFileStoreProductionReserve(bytes int64) QueueOption {
	return func(q *Queue) { q.fileStoreProductionReserve = bytes }
}

// Call under q.mu. Accepted attempts may reuse bytes after a lost completion
// acknowledgement, but cannot allocate new bytes. Old/replaced attempts fail.
func (q *Queue) fileStoreAttemptJob(ctx context.Context, allocating bool) (*Job, error) {
	r, ok := ctx.Value(fileStoreAttemptKey{}).(fileStoreAttempt)
	if !ok {
		return nil, nil
	}
	j := q.s.Jobs[r.ID]
	if r.Recovery {
		err := recoveryFence(j, RecoveryRequest{Execution: r.Execution, Machine: r.Machine, Session: r.Session})
		return j, err
	}
	if j == nil || j.Attempt != r.Attempt || j.Machine != r.Machine || j.Session != r.Session || cancelled(j) {
		return nil, ErrStale
	}
	if j.State == "succeeded" && !allocating {
		return j, nil
	}
	m := q.s.Machines[r.Machine]
	if j.State != "running" && j.State != "stopping" || m == nil || m.Session != r.Session {
		return nil, ErrStale
	}
	return j, nil
}
func (q *Queue) checkFileStoreAttempt(ctx context.Context) error {
	controlMu := q.mu.named("checkFileStoreAttempt", ctx)
	if ctx.Value(fileStoreAttemptKey{}) == nil {
		return nil
	}
	if err := lockUploadContext(ctx, controlMu.RLock, controlMu.RUnlock); err != nil {
		return err
	}
	defer controlMu.RUnlock()
	_, err := q.fileStoreAttemptJob(ctx, false)
	return err
}
func (q *Queue) nonProductionFileStoreFree(status FileStoreStatus, productionReserved uint64) uint64 {
	reserve := uint64(q.fileStoreProductionReserve)
	uncommitted := reserve - min(reserve, productionReserved)
	return status.FreeBytes - min(status.FreeBytes, uncommitted)
}

// Production output commitments back the reserve; only its uncommitted
// remainder is withheld. Disk inventory always precedes this metadata check.
func (q *Queue) fileStoreReserveRefusal(status FileStoreStatus, need, growth, productionReserved uint64, production bool) error {
	if err := fileStoreRefusal(status, need); err != nil {
		return err
	}
	if growth == 0 || production {
		return nil
	}
	available := q.nonProductionFileStoreFree(status, productionReserved)
	if need > available {
		return &HTTPError{Status: http.StatusInsufficientStorage, Code: "file_store_capacity", Message: fmt.Sprintf("queue-file-store: need=%d bytes non_production_free=%d bytes; production reserve=%d bytes (--file-store-production-reserve); production and unidentified legacy writes may use the reserve", need, available, q.fileStoreProductionReserve)}
	}
	return nil
}

// Caller holds q.mu. The current job controls worker classification. Input
// hints cannot raise a project above its server-side default/tier. Transfers
// with no identity remain admissible for deployed clients and old workers.
func (q *Queue) fileStoreWriteClass(ctx context.Context, files FileManifest, growth uint64) (production, identified bool, err error) {
	if j, err := q.fileStoreAttemptJob(ctx, growth > 0); err != nil {
		return false, true, err
	} else if j != nil {
		return ctx.Value(fileStoreAttemptKey{}).(fileStoreAttempt).Recovery || q.priorityMatch(j.Spec).Tier == "production", true, nil
	}
	if ctx.Value(fileStoreOrdinaryKey{}) != nil {
		return false, true, nil
	}
	if s, ok := ctx.Value(fileStoreSubmissionKey{}).(Spec); ok {
		class := q.s.Policies[s.Project].DefaultCallPriority
		if class == "" {
			class = "experiment"
		}
		production = q.priorityMatch(Spec{Project: s.Project, Class: class}).Tier == "production"
		if s.Class != "" && (s.Class == "experiment" || q.priorityMatch(s).Tier != "production") {
			production = false
		}
		return production, true, nil
	}
	// Old async workers declare their exact identities before uploading them.
	// Known experiment identities cannot fall through to the legacy exception.
	allKnown := len(files) > 0
	for _, ref := range files {
		known, prod := false, false
		for _, j := range q.liveJobs() {
			if terminal(j.State) || cancelled(j) || j.OutputAdmissionPending {
				continue
			}
			for _, pending := range j.PendingOutputRefs {
				if pending == ref {
					known = true
					prod = prod || q.priorityMatch(j.Spec).Tier == "production"
				}
			}
		}
		if known && !prod {
			return false, true, nil
		}
		allKnown = allKnown && known
	}
	return true, allKnown, nil
}

// Return only the unidentified portion admitted across the production floor.
// Preflight observes it; successful tus creation counts it once, not per PATCH.
// The reserve is a capacity guard against accidental fill by fleet workers
// sharing one credential, not an authorization boundary. Upload requests have
// no independently authenticated worker identity: copying a production attempt
// can claim its tier, and unidentified legacy writes can also use the reserve.
// Attempt checks fence stale executions; they do not bind bytes to their owner.
func (q *Queue) admitFileUpload(ctx context.Context, status FileStoreStatus, projected map[string]storedPayload, files FileManifest, need, growth uint64) (uint64, error) {
	controlMu := q.mu.named("admitFileUpload", ctx)
	if err := lockUploadContext(ctx, controlMu.RLock, controlMu.RUnlock); err != nil {
		return 0, err
	}
	ordinary, err := q.fileUploadAdmission(ctx, status, projected, files, need, growth)
	controlMu.RUnlock()
	if err == nil {
		q.recordStorageWritePressure(ctx, status, projected, files, growth, false, "")
	}
	return ordinary, err
}

// Caller holds q.mu; all work here is queue metadata and in-memory accounting.
func (q *Queue) fileUploadAdmission(ctx context.Context, status FileStoreStatus, projected map[string]storedPayload, files FileManifest, need, growth uint64) (uint64, error) {
	production, identified, classErr := q.fileStoreWriteClass(ctx, files, growth)
	reserved := q.outputReservationsFor(projected, "", true)
	refusal := q.fileStoreAdmissionRefusal(ctx, status, projected, need, growth, reserved, production)
	// Input uploads naming a project are checked against its file-store quota
	// at preflight and tus creation; worker results and bundles never are.
	// Use the class submit will see, unlike the reserve, so a production
	// submission is never refused before it can be submitted.
	if s, ok := ctx.Value(fileStoreSubmissionKey{}).(Spec); ok && refusal == nil && classErr == nil && ctx.Value(fileStoreAttemptKey{}) == nil && ctx.Value(fileStoreOrdinaryKey{}) == nil {
		if s.Class == "" {
			s.Class = q.s.Policies[s.Project].DefaultCallPriority
		}
		s.Class = jobClass(s)
		refusal = q.fileStoreQuotaRefusal(s, files, 0)
	}
	var ordinary uint64
	if !identified {
		available := q.nonProductionFileStoreFree(status, reserved)
		ordinary = min(growth, need-min(need, available))
	}
	if classErr != nil {
		return 0, classErr
	}
	return ordinary, refusal
}

func (q *Queue) admitFileCreation(ctx context.Context, view *fileCapacitySnapshot, ref FileRef) (uint64, error) {
	controlMu := q.mu.named("admitFileCreation", ctx)
	if err := lockUploadContext(ctx, controlMu.RLock, controlMu.RUnlock); err != nil {
		return 0, err
	}
	s := view.store
	s.mu.Lock()
	status, payloads := view.current()
	growth := uint64(ref.SizeBytes) - min(uint64(ref.SizeBytes), payloads[ref.SHA256].length)
	payloads[ref.SHA256] = storedPayload{length: uint64(ref.SizeBytes)}
	need := addDiskBytes(growth, q.outputReservations(payloads, ""))
	files := FileManifest{"upload": ref}
	ordinary, err := q.fileUploadAdmission(ctx, status, payloads, files, need, growth)
	if err == nil {
		s.creations[ref.SHA256].length = uint64(ref.SizeBytes)
	}
	s.mu.Unlock()
	controlMu.RUnlock()
	if err == nil {
		q.recordStorageWritePressure(ctx, status, payloads, files, growth, false, "")
	}
	return ordinary, err
}

// Use the same combined reserve pools for upload, submit and retry admission.
// The inventory is detached; caller holds q.mu only for metadata checks.
func (q *Queue) fileStoreAdmissionRefusal(ctx context.Context, status FileStoreStatus, payloads map[string]storedPayload, need, growth, productionReserved uint64, production bool) error {
	if err := q.fileStoreReserveRefusal(status, need, growth, productionReserved, production); err != nil {
		return err
	}
	if !production {
		status.FreeBytes = q.nonProductionFileStoreFree(status, productionReserved)
	}
	return q.projectFileReservationRefusal(ctx, status, payloads, need, growth)
}
