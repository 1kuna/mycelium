package myce

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"
)

// The job record durably reserves its estimate before admission. Once its local
// immutable spool is known, reserve the actual missing identities instead. Tus
// lengths already counted by capacity() are not charged twice. Call under q.mu.
func (q *Queue) outputReservations(payloads map[string]storedPayload, exclude string) uint64 {
	var bytes uint64
	seen := map[string]bool{}
	for _, j := range q.liveJobs() {
		if j.OutputAdmissionPending || j.ID == exclude || terminal(j.State) || j.Spec.OutputDelivery != asyncNAS {
			continue
		}
		if j.PendingOutputRefs == nil {
			bytes = addDiskBytes(bytes, j.Spec.Disk.OutputBytes)
			continue
		}
		for _, ref := range j.PendingOutputRefs {
			if seen[ref.SHA256] {
				continue
			}
			seen[ref.SHA256] = true
			if p, ok := payloads[ref.SHA256]; !ok || p.length != uint64(ref.SizeBytes) {
				bytes = addDiskBytes(bytes, uint64(ref.SizeBytes))
			}
		}
	}
	return bytes
}

func (q *Queue) reservedOutputBytes(payloads map[string]storedPayload) uint64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.outputReservations(payloads, "")
}

// Reservation is attempt-specific and replayable. No blob is accepted here;
// CompleteContext still hashes and syncs all bytes before success.
func (q *Queue) reserveDeliveryResult(ctx context.Context, r Result) error {
	if err := r.OutputRefs.validate(); err != nil {
		return err
	}
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	q.mu.RLock()
	store := q.files
	// Lost completion acknowledgements replay acceptance rather than treating
	// the already accepted current attempt as a stale reservation.
	accepted := q.s.Jobs[r.ID]
	replayed := accepted != nil && accepted.State == "succeeded" && accepted.ID == r.ID && accepted.Attempt == r.Attempt && accepted.Machine == r.Machine && accepted.Session == r.Session && accepted.Spec.OutputDelivery == asyncNAS && maps.Equal(accepted.OutputRefs, r.OutputRefs)
	q.mu.RUnlock()
	if replayed {
		return nil
	}
	if store == nil {
		return errors.New("file store unavailable")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	status, payloads, err := store.capacity(ctx)
	if err != nil {
		status.Error = "file store capacity unavailable"
	}
	return q.updateRequest(ctx, func() error {
		j, err := q.current(r)
		m := q.s.Machines[r.Machine]
		if err != nil || j.State != "running" || cancelled(j) || j.Spec.OutputDelivery != asyncNAS || m == nil || m.Session != r.Session || q.files != store {
			return ErrStale
		}
		if len(r.OutputRefs) != len(j.Spec.Outputs) {
			return errors.New("complete output manifest required")
		}
		for name := range j.Spec.Outputs {
			if _, ok := r.OutputRefs[name]; !ok {
				return errors.New("missing output")
			}
		}
		if j.PendingOutputRefs != nil {
			if !maps.Equal(j.PendingOutputRefs, r.OutputRefs) {
				return errors.New("conflicting result reservation")
			}
			return nil
		}
		var need uint64
		projected := maps.Clone(payloads)
		if projected == nil {
			projected = map[string]storedPayload{}
		}
		seen := map[string]bool{}
		for _, ref := range r.OutputRefs {
			if seen[ref.SHA256] {
				continue
			}
			seen[ref.SHA256] = true
			if p, ok := payloads[ref.SHA256]; !ok || p.length != uint64(ref.SizeBytes) {
				need = addDiskBytes(need, uint64(ref.SizeBytes))
			}
			projected[ref.SHA256] = storedPayload{length: uint64(ref.SizeBytes)}
		}
		need = addDiskBytes(need, q.outputReservations(projected, j.ID))
		if err := fileStoreRefusal(status, need); err != nil {
			return err
		}
		j = q.touchJob(j.ID)
		for _, ref := range r.OutputRefs {
			if at, owned := q.s.JobInputUploads[ref.SHA256]; owned {
				if q.s.AttemptOutputUploads == nil {
					q.s.AttemptOutputUploads = map[string]time.Time{}
				}
				q.s.AttemptOutputUploads[ref.SHA256] = at
				delete(q.s.JobInputUploads, ref.SHA256)
			}
		}
		j.PendingOutputRefs = maps.Clone(r.OutputRefs)
		return nil
	}, nil)
}

func (a API) deliveryResultHandler(w http.ResponseWriter, r *http.Request) {
	var result Result
	if !decode(w, r, &result) {
		return
	}
	respondOperation(w, map[string]bool{"ok": true}, a.Queue.reserveDeliveryResult(r.Context(), result), "result_reservation_invalid")
}

const outputRetryHold = "file_store_capacity: waiting to reacquire output reservation after retry"

// Requeue never conjures a fresh estimate. Tick reacquires the retry's budget
// through the same serialized file-store admission used for new tasks/uploads.
func (q *Queue) admitOutputRetries(ctx context.Context) error {
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	q.mu.RLock()
	store := q.files
	pending := false
	for _, j := range q.liveJobs() {
		if j.State == "queued" && j.OutputAdmissionPending {
			pending = true
			break
		}
	}
	q.mu.RUnlock()
	if !pending || store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	status, payloads, err := store.capacity(ctx)
	if err != nil {
		status.Error = "file store capacity unavailable"
	}
	return q.updateRequest(ctx, func() error {
		if store != q.files {
			return ErrStale
		}
		jobs := []*Job{}
		for _, j := range q.liveJobs() {
			if j.State == "queued" && j.OutputAdmissionPending {
				jobs = append(jobs, j)
			}
		}
		slices.SortFunc(jobs, func(a, b *Job) int {
			if c := a.Queued.Compare(b.Queued); c != 0 {
				return c
			}
			return strings.Compare(a.ID, b.ID)
		})
		for _, j := range jobs {
			need := addDiskBytes(q.outputReservations(payloads, ""), j.Spec.Disk.OutputBytes)
			if fileStoreRefusal(status, need) != nil {
				continue
			}
			j = q.touchJob(j.ID)
			j.OutputAdmissionPending = false
		}
		return nil
	}, nil)
}

// Positive ownership is recorded only on creation, or transferred from an
// already eligible input upload. Existing unmarked imports are never adopted.
// Unlike input markers, this marker survives the provisional manifest reference:
// it remains the cleanup owner if a failed/lost attempt discards that manifest.
func (q *Queue) recordAttemptOutputUpload(hash string, created bool, ctx context.Context) error {
	q.mu.RLock()
	_, input := q.s.JobInputUploads[hash]
	_, output := q.s.AttemptOutputUploads[hash]
	q.mu.RUnlock()
	if output || !created && !input {
		return nil
	}
	return q.updateRequest(ctx, func() error {
		_, input := q.s.JobInputUploads[hash]
		_, output := q.s.AttemptOutputUploads[hash]
		if !output && (created || input) {
			if q.s.AttemptOutputUploads == nil {
				q.s.AttemptOutputUploads = map[string]time.Time{}
			}
			q.s.AttemptOutputUploads[hash] = q.s.JobInputUploads[hash]
			delete(q.s.JobInputUploads, hash)
		}
		return nil
	}, nil)
}

func (q *Queue) jobUploadMarker(hash string) (time.Time, bool) {
	if at, ok := q.s.AttemptOutputUploads[hash]; ok {
		return at, true
	}
	at, ok := q.s.JobInputUploads[hash]
	return at, ok
}
func (q *Queue) jobUploadMarkers() map[string]time.Time {
	markers := maps.Clone(q.s.JobInputUploads)
	if markers == nil {
		markers = map[string]time.Time{}
	}
	maps.Copy(markers, q.s.AttemptOutputUploads)
	return markers
}
