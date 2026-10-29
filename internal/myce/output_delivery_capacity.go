package myce

import (
	"context"
	"errors"
	"maps"
	"net/http"
)

// The job record durably reserves its estimate before admission. Once its local
// immutable spool is known, reserve the actual missing identities instead. Tus
// lengths already counted by capacity() are not charged twice. Call under q.mu.
func (q *Queue) outputReservations(payloads map[string]storedPayload, exclude string) uint64 {
	var bytes uint64
	seen := map[string]bool{}
	for _, j := range q.liveJobs() {
		if j.ID == exclude || terminal(j.State) || j.Spec.OutputDelivery != asyncNAS {
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
	q.mu.RUnlock()
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
