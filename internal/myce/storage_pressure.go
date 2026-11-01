package myce

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// Legacy upload/publication pressure remains useful even while native storage
// protection is parked. These diagnostics never resize or yield commitments.
func (q *Queue) storagePressureReason(j *Job, status FileStoreStatus, need uint64) string {
	wait := ""
	if q.fileCollectionDeferred {
		wait = "; publication_fence_wait: collection deferred"
	}
	if status.Error != "" {
		return "file_store_capacity: capacity observation unavailable; retry after storage verification" + wait
	}
	var holders []string
	q.collectFileReferences(false, nil, func(_ string, ref FileKeepingReference) {
		holder := ref.Kind
		if ref.JobID != "" {
			holder += "/" + ref.JobID
		}
		holders = append(holders, holder)
	})
	for _, holder := range q.liveJobs() {
		if holder.ID != j.ID && !holder.OutputAdmissionPending && holder.Spec.OutputDelivery == asyncNAS && !terminal(holder.State) {
			holders = append(holders, "commitment/"+holder.ID)
		}
	}
	if status.UsedBytes > 0 {
		holders = append(holders, "stored payloads (including protected or unmarked imports)")
	}
	slices.Sort(holders)
	holders = slices.Compact(holders)
	destination := q.outputDeliveryHealth()
	return fmt.Sprintf("file_store_capacity: need=%d bytes free=%d bytes deficit=%d bytes stored=%d bytes holders=%s destination=%s; wait for authorized delivery/retention or add capacity%s", need, status.FreeBytes, need-min(need, status.FreeBytes), status.UsedBytes, strings.Join(holders, ","), destination, wait)
}

func (q *Queue) storageWriteDemand(ctx context.Context) *Job {
	if j, err := q.fileStoreAttemptJob(ctx, true); err == nil && j != nil {
		return j
	}
	if hint, ok := ctx.Value(fileStoreSubmissionKey{}).(Spec); ok {
		class := q.s.Policies[hint.Project].DefaultCallPriority
		if class == "" {
			class = "experiment"
		}
		return &Job{Spec: Spec{Project: hint.Project, Class: class}}
	}
	return nil
}

func (q *Queue) recordStorageWritePressure(ctx context.Context, status FileStoreStatus, projected map[string]storedPayload, files FileManifest, growth uint64, bad bool, exclude string) {
	q.mu.RLock()
	demand := q.storageWriteDemand(ctx)
	production, identified, err := q.fileStoreWriteClass(ctx, files, growth)
	key := ""
	if demand != nil {
		key = "storage-write/project/" + identityHash(demand.Spec.Project)
		if demand.ID != "" {
			key = "storage-write/job/" + demand.ID
		}
	}
	active := q.s.Health.Conditions[key].Active
	q.mu.RUnlock()
	if demand == nil || err != nil || !production || !identified || !bad && !active {
		return
	}
	_ = q.updateRequest(ctx, func() error {
		demand := q.storageWriteDemand(ctx)
		production, identified, err := q.fileStoreWriteClass(ctx, files, growth)
		if demand == nil || err != nil || !production || !identified {
			return nil
		}
		need := addDiskBytes(growth, q.outputReservations(projected, exclude))
		reason := q.storagePressureReason(demand, status, need)
		q.observeHealth(key, bad, 0, HealthAlert{Kind: "production_storage_capacity", Store: "queue-file-store", JobID: demand.ID, Attempt: demand.Attempt, Project: demand.Spec.Project, Reason: reason, Recipients: []string{"operator", "project"}})
		return nil
	}, nil)
}

func (q *Queue) outputDeliveryHealth() string {
	var observations []string
	visit := func(j *Job) {
		if j == nil || j.Delivery == nil {
			return
		}
		d := j.Delivery
		if j.Spec.OutputDestination != nil && (j.LocalOutputs == nil || !sameDelivery(j, *j)) {
			return // A superseded local receipt cannot describe this destination.
		}
		state, cause := "unknown", ""
		switch d.State {
		case "failed":
			state = "failed"
		case "publishing":
			state = "publishing"
			if d.Cause != "" {
				cause = "publication_stalled"
			}
		case "pending":
			state = "pending"
		case "verified":
			state = "verified"
		}
		switch d.Cause {
		case "NAS publication or receipt verification failed; source files retained":
			cause = "publication_or_receipt_verification_failed"
		case "destination publication or verification failed; worker custody retained":
			cause = "publication_or_receipt_verification_failed"
		case "destination preservation verification failed; worker custody retained":
			cause = "preservation_verification_failed"
		case "NAS destination configuration missing or changed; restore the original output-delivery-root":
			cause = "destination_configuration_changed"
		}
		observation := state + "/" + j.ID
		if d.DestinationID != "" {
			observation += "@" + d.DestinationID
		}
		if cause != "" {
			observation += ":" + cause
		}
		observations = append(observations, observation)
	}
	for _, j := range q.s.Jobs {
		visit(j)
	}
	for _, r := range q.s.Submissions {
		if q.s.Jobs[r.JobID] == nil {
			visit(r.Job)
		}
	}
	if len(observations) == 0 {
		return "unknown"
	}
	slices.Sort(observations)
	return strings.Join(slices.Compact(observations), ",")
}
