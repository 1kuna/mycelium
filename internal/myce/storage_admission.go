package myce

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// The job record durably reserves its estimate before admission. Once its local
// immutable spool is known, reserve the actual missing identities instead. Tus
// lengths already counted by capacity() are not charged twice. Call under q.mu.
func (q *Queue) outputReservations(payloads map[string]storedPayload, exclude string) uint64 {
	return q.outputReservationsFor(payloads, exclude, false)
}
func (q *Queue) outputReservationsFor(payloads map[string]storedPayload, exclude string, productionOnly bool) uint64 {
	var bytes uint64
	seen := map[string]bool{}
	for _, j := range q.liveJobs() {
		if productionOnly && q.priorityMatch(j.Spec).Tier != "production" {
			continue
		}
		if j.OutputAdmissionPending || j.ID == exclude || terminal(j.State) || j.Spec.OutputDelivery != asyncNAS || q.workerLocalOutput(j) {
			continue
		}
		if j.PendingOutputRefs == nil {
			commitment := j.Spec.Disk.OutputBytes
			if q.s.StorageAdmissionActive {
				commitment = outputCommitment(j)
			}
			bytes = addDiskBytes(bytes, commitment)
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

// Worker custody never borrows shared output capacity, including while NAS is
// healthy. The launch/attempt fences still require qualified independent backing.
func (q *Queue) workerLocalOutput(j *Job) bool {
	return j.Spec.StorageProtocol == workerCustodyProtocol && q.priorityMatch(j.Spec).Tier != "production"
}

func outputCommitment(j *Job) uint64 {
	if j.OutputReservationBytes != nil {
		return *j.OutputReservationBytes
	}
	if j.Spec.Disk != nil {
		return j.Spec.Disk.OutputBytes
	}
	return 0
}

// Freeze learned logical demand when acquiring a shared commitment. Consider
// every compatible qualified variant, without naming a host. The largest need
// funds any later placement; unknown variants retain the cold prior. Installed
// worker limits and observed physical allocations are separate ledgers.
func (q *Queue) sharedOutputDemand(j *Job) uint64 {
	if q.workerLocalOutput(j) {
		return 0
	}
	var need uint64
	found := false
	for _, m := range q.s.Machines {
		if !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		for _, target := range q.machineTargets(m) {
			for _, candidate := range extensionJobs(j, target, true) {
				if !executionTargetMatches(candidate.Spec, target) || !deviceAllowed(candidate, target) || !candidate.Spec.Needs.cpuOnly() && (target.GPU == "cpu" || len(candidate.Spec.Needs.Platform.Required) > 0 && !slices.Contains(candidate.Spec.Needs.Platform.Required, target.GPU)) {
					continue
				}
				envelope := q.storageEnvelope(candidate, target)
				if envelope.OutputBytes != nil {
					need = max(need, *envelope.OutputBytes)
					found = true
				}
			}
		}
	}
	if !found && j.Spec.Disk != nil {
		need = j.Spec.Disk.OutputBytes
	}
	return need
}

func (q *Queue) storageQueueOrder(jobs []*Job) {
	slices.SortFunc(jobs, func(a, b *Job) int {
		if n := q.comparePriority(a.Spec, b.Spec); n != 0 {
			return n
		}
		if n := a.Queued.Compare(b.Queued); n != 0 {
			return n
		}
		return strings.Compare(a.ID, b.ID)
	})
}

// Only unassigned estimates can yield synchronously. Preparation/assigned
// attempts may already have writers even before Started; neither stopping nor
// requeueing them authorizes releasing a native boundary or retained bytes.
// Caller holds the serialized store admission and a queue update transaction.
func (q *Queue) yieldSharedOutput(demand *Job, status FileStoreStatus, payloads map[string]storedPayload, growth uint64, exclude string) error {
	check := func() error {
		need := addDiskBytes(growth, q.outputReservations(payloads, exclude))
		return q.fileStoreReserveRefusal(status, need, growth, q.outputReservationsFor(payloads, exclude, true), q.priorityMatch(demand.Spec).Tier == "production")
	}
	err := check()
	if err == nil || status.Error != "" || !q.s.StorageAdmissionActive {
		return err
	}
	var victims []*Job
	for _, j := range q.liveJobs() {
		if j.ID != demand.ID && j.State == "queued" && !cancelled(j) && !j.OutputAdmissionPending && j.Spec.OutputDelivery == asyncNAS && j.PendingOutputRefs == nil && j.StorageAllocation == nil && !q.workerLocalOutput(j) && q.comparePriority(j.Spec, demand.Spec) > 0 && outputCommitment(j) > 0 {
			victims = append(victims, j)
		}
	}
	q.storageQueueOrder(victims)
	// Lowest owner tier and newest unstarted work yield first. No bytes are
	// deleted and every victim must reacquire before dispatch.
	for i := len(victims) - 1; i >= 0; i-- {
		victim := q.touchJob(victims[i].ID)
		victim.OutputAdmissionPending = true
		victim.Reason = "file_store_capacity: unstarted output commitment yielded to higher tier job " + demand.ID
		if err = check(); err == nil {
			return nil
		}
	}
	return err
}

const outputRetryHold = "file_store_capacity: waiting to reacquire output reservation after retry"

func outputAdmissionReason(j *Job) string {
	if strings.HasPrefix(j.Reason, "file_store_capacity:") {
		return j.Reason
	}
	return outputRetryHold
}

func (q *Queue) admitSharedOutput(j *Job, status FileStoreStatus, payloads map[string]storedPayload) error {
	if !q.s.StorageAdmissionActive {
		return q.yieldSharedOutput(j, status, payloads, j.Spec.Disk.OutputBytes, j.ID)
	}
	if q.workerLocalOutput(j) {
		j.OutputReservationBytes = storageValue(0)
		j.OutputAdmissionPending = false
		return nil
	}
	for _, higher := range q.liveJobs() {
		if higher.ID != j.ID && higher.State == "queued" && higher.OutputAdmissionPending && !cancelled(higher) && q.comparePriority(higher.Spec, j.Spec) < 0 && !q.workerLocalOutput(higher) {
			return &HTTPError{Status: 507, Code: "file_store_capacity", Message: "file_store_capacity: shared capacity held for higher tier queued demand"}
		}
	}
	need := q.sharedOutputDemand(j)
	if err := q.yieldSharedOutput(j, status, payloads, need, j.ID); err != nil {
		return err
	}
	j.OutputReservationBytes = storageValue(need)
	j.OutputAdmissionPending = false
	return nil
}

func (q *Queue) storageAdmissionActive() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.s.StorageAdmissionActive
}

// Reclaim only through existing authority/fences. Another bounded batch
// requires observed committed-byte progress; zero progress or a publication
// fence returns immediately. Shadow preserves the original single preflight pass.
func (q *Queue) reclaimStorageForAdmission(ctx context.Context) error {
	if !q.storageAdmissionActive() {
		return q.reclaimJobFilesContext(ctx)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		q.mu.RLock()
		store := q.files
		q.mu.RUnlock()
		if store == nil {
			return nil
		}
		observe := func() (uint64, error) {
			store.lifecycle.RLock()
			defer store.lifecycle.RUnlock()
			store.mu.Lock()
			defer store.mu.Unlock()
			_, payloads, err := store.capacity(ctx)
			var committed uint64
			for _, p := range payloads {
				committed = addDiskBytes(committed, p.length)
			}
			return committed, err
		}
		before, err := observe()
		if err != nil {
			return err
		}
		if err = q.reclaimJobFilesContext(ctx); err != nil {
			return err
		}
		after, err := observe()
		if err != nil {
			return err
		}
		q.mu.RLock()
		deferred := q.fileCollectionDeferred
		same := q.files == store
		q.mu.RUnlock()
		if deferred || !same || after >= before {
			return nil
		}
	}
}

// Retries and yielded estimates acquire in owner-tier order. A blocked higher
// tier keeps lower tiers from taking its remaining shared room; independent
// worker custody continues through the normal planner.
func (q *Queue) admitOutputRetries(ctx context.Context) error {
	if !q.storageAdmissionActive() {
		return q.admitLegacyOutputRetries(ctx)
	}
	q.mu.RLock()
	pending := false
	for _, j := range q.liveJobs() {
		if j.State == "queued" && j.OutputAdmissionPending && !cancelled(j) {
			pending = true
			break
		}
	}
	q.mu.RUnlock()
	if !pending {
		return nil
	}
	if err := q.reclaimStorageForAdmission(ctx); err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	q.mu.RLock()
	store := q.files
	q.mu.RUnlock()
	if store == nil {
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
		var jobs []*Job
		for _, j := range q.liveJobs() {
			if j.State == "queued" && j.OutputAdmissionPending && !cancelled(j) {
				jobs = append(jobs, j)
			}
		}
		q.storageQueueOrder(jobs)
		blocked := -1
		for _, candidate := range jobs {
			j := q.touchJob(candidate.ID)
			rank := q.priorityMatch(j.Spec).Index
			if blocked >= 0 && rank > blocked && !q.workerLocalOutput(j) {
				continue
			}
			if err := q.admitSharedOutput(j, status, payloads); err != nil {
				j.Reason = q.storageHoldReason(j, status, payloads)
				if blocked < 0 {
					blocked = rank
				}
				q.storageCapacityAlert(j, true)
				continue
			}
			if strings.HasPrefix(j.Reason, "file_store_capacity:") {
				j.Reason = ""
			}
			q.storageCapacityAlert(j, false)
		}
		return nil
	}, nil)
}

func (q *Queue) storageHoldReason(j *Job, status FileStoreStatus, payloads map[string]storedPayload) string {
	need := addDiskBytes(q.sharedOutputDemand(j), q.outputReservations(payloads, j.ID))
	if q.priorityMatch(j.Spec).Tier != "production" {
		status.FreeBytes = q.nonProductionFileStoreFree(status, q.outputReservationsFor(payloads, j.ID, true))
	}
	return q.storagePressureReason(j, status, need)
}

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

func (q *Queue) storageCapacityAlert(j *Job, bad bool) {
	if jobClass(j.Spec) != "production" {
		return
	}
	key := "storage-admission/" + j.ID
	if !bad {
		if _, exists := q.s.Health.Conditions[key]; !exists {
			return
		}
	}
	q.observeHealth(key, bad, 0, HealthAlert{Kind: "production_storage_capacity", Store: "queue-file-store", JobID: j.ID, Attempt: j.Attempt, Project: j.Spec.Project, Reason: j.Reason, Recipients: []string{"operator", "project"}})
}

// Collection integration seam for #508. This orders only candidates already
// selected by retention; it cannot authorize deletion or shorten protection.
// Caller holds q.mu. A digest shared by owners inherits its most valuable tier.
func (q *Queue) orderStorageReclaim(hashes []string) {
	if !q.s.StorageAdmissionActive {
		return
	}

	type value struct{ tier, cost int }
	values := map[string]value{}
	for _, hash := range hashes {
		values[hash] = value{tier: len(q.effectivePriorityPolicy().Tiers), cost: 1}
	}
	visit := func(j *Job) {
		if j == nil {
			return
		}
		cost := 1
		if j.Delivery != nil && j.Delivery.State == "verified" {
			cost = 0
		}
		for _, files := range []FileManifest{taskOutputFiles(*j), jobInputFiles(j)} {
			for _, ref := range files {
				old, ok := values[ref.SHA256]
				if !ok {
					continue
				}
				tier := q.priorityMatch(j.Spec).Index
				if tier < old.tier {
					old = value{tier: tier, cost: cost}
				} else if tier == old.tier {
					old.cost = max(old.cost, cost)
				}
				values[ref.SHA256] = old
			}
		}
	}
	for _, j := range q.s.Jobs {
		visit(j)
	}
	for _, r := range q.s.Submissions {
		visit(r.Job)
	}
	slices.SortFunc(hashes, func(a, b string) int {
		av, bv := values[a], values[b]
		if av.tier != bv.tier {
			return bv.tier - av.tier
		}
		if av.cost != bv.cost {
			return av.cost - bv.cost
		}
		return strings.Compare(a, b)
	})
}

// Evaluate all qualified compatible variants/hosts before reporting a protected
// backing shortage. A full local budget is never a cluster-wide admission hold.
func (q *Queue) protectedStorageWaitingReason(j *Job, host string) string {
	if q.s.StorageProtection == nil || !q.s.StorageProtection.Enabled || !q.storageProtectionVerified {
		return ""
	}
	var reasons []string
	scan := q.placementScan()
	live := q.placementLiveness(scan)
	for _, m := range scan.machines {
		if !live[m.ID] || q.unreported[m.ID] || !q.placementPolicyAllowed(j, m, false) || !q.checkpointReady(j, m) || !j.Spec.Needs.cpuOnly() && q.deviceAdmissionBlocked(m) || host != "" && hostID(m) != host {
			continue
		}
		for _, candidate := range extensionJobs(j, m, true) {
			if !executionTargetMatches(candidate.Spec, m) || !deviceAllowed(candidate, m) || !candidate.Spec.Needs.cpuOnly() && (m.GPU == "cpu" || len(candidate.Spec.Needs.Platform.Required) > 0 && !slices.Contains(candidate.Spec.Needs.Platform.Required, m.GPU)) {
				continue
			}
			q.applyLearnedNeeds(candidate)
			candidate = q.memoryCandidate(candidate, m)
			if q.capReasonFor(candidate, m) != "" || (!sharedHostJobFits(candidate, m) || !fitsTargetResourcesChecked(candidate, m, m.TotalMemoryGB, nil)) {
				continue
			}
			if _, reason := q.storageRequest(candidate, m); reason != "" {
				continue
			}
			reason := q.protectedStorageReason(candidate, m)
			if reason == "" {
				return ""
			}
			reasons = append(reasons, reason)
		}
	}
	slices.Sort(reasons)
	if len(reasons) == 0 {
		return ""
	}
	destination := q.outputDeliveryHealth()
	return strings.Join(slices.Compact(reasons), "; ") + "; destination=" + destination
}

// Caller holds q.mu. Attribution is still the existing shared-credential policy;
// anonymous legacy uploads gain no new authority to displace another owner.
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

// Persist pressure separately from a refused upload's rolled-back reservation.
// The existing notifier fans the attributed event out to both configured owners.
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

// Requeue never conjures a fresh estimate. Tick reacquires the retry's budget
// through the same serialized file-store admission used for new tasks/uploads.
func (q *Queue) admitLegacyOutputRetries(ctx context.Context) error {
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
			if q.fileStoreReserveRefusal(status, need, j.Spec.Disk.OutputBytes, q.outputReservationsFor(payloads, "", true), q.priorityMatch(j.Spec).Tier == "production") != nil {
				continue
			}
			j = q.touchJob(j.ID)
			j.OutputAdmissionPending = false
		}
		return nil
	}, nil)
}

// These are proposals against the observed legacy cap/reserve, with every
// compatible learned estimate. Reporting does not run collection or release a
// live commitment. #509 must qualify and activate the policy before cutover.
type StorageAdmissionStatus struct {
	Migration          *StorageMigrationStatus    `json:"migration,omitempty"`
	Mode               string                     `json:"mode"`
	YieldedCommitments []string                   `json:"yielded_commitments,omitempty"`
	Proposals          []StorageAdmissionProposal `json:"proposals"`
}
type StorageAdmissionProposal struct {
	JobID       string `json:"job_id"`
	Tier        string `json:"tier"`
	OutputBytes uint64 `json:"output_bytes"`
	Admitted    bool   `json:"admitted"`
	Reason      string `json:"reason,omitempty"`
}

// q is a detached observation. The second copy isolates hypothetical yielding
// and learned commitments even from the rest of the status projection.
func (q *Queue) storageAdmissionStatus(status FileStoreStatus, payloads map[string]storedPayload, migration *storageMigrationObservation) *StorageAdmissionStatus {
	out := q.storageAdmissionProposals(status, payloads)
	if migration != nil {
		out.Migration = migration.status(status, payloads)
	}
	return out
}

func (q *Queue) storageAdmissionProposals(status FileStoreStatus, payloads map[string]storedPayload) *StorageAdmissionStatus {
	out := &StorageAdmissionStatus{Mode: "shadow", Proposals: []StorageAdmissionProposal{}}
	if q.s.StorageMigration != nil {
		status = q.automaticStorageCapacity(status, payloads)
	}
	if q.s.StorageAdmissionActive {
		out.Mode = "active"
	}
	proposed := &Queue{s: q.s, clock: q.clock, models: q.models, fileStoreProductionReserve: q.fileStoreProductionReserve}
	proposed.s.Jobs = map[string]*Job{}
	proposed.s.Submissions = nil
	proposed.s.StorageAdmissionActive = true
	var queued []*Job
	accepted := map[string]bool{}
	for _, original := range q.liveJobs() {
		j := *original
		accepted[j.ID] = !j.OutputAdmissionPending
		if j.Spec.OutputDelivery != asyncNAS || terminal(j.State) || cancelled(&j) {
			continue
		}
		if j.State == "queued" && j.PendingOutputRefs == nil && j.StorageAllocation == nil {
			j.OutputReservationBytes = storageValue(q.sharedOutputDemand(&j))
			queued = append(queued, &j)
		}
		proposed.s.Jobs[j.ID] = &j
	}
	proposed.storageQueueOrder(queued)
	// Replay proposed acquisitions in tier order, retaining writers and their
	// actual manifests. A failed acquisition remains a hold for lower tiers.
	for _, j := range queued {
		j.OutputAdmissionPending = true
	}
	for _, j := range queued {
		decision := StorageAdmissionProposal{JobID: j.ID, Tier: q.priorityMatch(j.Spec).Tier, OutputBytes: proposed.sharedOutputDemand(j)}
		if err := proposed.admitSharedOutput(j, status, payloads); err != nil {
			decision.Reason = proposed.storageHoldReason(j, status, payloads)
		} else {
			decision.Admitted = true
		}
		out.Proposals = append(out.Proposals, decision)
	}
	for _, j := range queued {
		if j.OutputAdmissionPending && accepted[j.ID] {
			out.YieldedCommitments = append(out.YieldedCommitments, j.ID)
		}
	}
	return out
}

// Read-only #508 observation seam: the queue's exact output-delivery ledger is
// authoritative. No observation means unknown, regardless of checkpoint health.
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
