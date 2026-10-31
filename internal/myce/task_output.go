package myce

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const taskOutputProtocol = "task-output/1"
const taskOutputCapability = "task_outputs_v1"

// Admission and the independent start fence share this metadata-only filter.
func taskOutputCapabilityReason(s Spec, m *Machine) string {
	if s.OutputDestination == nil {
		return ""
	}
	if m == nil || !slices.Contains(m.Capabilities, taskOutputCapability) {
		return "task_output_protocol_unavailable"
	}
	if s.OutputDestination.Kind == "nas" && !slices.Contains(m.OutputDestinationIDs, s.OutputDestination.ID) {
		return "task_output_destination_unavailable"
	}
	return ""
}

// The destination ID is operator/submitter-owned, never a worker or submitter
// filesystem path. Spec.Outputs supplies relative NAS prefixes as before.
type OutputDestination struct {
	Protocol string `json:"protocol" yaml:"protocol"`
	Kind     string `json:"kind" yaml:"kind"`
	ID       string `json:"id" yaml:"id"`
}

// Files are deliberately separate from ordinary file-store output_refs. A
// successful versioned computation can have only worker-local availability.
type TaskOutputCustody struct {
	State       string       `json:"state"`
	Allocation  string       `json:"allocation"`
	Files       FileManifest `json:"files"`
	StoredAt    time.Time    `json:"stored_at"`
	CollectedAt *time.Time   `json:"collected_at,omitempty"`
}

func validateTaskOutputs(s Spec) error {
	d := s.OutputDestination
	if d == nil {
		if s.OutputRetention != "" {
			return errors.New("output_retention requires output_destination")
		}
		return nil
	}
	if d.Protocol != taskOutputProtocol || (d.Kind != "nas" && d.Kind != "submitter") || !extensionName.MatchString(d.ID) || s.StorageProtocol != workerCustodyProtocol || s.OutputDelivery != "" || len(s.Outputs) == 0 || s.FanOut != nil {
		return errors.New("output_destination requires task-output/1, nas or submitter, a stable ID, outputs, and worker-custody/1 individual tasks")
	}
	if s.OutputRetention != "" && s.OutputRetention != "until_delivered" && s.OutputRetention != "retained" {
		return errors.New("output_retention must be until_delivered or retained")
	}
	for name, prefix := range s.Outputs {
		if !safeRelative(name) || !safeRelative(prefix) || strings.Split(name, "/")[0] == deliveryReceiptName || strings.Split(name, "/")[0] == ".myce-result-spool" {
			return errors.New("destination outputs require clean relative names and prefixes")
		}
	}
	return nil
}

func taskOutputFiles(j Job) FileManifest {
	if j.LocalOutputs != nil {
		return j.LocalOutputs.Files
	}
	return j.OutputRefs
}

func exactTaskOutputs(j *Job, r StorageCustodyReceipt) bool {
	return j != nil && j.Spec.OutputDestination != nil && j.LocalOutputs != nil && j.StorageAllocation != nil && j.ID == r.Execution.ID && j.Attempt == r.Execution.Attempt && j.Machine == r.Machine && j.Session == r.Session && j.StorageAllocation.ID == r.Allocation && j.LocalOutputs.Allocation == r.Allocation && r.Protocol == workerCustodyProtocol && maps.Equal(j.LocalOutputs.Files, r.Files)
}

func (q *Queue) acceptTaskOutputCustody(j *Job, r StorageCustodyReceipt) error {
	if j.Spec.OutputDestination == nil {
		return nil
	}
	if j.State != "running" || r.Result == nil || !r.Result.Success || r.Result.Interrupted || r.Result.ReleasePending || r.Result.Execution != r.Execution || r.Result.Machine != r.Machine || r.Result.Session != r.Session {
		return errors.New("task output custody requires successful computation and confirmed compute cleanup")
	}
	now := q.clock.Now()
	j.LocalOutputs = &TaskOutputCustody{State: "worker_local_pending_delivery", Allocation: r.Allocation, Files: maps.Clone(r.Files), StoredAt: now}
	j.State, j.Reason, j.FinishedAt = "succeeded", "worker-local outputs pending verified destination delivery", &now
	j.OutputRefs, j.PendingOutputRefs, j.OutputFiles = nil, nil, nil
	j.Delivery = &OutputDelivery{Protocol: taskOutputProtocol, Kind: j.Spec.OutputDestination.Kind, DestinationID: j.Spec.OutputDestination.ID, Execution: r.Execution, Machine: r.Machine, Session: r.Session, State: "pending", PendingAt: now, Bytes: manifestDiskBytes(r.Files)}
	j.Delivery.ManifestSHA256 = deliveryDigest(*j)
	// The synced spool and delivery-pending ledger settle compute in this same
	// SQLite transaction. No earlier release-only ACK can create a custody gap.
	q.settleRelease(*r.Result)
	j.ReleaseConfirmed = true
	retainInputsStagedAt(j, r.Result.InputsStagedAt)
	q.recordAttemptStorage(j, *r.Result)
	q.recordAttemptMemory(j, *r.Result)
	q.recordModelUse(j.Spec.Needs.Model)
	learn := *r.Result
	learn.OutputRefs = maps.Clone(r.Files)
	q.recordAttemptLearning(j, learn)
	q.recordAttemptOutcome(j, attemptOutcome(j, learn))
	q.appendJobLedger(j, ledgerAttempt(j, "worker_output_custody", j.Delivery.ManifestSHA256))
	return nil
}

type taskOutputWork struct {
	Job            Job    `json:"job"`
	Action         string `json:"action"`
	RelayID        string `json:"relay_id,omitempty"`
	Name           string `json:"name,omitempty"`
	PreservationID string `json:"preservation_id,omitempty"`
}

// Collection permission is a durable expiry, not a health probe or a completed
// command. It uses the same keeping-reference graph as #479's collector.
func (q *Queue) taskOutputCollectible(j *Job) bool {
	return q.taskOutputCollectionPolicyReady(j) && j.Delivery.State == "verified" && j.Delivery.VerifiedAt != nil
}

func (q *Queue) taskOutputCollectionPolicyReady(j *Job) bool {
	if j == nil || j.LocalOutputs == nil || j.LocalOutputs.CollectedAt != nil || j.Delivery == nil || !j.FilesExpired || !sameDelivery(j, *j) || q.jobReleasePending(j) || q.clock.Now().Before(j.LocalOutputs.StoredAt.Add(filePublicationGrace)) {
		return false
	}
	_, keep, _ := q.fileReferences(false)
	for _, f := range j.LocalOutputs.Files {
		if keep[f.SHA256] {
			return false
		}
	}
	// These protected paths share the attempt boundary. Do not retire their
	// backing merely because the ordinary outputs were delivered.
	return len(j.FileCheckpoints) == 0 && j.ResumeFiles == nil && len(j.RecoveredResults) == 0
}

func (q *Queue) TaskOutputWork(ctx context.Context, r StorageCustodyReceipt) (taskOutputWork, error) {
	var out taskOutputWork
	err := q.updateRequest(ctx, func() error {
		j := q.touchJob(r.Execution.ID)
		if !exactTaskOutputs(j, r) {
			return ErrStale
		}
		out.Job = cloneJob(j)
		out.Action = "wait"
		if j.LocalOutputs.CollectedAt != nil {
			out.Action = "collected"
			return nil
		}
		if q.taskOutputCollectible(j) {
			out.Action = "collect"
			return nil
		}
		if deliveryPending(j) && !q.clock.Now().Before(j.Delivery.NextAttemptAt) && j.Spec.OutputDestination.Kind == "nas" {
			now := q.clock.Now()
			j.Delivery.State, j.Delivery.Cause = "publishing", ""
			j.Delivery.LastAttemptAt, j.Delivery.LastProgressAt = &now, &now
			j.Delivery.Attempts++
			j.Delivery.ProgressOperations = 0
			out.Action, out.Job = "publish", cloneJob(j)
		}
		return nil
	}, nil)
	if err == nil && out.Action == "collect" && out.Job.Spec.OutputDestination.Kind == "submitter" {
		q.taskOutputMu.Lock()
		if p := q.taskOutputPreservation; p != nil && sameDelivery(&out.Job, p.job) {
			out.PreservationID = p.id
		}
		q.taskOutputMu.Unlock()
	}
	if err == nil && out.Action == "wait" && out.Job.Spec.OutputDestination.Kind == "submitter" {
		q.taskOutputMu.Lock()
		for id, relay := range q.taskOutputRelays {
			if relay.job.ID == out.Job.ID && sameDelivery(&out.Job, relay.job) && !relay.claimed {
				out.Action, out.RelayID, out.Name = "relay", id, relay.name
				break
			}
		}
		q.taskOutputMu.Unlock()
	}
	return out, err
}

type TaskOutputReceipt struct {
	StorageCustodyReceipt
	VerificationRound uint64 `json:"verification_round,omitempty"`
	DestinationID     string `json:"destination_id"`
	ManifestSHA256    string `json:"manifest_sha256"`
	Verified          bool   `json:"verified"`
}

func taskOutputReceipt(j Job) TaskOutputReceipt {
	return TaskOutputReceipt{StorageCustodyReceipt: StorageCustodyReceipt{Protocol: workerCustodyProtocol, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Allocation: j.LocalOutputs.Allocation, Files: maps.Clone(j.LocalOutputs.Files)}, DestinationID: j.Delivery.DestinationID, ManifestSHA256: j.Delivery.ManifestSHA256, VerificationRound: j.Delivery.VerificationRound, Verified: true}
}

func (q *Queue) ConfirmTaskOutputs(ctx context.Context, r TaskOutputReceipt) error {
	return q.updateRequest(ctx, func() error {
		j := q.touchJob(r.Execution.ID)
		if !exactTaskOutputs(j, r.StorageCustodyReceipt) || j.Delivery == nil || !sameDelivery(j, *j) || j.Delivery.DestinationID != r.DestinationID || j.Delivery.ManifestSHA256 != r.ManifestSHA256 || j.Delivery.VerificationRound != r.VerificationRound {
			return ErrStale
		}
		now := q.clock.Now()
		j.Delivery.LastProgressAt = &now
		if !r.Verified {
			if j.LocalOutputs.CollectedAt != nil {
				return ErrStale
			}
			preservationLost := j.Delivery.State == "verified" || j.FilesExpired
			// Invalidate historical success/negative receipts for this execution,
			// including after SQLite restart. Only a fresh work/download snapshot
			// can acknowledge redelivery; collection rechecks the same round.
			j.Delivery.VerificationRound++
			j.Delivery.State, j.Delivery.Cause, j.Delivery.VerifiedAt = "failed", "destination publication or verification failed; worker custody retained", nil
			j.LocalOutputs.State = "worker_local_pending_delivery"
			j.Delivery.NextAttemptAt = now.Add(deliveryRetryDelay)
			if preservationLost {
				j.Delivery.Cause = "destination preservation verification failed; worker custody retained"
				j.Reason = "destination copy unavailable or invalid; worker outputs retained for redelivery"
				q.observeHealth("output-preservation/"+j.ID, true, 0, HealthAlert{Kind: "output_destination_preservation_lost", JobID: j.ID, Attempt: j.Attempt, Host: j.Machine, Project: j.Spec.Project, Recipients: []string{"operator", "project"}, Reason: j.Reason})
			}
			return nil
		}
		if j.Delivery.State == "verified" {
			return nil
		}
		j.Delivery.State, j.Delivery.Cause, j.Delivery.VerifiedAt = "verified", "", &now
		j.LocalOutputs.State = "destination_verified"
		j.Delivery.NextAttemptAt = time.Time{}
		j.Reason = "outputs verified at destination; local collection follows retention and reference policy"
		q.appendJobLedger(j, ledgerAttempt(j, "output_delivery_verified", r.ManifestSHA256))
		q.observeHealth("output-preservation/"+j.ID, false, 0, HealthAlert{})
		return nil
	}, nil)
}

// Called after actual local collection and boundary retirement, never on compute
// completion. The ledger remains after its allocation/custody charge is removed.
func (q *Queue) CollectedTaskOutputs(ctx context.Context, r TaskOutputReceipt) error {
	return q.updateRequest(ctx, func() error {
		j := q.touchJob(r.Execution.ID)
		if !exactTaskOutputs(j, r.StorageCustodyReceipt) || j.Delivery == nil || j.Delivery.ManifestSHA256 != r.ManifestSHA256 || j.Delivery.DestinationID != r.DestinationID || j.Delivery.VerificationRound != r.VerificationRound {
			return ErrStale
		}
		if j.LocalOutputs.CollectedAt != nil {
			return nil
		}
		if !q.taskOutputCollectible(j) {
			return errors.New("worker output collection is protected")
		}
		p := *q.s.StorageProtection
		p.Allocations, p.Custody = maps.Clone(p.Allocations), maps.Clone(p.Custody)
		delete(p.Allocations, r.Allocation)
		delete(p.Custody, r.Allocation)
		q.s.StorageProtection = &p
		now := q.clock.Now()
		j.LocalOutputs.CollectedAt = &now
		j.LocalOutputs.State = "collected"
		q.appendJobLedger(j, ledgerAttempt(j, "worker_outputs_collected", r.ManifestSHA256))
		return nil
	}, nil)
}

// Metadata only; reuse Tick's health transaction instead of committing a second
// maintenance transaction on every tick, including queues without local output.
func (q *Queue) observeTaskOutputCustody() {
	for _, j := range q.liveJobs() {
		if j.LocalOutputs == nil || j.LocalOutputs.CollectedAt != nil {
			continue
		}
		m := q.s.Machines[j.Machine]
		lost := m == nil || m.Session != j.Session || m.Gone || q.clock.Now().Sub(q.lastContact(m, q.clock.Now())) >= q.machineLossTimeout(m.ID)
		q.observeHealth("output-custody/"+j.ID, lost, 0, HealthAlert{Kind: "worker_output_custodian_unavailable", JobID: j.ID, Attempt: j.Attempt, Host: j.Machine, Project: j.Spec.Project, Recipients: []string{"operator", "project"}})
	}
}

func (a API) taskOutputHandlers(mux *requestMux) {
	mux.HandleFunc("POST /api/workers/task-output-work", func(w http.ResponseWriter, r *http.Request) {
		var req StorageCustodyReceipt
		if !decode(w, r, &req) {
			return
		}
		out, err := a.Queue.TaskOutputWork(r.Context(), req)
		respondOperation(w, out, err, "task_output_invalid")
	})
	mux.HandleFunc("POST /api/task-outputs/verified", func(w http.ResponseWriter, r *http.Request) {
		var req TaskOutputReceipt
		if !decode(w, r, &req) {
			return
		}
		respondOperation(w, nil, a.Queue.ConfirmTaskOutputs(r.Context(), req), "task_output_invalid")
	})
	mux.HandleFunc("POST /api/workers/task-outputs-collected", func(w http.ResponseWriter, r *http.Request) {
		var req TaskOutputReceipt
		if !decode(w, r, &req) {
			return
		}
		respondOperation(w, nil, a.Queue.CollectedTaskOutputs(r.Context(), req), "task_output_invalid")
	})
	a.taskOutputRelayHandlers(mux)
	a.taskOutputPreservationHandlers(mux)
}

// The native backend must retire the exact installed write boundary after all
// its writers stop, retaining required logs/metadata on charged control backing.
// Missing support keeps the allocation charged. No production backend is yet
// qualified by #506; this seam cannot itself grant native qualification.
type StorageAttemptCollector interface {
	// Idempotent after partial release. Preserve the active worker state and
	// collection intent through its queue acknowledgement, as well as required
	// diagnostics; any remaining allocation must be independently charged.
	Collect(context.Context, StorageAllocation) error
}

func (w Worker) finishTaskOutputs(j Job, r Result, parent string) (Result, error) {
	if j.StorageAllocation == nil || r.Execution != (Execution{ID: j.ID, Attempt: j.Attempt}) || r.Machine != j.Machine || r.Session != j.Session || !r.Success || r.ReleasePending || r.Interrupted {
		return r, ErrStale
	}
	// A persisted collection intent is safe to replay after a lost acknowledgement
	// or partial unlink. Never recapture missing outputs after collection began.
	intent := filepath.Join(parent, "task-output-collection.json")
	var collected TaskOutputReceipt
	if data, err := os.ReadFile(intent); err == nil {
		if err = json.Unmarshal(data, &collected); err != nil {
			return r, err
		}
		if collected.Execution != (Execution{ID: j.ID, Attempt: j.Attempt}) || collected.Machine != j.Machine || collected.Session != j.Session || j.StorageAllocation == nil || collected.Allocation != j.StorageAllocation.ID {
			return r, ErrStale
		}
		return r, w.collectTaskOutputs(j, collected, parent)
	} else if !os.IsNotExist(err) {
		return r, err
	}
	var receipt StorageCustodyReceipt
	var work taskOutputWork
	var root string
	var err error
	haveWork := false
	if data, readErr := os.ReadFile(filepath.Join(deliverySpoolPath(j, parent), "custody.json")); readErr == nil {
		if json.Unmarshal(data, &receipt) != nil || receipt.Protocol != workerCustodyProtocol || receipt.Files.validate() != nil || receipt.Execution != (Execution{ID: j.ID, Attempt: j.Attempt}) || receipt.Machine != j.Machine || receipt.Session != j.Session || receipt.Allocation != j.StorageAllocation.ID {
			return r, ErrStale
		}
		err = w.Client.Call(context.Background(), "POST", "/api/workers/task-output-work", receipt, &work)
		if err == nil {
			haveWork = true
			if !exactTaskOutputs(&work.Job, receipt) || !sameDelivery(&work.Job, work.Job) {
				return r, ErrStale
			}
			if w.outputCustodyAccepted != nil {
				w.outputCustodyAccepted.Store(true)
			}
		} else if !stale(err) {
			return r, err
		}
	} else if !os.IsNotExist(readErr) {
		return r, readErr
	}
	// A supervisor restart re-verifies its immutable spool once, including
	// content and sync. Healthy idle polling never rehashes multi-GB outputs.
	verifiedHere := w.outputCustodyVerified != nil && w.outputCustodyVerified.Load()
	if haveWork && work.Action == "wait" && verifiedHere {
		if !exactTaskOutputs(&work.Job, receipt) || !sameDelivery(&work.Job, work.Job) {
			return r, ErrStale
		}
		return r, errStorageCustodyPending
	}
	if w.outputDeliverySlots != nil && (!haveWork || work.Action != "collect") {
		select {
		case w.outputDeliverySlots <- struct{}{}:
			defer func() { <-w.outputDeliverySlots }()
		default:
			return r, errStorageCustodyPending
		}
	}

	if !haveWork || work.Action == "publish" || work.Action == "relay" || work.Action == "wait" && !verifiedHere {
		var files FileManifest
		root, files, err = captureDeliverySpool(j, parent, w.storageObserver)
		if err != nil {
			return r, err
		}
		if haveWork && !maps.Equal(files, receipt.Files) {
			return r, ErrStale
		}
		if w.outputCustodyVerified != nil {
			w.outputCustodyVerified.Store(true)
		}
		if w.storageObserver != nil {
			w.storageObserver.sample()
			r.StoragePeak = w.storageObserver.snapshot()
		}
		if !haveWork {
			receipt = StorageCustodyReceipt{Protocol: workerCustodyProtocol, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Allocation: j.StorageAllocation.ID, Files: files, Result: &r}
			if err = saveWorkerJSON(deliverySpoolPath(j, parent), "custody.json", receipt); err != nil {
				return r, err
			}
			if err = w.Client.Call(context.Background(), "POST", "/api/workers/storage-custody", receipt, nil); err != nil {
				return r, err
			}
			if err = w.Client.Call(context.Background(), "POST", "/api/workers/task-output-work", receipt, &work); err != nil {
				return r, err
			}
			if !exactTaskOutputs(&work.Job, receipt) || !sameDelivery(&work.Job, work.Job) {
				return r, ErrStale
			}
			if w.outputCustodyAccepted != nil {
				w.outputCustodyAccepted.Store(true)
			}
		}
	}
	if !exactTaskOutputs(&work.Job, receipt) || !sameDelivery(&work.Job, work.Job) {
		return r, ErrStale
	}
	switch work.Action {
	case "collected":
		return r, nil
	case "collect":
		collected = taskOutputReceipt(work.Job)
		if _, ok := w.StorageBoundary.(StorageAttemptCollector); !ok {
			return r, errStorageCustodyPending
		}
		if err = saveWorkerJSON(parent, filepath.Base(intent), collected); err != nil {
			return r, err
		}
		return r, w.collectTaskOutputs(j, collected, parent)
	case "publish":
		destination := w.OutputDestinations[work.Job.Spec.OutputDestination.ID]
		publishErr := publishQualifiedTaskOutputDelivery(work.Job, root, destination, w.outputDeliveryProgress)
		confirmed := taskOutputReceipt(work.Job)
		confirmed.Verified = publishErr == nil
		if err = w.Client.Call(context.Background(), "POST", "/api/task-outputs/verified", confirmed, nil); err != nil {
			return r, err
		}
	case "relay":
		if err = w.relayTaskOutput(work, root); err != nil {
			return r, err
		}
	case "wait":
	default:
		return r, errors.New("unsupported task output work")
	}
	r.OutputRefs = nil
	return r, errStorageCustodyPending
}

func (w Worker) collectTaskOutputs(j Job, receipt TaskOutputReceipt, parent string) error {
	// Recheck the durable authorization before replaying any deletion. New
	// references to local-only expired results are forbidden at publication.
	var work taskOutputWork
	if err := w.Client.Call(context.Background(), "POST", "/api/workers/task-output-work", receipt.StorageCustodyReceipt, &work); err != nil {
		return err
	}
	if !exactTaskOutputs(&work.Job, receipt.StorageCustodyReceipt) || !sameDelivery(&work.Job, work.Job) || receipt.ManifestSHA256 != work.Job.Delivery.ManifestSHA256 || receipt.DestinationID != work.Job.Delivery.DestinationID {
		return ErrStale
	}
	if w.outputCustodyAccepted != nil {
		w.outputCustodyAccepted.Store(true)
	}
	// An intent carries immutable execution/content identity, never a reusable
	// preservation proof. Bind this invocation to the queue's current round.
	receipt = taskOutputReceipt(work.Job)
	if work.Action == "collected" {
		return nil
	}
	// Reconcile custody before waiting for the shared I/O slot: a blocked NAS
	// verifier must not keep another released actor in compute recovery.
	if w.outputDeliverySlots != nil {
		select {
		case w.outputDeliverySlots <- struct{}{}:
			defer func() { <-w.outputDeliverySlots }()
		default:
			return errStorageCustodyPending
		}
	}
	if work.Action != "collect" {
		// A failed fresh preservation check can resume publication only while
		// the entire immutable local spool still exists. Partial unlink replay
		// must keep its intent and can never recapture missing command outputs.
		if work.Action == "publish" || work.Action == "relay" || work.Action == "wait" && work.Job.Delivery.State == "failed" {
			if _, err := os.Stat(filepath.Join(deliverySpoolPath(j, parent), "manifest.json")); err == nil {
				_, files, verifyErr := captureDeliverySpool(j, parent, w.storageObserver)
				if verifyErr == nil && maps.Equal(files, receipt.Files) {
					if err = os.Remove(filepath.Join(parent, "task-output-collection.json")); err != nil && !os.IsNotExist(err) {
						return err
					}
					if err = syncWorkerDir(parent); err != nil {
						return err
					}
					return errStorageCustodyPending
				}
			}
		}
		return errStorageCustodyPending
	}
	if work.PreservationID != "" {
		defer w.Client.Call(context.Background(), "DELETE", "/api/task-output-preservation/"+work.PreservationID, nil, nil)
	}
	collector, ok := w.StorageBoundary.(StorageAttemptCollector)
	if !ok {
		return errStorageCustodyPending
	}
	// Remove only the authorized attempt's ordinary work/spool. Checkpoints and
	// diagnostic/control state are not swept by an output collector.
	workdir := filepath.Dir(deliverySpoolPath(j, parent))
	if err := noModelSymlinks(workdir, true); err != nil && !os.IsNotExist(err) {
		return err
	}
	// Historical receipts cannot authorize deleting the last local copy. Hash
	// the destination now on every invocation, including collection-intent
	// replay after restart. Neither this I/O nor the HTTP stream owns q.mu.
	var verifyErr error
	if work.Job.Spec.OutputDestination.Kind == "nas" {
		verifyErr = verifyQualifiedTaskOutputDelivery(work.Job, w.OutputDestinations[work.Job.Spec.OutputDestination.ID], w.outputDeliveryProgress)
	} else {
		verifyErr = w.verifySubmitterPreservation(work)
	}
	if verifyErr != nil {
		receipt.Verified = false
		if err := w.Client.Call(context.Background(), "POST", "/api/task-outputs/verified", receipt, nil); err != nil {
			return err
		}
		return errStorageCustodyPending
	}
	// Recheck exact execution/manifest and the live retention/reference fences
	// inside the queue transaction after verification, immediately before unlink.
	var current taskOutputWork
	if err := w.Client.Call(context.Background(), "POST", "/api/workers/task-output-work", receipt.StorageCustodyReceipt, &current); err != nil {
		return err
	}
	if current.Action != "collect" || !sameDelivery(&current.Job, work.Job) || current.PreservationID != work.PreservationID {
		return errStorageCustodyPending
	}
	if err := os.RemoveAll(workdir); err != nil {
		return err
	}
	if err := syncWorkerDir(filepath.Dir(workdir)); err != nil {
		return err
	}
	if err := collector.Collect(context.Background(), *j.StorageAllocation); err != nil {
		return err
	}
	return w.Client.Call(context.Background(), "POST", "/api/workers/task-outputs-collected", receipt, nil)
}
