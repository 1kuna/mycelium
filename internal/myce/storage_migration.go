package myce

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"time"
)

// Saved values are the effective service arguments, whether supplied by saved
// installer settings or ExecStart. Never rewrite either source during cutover.
type StorageLegacySettings struct {
	LimitBytes             uint64 `json:"limit_bytes"`
	ProductionReserveBytes uint64 `json:"production_reserve_bytes"`
}

type storageMigration struct {
	Saved            StorageLegacySettings `json:"saved"`
	Since            time.Time             `json:"since"`
	Last             time.Time             `json:"last"`
	Samples          uint64                `json:"samples"`
	Comparisons      uint64                `json:"comparisons"`
	Disagreements    uint64                `json:"disagreements"`
	RequiredPeriod   time.Duration         `json:"required_period"`
	PeriodJob        string                `json:"period_job,omitempty"`
	CompletedCycles  uint64                `json:"completed_cycles"`
	EvidenceIdentity string                `json:"evidence_identity"`
	ActivatedAt      *time.Time            `json:"activated_at,omitempty"`
	// Cohort is bounded by existing job/history retention. Only jobs compared
	// while queued in this streak can prove a complete observed lifecycle.
	Cohort map[string]bool `json:"cohort,omitempty"`
}

type StorageMigrationStatus struct {
	Saved                       *StorageLegacySettings `json:"saved,omitempty"`
	SavedSettingsActive         bool                   `json:"saved_settings_active"`
	Since                       time.Time              `json:"since"`
	Last                        time.Time              `json:"last"`
	Samples                     uint64                 `json:"samples"`
	Comparisons                 uint64                 `json:"comparisons"`
	Disagreements               uint64                 `json:"disagreements"`
	ConsistentSeconds           float64                `json:"consistent_seconds"`
	RequiredSeconds             float64                `json:"required_seconds"`
	PeriodJob                   string                 `json:"period_job,omitempty"`
	CompletedCycles             uint64                 `json:"completed_cycles"`
	ManualWriterLimitsRetirable bool                   `json:"manual_writer_limits_retirable"`
	Ready                       bool                   `json:"ready"`
	Blockers                    []string               `json:"blockers"`
}

// The old latch alone retains #518's cap/reserve shadow contract. Only the
// qualified migration retires both guards, in the same SQLite transaction.
func (q *Queue) automaticStorage() bool {
	return q.s.StorageAdmissionActive && q.s.StorageMigration != nil && q.s.StorageMigration.ActivatedAt != nil
}

func (q *Queue) storageCapacitySettings(limit, reserve uint64) (uint64, uint64, bool, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if m := q.s.StorageMigration; m != nil {
		limit, reserve = m.Saved.LimitBytes, m.Saved.ProductionReserveBytes
	}
	active := q.automaticStorage()
	if active && (q.s.StorageProtection == nil || !q.s.StorageProtection.Enabled || !q.storageProtectionVerified) {
		return limit, reserve, active, errors.New("automatic storage requires fresh backend verification")
	}
	return limit, reserve, active, nil
}

// No operator-selected waiting period: require at least one complete observed
// queued->verified-delivery->legal-collection cycle, spanning the largest such
// duration in retained evidence. Idle time and repeatedly reading status earn
// no evidence. A mismatch or process restart starts a new observation streak.
func (q *Queue) sampleStorageMigration(status FileStoreStatus, payloads map[string]storedPayload) {
	m := q.s.StorageMigration
	if m == nil || q.s.StorageAdmissionActive {
		return
	}
	copy := *m
	copy.Cohort = make(map[string]bool)
	for id, yes := range m.Cohort {
		if q.s.Jobs[id] != nil {
			copy.Cohort[id] = yes
		}
	}
	m = &copy
	now := q.clock.Now()
	evidenceIdentity := q.storageMigrationEvidenceIdentity()
	if !q.storageShadowContinuity || now.Before(m.Last) || m.EvidenceIdentity != evidenceIdentity {
		m.EvidenceIdentity = evidenceIdentity
		m.Since, m.Samples, m.Comparisons, m.CompletedCycles = now, 0, 0, 0
		m.Cohort = map[string]bool{}
	}
	q.storageShadowContinuity = true
	m.Last = now
	m.Samples++
	proposed := q.storageAdmissionProposals(status, payloads)
	mismatch := status.Error != ""
	for _, p := range proposed.Proposals {
		j := q.s.Jobs[p.JobID]
		agree := j != nil && p.Admitted == !j.OutputAdmissionPending && (!p.Admitted || p.OutputBytes == outputCommitment(j))
		m.Comparisons++
		if !agree {
			mismatch = true
			m.Disagreements++
		} else {
			m.Cohort[p.JobID] = true
		}
	}
	// Destination tasks already use the protected launch path in shadow. Compare
	// every eligible worker's bounded decision with automatic mode, which retires
	// only the unrelated legacy disk floor. Incapable hosts earn no evidence.
	for _, j := range q.liveJobs() {
		if j.State != "queued" || j.Spec.OutputDestination == nil {
			continue
		}
		for _, machine := range q.s.Machines {
			if !q.jobAllowsMachine(j, machine.ID) || !executionTargetMatches(j.Spec, machine) {
				continue
			}
			if _, reason := q.storageRequest(j, machine); reason != "" {
				continue
			}
			live := q.diskHeadroomReason(j, machine) == ""
			automatic := q.protectedStorageReason(j, machine) == "" && !j.OutputAdmissionPending
			m.Comparisons++
			if live != automatic {
				mismatch = true
				m.Disagreements++
			} else if live {
				m.Cohort[j.ID] = true
			}
		}
	}
	if mismatch {
		m.Since, m.CompletedCycles = now, 0
		m.Cohort = map[string]bool{}
	}
	m.CompletedCycles = 0
	for _, j := range q.s.Jobs {
		if j.LocalOutputs == nil || j.LocalOutputs.CollectedAt == nil || j.Delivery == nil || j.Delivery.State != "verified" || j.Delivery.VerifiedAt == nil || !sameDelivery(j, *j) {
			continue
		}
		end := *j.LocalOutputs.CollectedAt
		duration := end.Sub(j.Queued)
		if duration > m.RequiredPeriod {
			m.RequiredPeriod, m.PeriodJob = duration, j.ID
		}
		if !mismatch && m.Cohort[j.ID] && !j.Queued.Before(m.Since) && !end.After(now) && duration > 0 {
			m.CompletedCycles++
		}
	}
	q.s.StorageMigration = m
}

func (q *Queue) storageMigrationStatus(payloads map[string]storedPayload) *StorageMigrationStatus {
	out := &StorageMigrationStatus{SavedSettingsActive: !q.automaticStorage(), Blockers: []string{}}
	if m := q.s.StorageMigration; m != nil {
		saved := m.Saved
		out.Saved, out.Since, out.Last, out.Samples = &saved, m.Since, m.Last, m.Samples
		out.Comparisons, out.Disagreements, out.CompletedCycles = m.Comparisons, m.Disagreements, m.CompletedCycles
		out.ConsistentSeconds = max(0, m.Last.Sub(m.Since).Seconds())
		out.RequiredSeconds, out.PeriodJob = m.RequiredPeriod.Seconds(), m.PeriodJob
		if (!q.storageShadowContinuity && !q.s.StorageAdmissionActive) || m.EvidenceIdentity != q.storageMigrationEvidenceIdentity() || m.RequiredPeriod <= 0 || m.CompletedCycles == 0 || m.Last.Sub(m.Since) < m.RequiredPeriod || m.Comparisons == 0 {
			out.Blockers = append(out.Blockers, "shadow requires a consistent complete queued/delivery/collection cycle; period is the longest measured cycle")
		}
	} else {
		out.Blockers = append(out.Blockers, "start shadow observation to save effective settings")
	}
	p := q.s.StorageProtection
	if p == nil || !p.Enabled || !q.storageProtectionVerified {
		out.Blockers = append(out.Blockers, "fresh native backing/control/confinement qualification required")
	} else if err := validateStorageProtection(p.Snapshot, p.Control); err != nil {
		out.Blockers = append(out.Blockers, err.Error())
	} else {
		if len(p.Snapshot.Workers) == 0 {
			out.Blockers = append(out.Blockers, "qualified worker/client capability required")
		}
		for _, w := range p.Snapshot.Workers {
			m := q.s.Machines[w.Machine]
			if m == nil || m.Session != w.Session || !slices.Contains(m.Capabilities, boundedStorageCapability) || !slices.Contains(m.Capabilities, taskOutputCapability) || w.ProductionBacking == "" {
				out.Blockers = append(out.Blockers, "worker/client capability or production backing unqualified: "+w.Machine)
			}
		}
		for id, c := range p.Custody {
			a, ok := p.Allocations[id]
			if !ok || c.Allocation != id || a.ID != id {
				out.Blockers = append(out.Blockers, "custody reconstruction incomplete: "+id)
			}
		}
	}
	// Legacy shared experiment output must actually leave shared storage through
	// the existing preservation/release/collector path. No new deletion authority.
	visit := func(j *Job) {
		if j == nil {
			return
		}
		if !terminal(j.State) && (j.Spec.StorageProtocol != workerCustodyProtocol || len(j.Spec.Outputs) > 0 && j.Spec.OutputDestination == nil) {
			out.Blockers = append(out.Blockers, "legacy output writer must drain without policy rewrite: "+j.ID)
		}
		if q.priorityMatch(j.Spec).Tier == "production" {
			return
		}
		// Inputs/registry assets may remain protected shared data. Every kind of
		// legacy produced progress, including failed attempts, must be drained.
		outputs := []FileManifest{j.OutputRefs, j.PendingOutputRefs}
		for _, attempt := range j.AttemptHistory {
			if attempt.Diagnostics != nil {
				outputs = append(outputs, attempt.Diagnostics.Files)
			}
		}
		if j.FanOut != nil {
			for _, piece := range j.FanOut.Results {
				outputs = append(outputs, piece.OutputRefs)
			}
		}
		for _, checkpoint := range j.FileCheckpoints {
			outputs = append(outputs, checkpoint.Files)
		}
		if j.ResumeFiles != nil {
			outputs = append(outputs, j.ResumeFiles.Files)
		}
		for _, recovered := range j.RecoveredResults {
			outputs = append(outputs, recovered.Files)
		}
		for _, refs := range outputs {
			for _, ref := range refs {
				if _, exists := payloads[ref.SHA256]; exists {
					out.Blockers = append(out.Blockers, "legacy shared experiment backlog must be preserved and legally collected: "+j.ID)
					return
				}
			}
		}
	}
	for _, j := range q.s.Jobs {
		visit(j)
	}
	for _, r := range q.s.Submissions {
		if q.s.Jobs[r.JobID] == nil {
			visit(r.Job)
		}
	}
	slices.Sort(out.Blockers)
	out.Blockers = slices.Compact(out.Blockers)
	out.Ready = len(out.Blockers) == 0
	out.ManualWriterLimitsRetirable = q.automaticStorage() && out.Ready && out.CompletedCycles > 0
	return out
}

// StorageAdmissionMigration serializes cutover with every upload/submission and
// snapshots inventory outside q.mu. Neither status nor observe collects bytes.
func (q *Queue) StorageAdmissionMigration(ctx context.Context, action string) (*StorageMigrationStatus, error) {
	if action != "status" && action != "observe" && action != "activate" && action != "rollback" {
		return nil, errors.New("admission requires status, observe, activate or rollback")
	}
	q.fileLifecycle.Lock()
	defer q.fileLifecycle.Unlock()
	q.mu.RLock()
	store := q.files
	q.mu.RUnlock()
	if store == nil {
		return nil, errors.New("file store unavailable")
	}
	status, payloads, err := store.capacity(ctx)
	if err != nil {
		status.Error = "file store capacity unavailable"
	}
	var out *StorageMigrationStatus
	var refusal error
	if action == "status" {
		q.mu.RLock()
		out = q.storageMigrationStatus(payloads)
		if status.Error != "" {
			out.Ready = false
			out.Blockers = append(out.Blockers, status.Error)
		}
		q.mu.RUnlock()
		return out, nil
	}
	err = q.updateRequest(ctx, func() error {
		if store != q.files {
			return ErrStale
		}
		if action == "rollback" {
			if q.s.StorageMigration == nil {
				return errors.New("no saved migration settings")
			}
			q.s.StorageAdmissionActive = false
			q.storageShadowContinuity = false
			out = q.storageMigrationStatus(payloads)
			return nil
		}
		if q.s.StorageMigration == nil {
			if q.s.StorageAdmissionActive {
				return &HTTPError{Status: 409, Code: "storage_migration_incomplete", Message: "active admission latch has no qualified migration; legacy capacity guards remain"}
			}
			q.s.StorageMigration = &storageMigration{Saved: StorageLegacySettings{uint64(store.limit), uint64(q.fileStoreProductionReserve)}, Cohort: map[string]bool{}}
		}
		q.sampleStorageMigration(status, payloads)
		out = q.storageMigrationStatus(payloads)
		if action == "activate" && !q.automaticStorage() {
			if !out.Ready || status.Error != "" {
				refusal = &HTTPError{Status: 409, Code: "storage_migration_incomplete", Message: "storage migration incomplete; inspect file admission status blockers and measured shadow evidence"}
				return nil // Persist the failed comparison; refusal grants no activation.
			}
			now := q.clock.Now()
			m := *q.s.StorageMigration
			m.ActivatedAt = &now
			q.s.StorageMigration = &m
			q.s.StorageAdmissionActive = true
			out = q.storageMigrationStatus(payloads)
		}
		return nil
	}, nil)
	if err != nil {
		q.mu.Lock()
		q.storageShadowContinuity = false
		q.mu.Unlock()
	}
	return out, errors.Join(err, refusal)
}

func (a API) registerStorageMigration(mux *requestMux) {
	mux.HandleFunc("GET /api/files/admission", func(w http.ResponseWriter, r *http.Request) {
		out, err := a.Queue.StorageAdmissionMigration(r.Context(), "status")
		respond(w, out, err)
	})
	mux.HandleFunc("POST /api/files/admission/{action}", func(w http.ResponseWriter, r *http.Request) {
		out, err := a.Queue.StorageAdmissionMigration(r.Context(), r.PathValue("action"))
		respond(w, out, err)
	})
}

func (q *Queue) automaticStorageCapacity(status FileStoreStatus, payloads map[string]storedPayload) FileStoreStatus {
	var remaining, excluded uint64
	for _, p := range payloads {
		remaining = addDiskBytes(remaining, p.length-p.size)
	}
	if p := q.s.StorageProtection; p != nil {
		seen := map[string]bool{}
		for _, b := range p.Snapshot.Backings {
			if b.Pool == p.Snapshot.SharedPool && !seen[b.ID] {
				excluded = addDiskBytes(excluded, b.ExcludedFreeBytes)
				seen[b.ID] = true
			}
		}
	}
	status.FreeBytes = status.DiskFreeBytes - min(status.DiskFreeBytes, addDiskBytes(remaining, excluded))
	status.LimitBytes, status.ProductionReserveBytes, status.FloorBytes = 0, 0, 0
	return status
}

func (q *Queue) observeStorageMigration(ctx context.Context) error {
	q.mu.RLock()
	observing := q.s.StorageMigration != nil && !q.s.StorageAdmissionActive
	q.mu.RUnlock()
	if !observing {
		return nil
	}
	_, err := q.StorageAdmissionMigration(ctx, "observe")
	return err
}

// Free/usage/reservation observations vary on the same installed boundary.
// Only identities, installed limits, confinement and measured control demand
// bind the shadow proof; refreshing a sample never resets a healthy streak.
func (q *Queue) storageMigrationEvidenceIdentity() string {
	p := q.s.StorageProtection
	if p == nil {
		return ""
	}
	type boundary struct {
		ID, NativeIdentity, Pool, Kind string
		Bytes, Files                   uint64
	}
	boundaries := []boundary{}
	for _, b := range p.Snapshot.Backings {
		boundaries = append(boundaries, boundary{b.ID, b.NativeIdentity, b.Pool, b.Kind, b.CapacityBytes, b.CapacityFiles})
	}
	slices.SortFunc(boundaries, func(a, b boundary) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	workers := slices.Clone(p.Snapshot.Workers)
	slices.SortFunc(workers, func(a, b StorageWorkerBacking) int {
		if a.Machine < b.Machine {
			return -1
		}
		if a.Machine > b.Machine {
			return 1
		}
		return 0
	})
	return identityHash(struct {
		Boundaries                 []boundary
		Workers                    []StorageWorkerBacking
		ControlBytes, ControlFiles uint64
		Shared, Control            string
	}{boundaries, workers, p.Control.RequiredBytes, p.Control.Files, p.Snapshot.SharedPool, p.Snapshot.ControlBacking})
}
