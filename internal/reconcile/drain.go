package reconcile

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"mycelium/internal/catalog"
	"mycelium/internal/domain"
	"mycelium/internal/engine"
	"mycelium/internal/enginecatalog"
	"mycelium/internal/modelruntime"
	"mycelium/internal/ports"
)

type TaskStore interface {
	ListReconciliationTasks(ctx context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error)
	SaveReconciliationTask(ctx context.Context, task domain.ReconciliationTaskRecord) error
	SaveIncompatibility(ctx context.Context, rec domain.IncompatibilityRecord) error
	Preset(ctx context.Context, id string) (domain.Preset, error)
	Node(ctx context.Context, id string) (domain.Node, error)
	SaveLocalityPlan(ctx context.Context, plan domain.LocalityPlan) error
	SaveModelLocality(ctx context.Context, locality domain.ModelLocality) error
	SaveEngineProfile(ctx context.Context, profile domain.EngineProfile) error
	ListEngineProfiles(ctx context.Context) ([]domain.EngineProfile, error)
	MarkEngineProfileUnready(ctx context.Context, profileID, reason string) error
}

type taskJobReader interface {
	Job(ctx context.Context, id string) (domain.Job, error)
}

type DrainOptions struct {
	JobID         string
	Limit         int
	Now           time.Time
	Project       string
	Prewarm       PrewarmExecutor
	Capacity      CapacityRepairExecutor
	Catalog       CatalogRefresher
	Stage         ports.PeerCatalogStager
	Kinds         []TaskKind
	ActionTimeout time.Duration
}

type DrainReport struct {
	Drained int               `json:"drained"`
	Skipped int               `json:"skipped"`
	Results []DrainTaskResult `json:"results,omitempty"`
}

type DrainTaskResult struct {
	TaskID     string                          `json:"task_id"`
	Kind       string                          `json:"kind"`
	Status     domain.ReconciliationTaskStatus `json:"status"`
	Action     string                          `json:"action,omitempty"`
	WorkloadID string                          `json:"workload_id,omitempty"`
	NodeID     string                          `json:"node_id,omitempty"`
	InstanceID string                          `json:"instance_id,omitempty"`
	LeaseID    string                          `json:"lease_id,omitempty"`
	Error      string                          `json:"error,omitempty"`
}

type PrewarmExecutor interface {
	PrewarmReplica(ctx context.Context, job domain.Job) (PrewarmResult, error)
}

type PrewarmResult struct {
	WorkloadID string `json:"workload_id,omitempty"`
	NodeID     string `json:"node_id,omitempty"`
	InstanceID string `json:"instance_id,omitempty"`
	LeaseID    string `json:"lease_id,omitempty"`
}

type CapacityRepairExecutor interface {
	RepairCapacity(ctx context.Context, limit int) (CapacityRepairResult, error)
}

type CapacityRepairResult struct {
	Drained     int      `json:"drained"`
	WorkloadIDs []string `json:"workload_ids,omitempty"`
}

type CatalogRefresher interface {
	RefreshCatalog(ctx context.Context, task domain.ReconciliationTaskRecord) error
}

type CatalogPresetStore interface {
	SavePreset(ctx context.Context, preset domain.Preset) error
}

type CatalogPresetRegistrar interface {
	RegisterPreset(preset domain.Preset) error
}

type EngineProfileLister interface {
	ListEngineProfiles(ctx context.Context) ([]domain.EngineProfile, error)
}

type ConfigCatalogRefresher struct {
	Store       CatalogPresetStore
	Registrar   CatalogPresetRegistrar
	Presets     []domain.Preset
	StoreDir    string
	LocalNodeID string
}

func (r ConfigCatalogRefresher) RefreshCatalog(ctx context.Context, task domain.ReconciliationTaskRecord) error {
	if r.Store == nil {
		return fmt.Errorf("catalog refresh requires a preset store")
	}
	if strings.TrimSpace(task.ModelSource) != "" {
		if strings.TrimSpace(r.StoreDir) == "" {
			return fmt.Errorf("catalog source materialization requires a catalog store dir")
		}
		if task.Backend != "" && task.Backend != domain.BackendLlamaCpp {
			contract, err := r.sourceContract(ctx, task)
			if err != nil {
				return err
			}
			result, err := catalog.NewInstaller(r.StoreDir).MaterializeSourcePreset(ctx, catalog.SourcePresetRequest{
				Source:        strings.TrimSpace(task.ModelSource),
				ID:            task.PresetID,
				Model:         task.Model,
				Backend:       task.Backend,
				ContextLength: task.ContextRequest,
				Contract:      contract,
			})
			if err != nil {
				return err
			}
			return r.savePreset(ctx, r.localSourcePreset(result.Preset))
		}
		result, err := catalog.NewInstaller(r.StoreDir).Install(ctx, catalog.InstallRequest{
			Source:        strings.TrimSpace(task.ModelSource),
			ID:            task.PresetID,
			Model:         task.Model,
			ContextLength: task.ContextRequest,
			Backend:       task.Backend,
		})
		if err != nil {
			return err
		}
		return r.savePreset(ctx, r.localSourcePreset(result.Preset))
	}
	matches := catalogRefreshMatches(r.Presets, task)
	if len(matches) == 0 {
		target := firstNonEmpty(task.PresetID, task.Model)
		if target == "" {
			return fmt.Errorf("configured catalog has no presets to refresh")
		}
		return fmt.Errorf("configured catalog has no preset matching %q", target)
	}
	for _, preset := range matches {
		if preset.ID == "" {
			return fmt.Errorf("configured catalog contains a preset without an id")
		}
		if err := r.savePreset(ctx, preset); err != nil {
			return err
		}
	}
	return nil
}

func (r ConfigCatalogRefresher) localSourcePreset(preset domain.Preset) domain.Preset {
	if r.LocalNodeID != "" && preset.NodeID == "" {
		preset.NodeID = r.LocalNodeID
	}
	return preset
}

func (r ConfigCatalogRefresher) savePreset(ctx context.Context, preset domain.Preset) error {
	if err := r.Store.SavePreset(ctx, preset); err != nil {
		return err
	}
	if r.Registrar != nil {
		if err := r.Registrar.RegisterPreset(preset); err != nil {
			return err
		}
	}
	return nil
}

func (r ConfigCatalogRefresher) sourceContract(ctx context.Context, task domain.ReconciliationTaskRecord) (*domain.ModelSourceContract, error) {
	if task.ModelSourceContract == nil {
		return nil, fmt.Errorf("source preset requires resource contract")
	}
	contract := *task.ModelSourceContract
	if strings.TrimSpace(contract.EngineProfile) != "" {
		return &contract, nil
	}
	profile, err := r.resolveReadyEngineProfile(ctx, task.Backend, contract.Format)
	if err != nil {
		return nil, err
	}
	contract.EngineProfile = profile.ID
	return &contract, nil
}

func (r ConfigCatalogRefresher) resolveReadyEngineProfile(ctx context.Context, backend domain.Backend, format string) (domain.EngineProfile, error) {
	lister, ok := r.Store.(EngineProfileLister)
	if !ok {
		return domain.EngineProfile{}, fmt.Errorf("source preset requires engine_profile readiness contract; catalog store cannot list engine profiles")
	}
	profiles, err := lister.ListEngineProfiles(ctx)
	if err != nil {
		return domain.EngineProfile{}, err
	}
	format = strings.TrimSpace(format)
	if format == "" {
		format = catalog.FormatHFTransformers
	}
	var matches []domain.EngineProfile
	for _, profile := range profiles {
		if !profile.Ready || profile.Backend != backend || strings.TrimSpace(profile.ID) == "" {
			continue
		}
		if !engineProfileSupportsFormat(profile, format) {
			continue
		}
		matches = append(matches, profile)
	}
	switch len(matches) {
	case 0:
		return domain.EngineProfile{}, fmt.Errorf("no saved ready engine profile for backend %s and source format %s", backend, format)
	case 1:
		return matches[0], nil
	default:
		return domain.EngineProfile{}, fmt.Errorf("multiple saved ready engine profiles match backend %s and source format %s; source contract must name engine_profile", backend, format)
	}
}

func engineProfileSupportsFormat(profile domain.EngineProfile, format string) bool {
	if len(profile.SupportedModels) == 0 {
		return true
	}
	for _, supported := range profile.SupportedModels {
		if enginecatalog.FormatsCompatible(supported, format) {
			return true
		}
	}
	return false
}

func catalogRefreshMatches(presets []domain.Preset, task domain.ReconciliationTaskRecord) []domain.Preset {
	if task.PresetID == "" && task.Model == "" {
		return append([]domain.Preset(nil), presets...)
	}
	out := make([]domain.Preset, 0, len(presets))
	for _, preset := range presets {
		if task.PresetID != "" && preset.ID != task.PresetID {
			continue
		}
		if task.Model != "" && !presetMatchesModel(preset, task.Model) {
			continue
		}
		out = append(out, preset)
	}
	return out
}

func presetMatchesModel(preset domain.Preset, model string) bool {
	if model == "" {
		return true
	}
	if preset.ID == model || preset.ModelRef == model {
		return true
	}
	for _, alias := range preset.Aliases {
		if alias == model {
			return true
		}
	}
	return false
}

func DrainAllowedTasks(ctx context.Context, store TaskStore, opts DrainOptions) (DrainReport, error) {
	if store == nil {
		return DrainReport{}, fmt.Errorf("reconciliation task store is required")
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tasks, err := store.ListReconciliationTasks(ctx, opts.JobID)
	if err != nil {
		return DrainReport{}, err
	}
	report := DrainReport{}
	for _, task := range tasks {
		if task.Status == domain.ReconciliationTaskRunning {
			result, recovered, err := failStaleRunningTask(ctx, store, task, opts.ActionTimeout, now)
			if err != nil {
				return report, err
			}
			if recovered {
				report.Drained++
				report.Results = append(report.Results, result)
				continue
			}
		}
		if task.Status != domain.ReconciliationTaskAllowed {
			report.Skipped++
			continue
		}
		if opts.Limit > 0 && report.Drained >= opts.Limit {
			report.Skipped++
			continue
		}
		if result, closed, err := closeTaskForTerminalSourceJob(ctx, store, task, now); err != nil {
			return report, err
		} else if closed {
			report.Drained++
			report.Results = append(report.Results, result)
			continue
		}
		if !drainKindAllowed(TaskKind(task.Kind), opts.Kinds) {
			report.Skipped++
			continue
		}
		taskCtx := ctx
		cancel := func() {}
		if opts.ActionTimeout > 0 {
			taskCtx, cancel = context.WithTimeout(ctx, opts.ActionTimeout)
		}
		result := drainTask(taskCtx, store, task, opts, now)
		cancel()
		report.Drained++
		report.Results = append(report.Results, result)
	}
	return report, nil
}

type taskJobWriter interface {
	SaveJob(ctx context.Context, job domain.Job) error
}

func failStaleRunningTask(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, timeout time.Duration, now time.Time) (DrainTaskResult, bool, error) {
	if timeout <= 0 || task.UpdatedAt.IsZero() || now.Sub(task.UpdatedAt) <= timeout {
		return DrainTaskResult{}, false, nil
	}
	msg := fmt.Sprintf("reconciliation task exceeded action timeout %s", timeout)
	failed := transitionTask(task, domain.ReconciliationTaskFailed, msg, msg, now)
	if err := store.SaveReconciliationTask(ctx, failed); err != nil {
		return DrainTaskResult{}, false, err
	}
	if TaskKind(task.Kind) == TaskPrewarmReplica {
		if err := failPrewarmWorkload(ctx, store, task, msg); err != nil {
			return DrainTaskResult{}, false, err
		}
	}
	return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: msg}, true, nil
}

func failPrewarmWorkload(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, cause string) error {
	reader, canRead := store.(taskJobReader)
	writer, canWrite := store.(taskJobWriter)
	if !canRead || !canWrite {
		return nil
	}
	jobID := "reconcile-prewarm-" + task.ID
	job, err := reader.Job(ctx, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if jobStatusTerminal(job.Status) {
		return nil
	}
	job.Status = domain.JobFailed
	job.Error = cause
	if job.Result == nil {
		job.Result = &domain.JobResult{Source: "reconcile", ResultRedacted: true}
	}
	job.Result.Terminal = true
	return writer.SaveJob(ctx, job)
}

func jobStatusTerminal(status domain.JobStatus) bool {
	return status == domain.JobDone || status == domain.JobFailed || status == domain.JobPreempted
}

func drainKindAllowed(kind TaskKind, allowed []TaskKind) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, candidate := range allowed {
		if kind == candidate {
			return true
		}
	}
	return false
}

func closeTaskForTerminalSourceJob(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, now time.Time) (DrainTaskResult, bool, error) {
	if task.JobID == "" || task.Source != "placement_decision" {
		return DrainTaskResult{}, false, nil
	}
	reader, ok := store.(taskJobReader)
	if !ok {
		return DrainTaskResult{}, false, nil
	}
	job, err := reader.Job(ctx, task.JobID)
	if errors.Is(err, sql.ErrNoRows) {
		return DrainTaskResult{}, false, nil
	}
	if err != nil {
		return DrainTaskResult{}, false, err
	}
	if !terminalReconciliationSourceStatus(job.Status) {
		return DrainTaskResult{}, false, nil
	}
	done := transitionTask(task, domain.ReconciliationTaskDone, "", "source job is terminal; reconciliation task no longer applies", now)
	if err := store.SaveReconciliationTask(ctx, done); err != nil {
		return DrainTaskResult{}, false, err
	}
	return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: done.Status, Action: "source_job_terminal"}, true, nil
}

func terminalReconciliationSourceStatus(status domain.JobStatus) bool {
	return status == domain.JobDone || status == domain.JobFailed || status == domain.JobPreempted
}

func drainTask(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, opts DrainOptions, now time.Time) DrainTaskResult {
	running := transitionTask(task, domain.ReconciliationTaskRunning, "", "executor started", now)
	if err := store.SaveReconciliationTask(ctx, running); err != nil {
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: task.Status, Error: err.Error()}
	}
	switch TaskKind(task.Kind) {
	case TaskRecordIncompatibility:
		rec := incompatibilityFromTask(running, now)
		if err := store.SaveIncompatibility(ctx, rec); err != nil {
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err.Error(), "failed to record incompatibility", now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err.Error()}
		}
		done := transitionTask(running, domain.ReconciliationTaskDone, "", "recorded placement incompatibility", now)
		_ = store.SaveReconciliationTask(ctx, done)
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: done.Status, Action: "record_incompatibility"}
	case TaskStageModelArtifact:
		if err := stageModelArtifact(ctx, store, running, opts.Stage, now); err != nil {
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err.Error(), "failed to stage model artifact", now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err.Error()}
		}
		done := transitionTask(running, domain.ReconciliationTaskDone, "", "staged model artifact through peer catalog", now)
		_ = store.SaveReconciliationTask(ctx, done)
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: done.Status, Action: "stage_model_artifact"}
	case TaskVerifyEngineReadiness:
		if err := verifyEngineReadiness(ctx, store, running); err != nil {
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err.Error(), "engine readiness verification failed", now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err.Error()}
		}
		done := transitionTask(running, domain.ReconciliationTaskDone, "", "verified saved engine readiness", now)
		_ = store.SaveReconciliationTask(ctx, done)
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: done.Status, Action: "verify_engine_readiness"}
	case TaskWaitCapacity:
		result, err := repairCapacity(ctx, opts.Capacity, opts.Limit)
		if err != nil {
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err.Error(), "capacity repair failed", now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err.Error()}
		}
		if result.Drained == 0 {
			blocked := transitionTask(running, domain.ReconciliationTaskBlocked, "", "capacity repair found no runnable queued work", now)
			blocked.Blocker = "capacity_unavailable"
			_ = store.SaveReconciliationTask(ctx, blocked)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: blocked.Status, Action: "wait_capacity", Error: blocked.Blocker}
		}
		done := transitionTask(running, domain.ReconciliationTaskDone, "", "drained queued work after capacity became available", now)
		_ = store.SaveReconciliationTask(ctx, done)
		out := DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: done.Status, Action: "wait_capacity"}
		if len(result.WorkloadIDs) == 1 {
			out.WorkloadID = result.WorkloadIDs[0]
		}
		return out
	case TaskPrewarmReplica:
		result, err := prewarmReplica(ctx, store, running, opts)
		if err != nil {
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err.Error(), "prewarm replica failed", now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err.Error()}
		}
		done := transitionTask(running, domain.ReconciliationTaskDone, "", "prewarmed replica through workload admission", now)
		_ = store.SaveReconciliationTask(ctx, done)
		return DrainTaskResult{
			TaskID:     task.ID,
			Kind:       task.Kind,
			Status:     done.Status,
			Action:     "prewarm_replica",
			WorkloadID: result.WorkloadID,
			NodeID:     result.NodeID,
			InstanceID: result.InstanceID,
			LeaseID:    result.LeaseID,
		}
	case TaskRefreshCatalog:
		if opts.Catalog == nil {
			err := "catalog refresh executor is not configured; no catalog source contract is registered"
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err, "catalog refresh executor unavailable", now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err}
		}
		if err := opts.Catalog.RefreshCatalog(ctx, running); err != nil {
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err.Error(), "catalog refresh failed", now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err.Error()}
		}
		done := transitionTask(running, domain.ReconciliationTaskDone, "", "refreshed catalog from configured source", now)
		_ = store.SaveReconciliationTask(ctx, done)
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: done.Status, Action: "refresh_catalog"}
	default:
		err := fmt.Sprintf("reconciliation executor for %q is not implemented", task.Kind)
		failed := transitionTask(running, domain.ReconciliationTaskFailed, err, "executor unavailable", now)
		_ = store.SaveReconciliationTask(ctx, failed)
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err}
	}
}

func repairCapacity(ctx context.Context, executor CapacityRepairExecutor, limit int) (CapacityRepairResult, error) {
	if executor == nil {
		return CapacityRepairResult{}, fmt.Errorf("capacity repair executor is not configured; provide an explicit workload queue drain path")
	}
	if limit <= 0 {
		limit = 1
	}
	return executor.RepairCapacity(ctx, limit)
}

func prewarmReplica(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, opts DrainOptions) (PrewarmResult, error) {
	if opts.Prewarm == nil {
		return PrewarmResult{}, fmt.Errorf("prewarm executor is not configured; provide an explicit workload admission endpoint")
	}
	if task.PresetID == "" {
		return PrewarmResult{}, fmt.Errorf("prewarm replica task requires preset id")
	}
	preset, err := store.Preset(ctx, task.PresetID)
	if err != nil {
		return PrewarmResult{}, err
	}
	job := domain.Job{
		ID:                  "reconcile-prewarm-" + task.ID,
		TaskType:            string(domain.CapabilityChat),
		Model:               firstNonEmpty(task.Model, firstAlias(preset.Aliases), preset.ModelRef, preset.ID),
		PresetID:            preset.ID,
		Project:             firstNonEmpty(task.Project, opts.Project, "reconcile-prewarm"),
		Priority:            domain.PriorityBackground,
		SpeedPref:           domain.SpeedThroughput,
		ContextRequest:      firstPositive(task.ContextRequest, modelruntime.EffectiveContextLength(preset)),
		ExpectedConcurrency: firstPositive(task.ExpectedConcurrency, 1),
		Preemption:          domain.PreemptSoft,
		Status:              domain.JobQueued,
		ParentID:            task.JobID,
	}
	if task.NodeID != "" {
		job.NodeSelector = map[string]string{domain.LabelNodeID: task.NodeID}
	}
	return opts.Prewarm.PrewarmReplica(ctx, job)
}

func stageModelArtifact(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, stager ports.PeerCatalogStager, now time.Time) error {
	if task.NodeID == "" {
		return fmt.Errorf("stage model artifact task requires node id")
	}
	if task.PresetID == "" {
		return fmt.Errorf("stage model artifact task requires preset id")
	}
	preset, err := store.Preset(ctx, task.PresetID)
	if err != nil {
		return err
	}
	node, err := store.Node(ctx, task.NodeID)
	if err != nil {
		return err
	}
	action := domain.LocalityAction{
		ID:             localityActionID(task),
		Kind:           domain.LocalityActionStage,
		PresetID:       preset.ID,
		NodeID:         node.ID,
		Source:         preset.ModelRef,
		ArtifactSizeMB: preset.ArtifactSizeMB,
		Reason:         firstNonEmpty(task.Reason, "reconciliation task"),
		State:          domain.ModelLocalityPlanned,
	}
	plan := domain.LocalityPlan{
		ID:        "reconcile-" + task.ID,
		CreatedAt: now,
		Actions:   []domain.LocalityAction{action},
	}
	if err := store.SaveLocalityPlan(ctx, plan); err != nil {
		return err
	}
	locality := domain.ModelLocality{
		ID:             modelLocalityID(node.ID, preset.ID),
		PresetID:       preset.ID,
		NodeID:         node.ID,
		State:          domain.ModelLocalityPlanned,
		ModelRef:       preset.ModelRef,
		Source:         preset.ModelRef,
		ArtifactSizeMB: preset.ArtifactSizeMB,
		Managed:        true,
		Reason:         "planned by reconciliation task " + task.ID,
		UpdatedAt:      now,
	}
	if err := store.SaveModelLocality(ctx, locality); err != nil {
		return err
	}
	if stager == nil {
		return fmt.Errorf("stage model artifact executor is not configured; locality plan recorded")
	}
	if node.Address == "" {
		return fmt.Errorf("node %q has no reachable address for model artifact staging", node.ID)
	}
	action.State = domain.ModelLocalityStaging
	plan.Actions[0] = action
	if err := store.SaveLocalityPlan(ctx, plan); err != nil {
		return err
	}
	locality.State = domain.ModelLocalityStaging
	locality.Reason = "staging by reconciliation task " + task.ID
	locality.UpdatedAt = now
	if err := store.SaveModelLocality(ctx, locality); err != nil {
		return err
	}
	staged, err := stager.StageModel(ctx, domain.Peer{ID: node.ID, Addresses: []string{node.Address}, Compute: true}, preset)
	if err != nil {
		action.State = domain.ModelLocalityFailed
		action.Error = err.Error()
		plan.Actions[0] = action
		_ = store.SaveLocalityPlan(ctx, plan)
		locality.State = domain.ModelLocalityFailed
		locality.Reason = err.Error()
		locality.UpdatedAt = now
		_ = store.SaveModelLocality(ctx, locality)
		return err
	}
	if staged.State != domain.ModelLocalityReady {
		return fmt.Errorf("catalog stage returned locality state %q", staged.State)
	}
	if staged.PresetID != preset.ID {
		return fmt.Errorf("catalog stage returned preset %q, expected %q", staged.PresetID, preset.ID)
	}
	if staged.NodeID != node.ID {
		return fmt.Errorf("catalog stage returned node %q, expected %q", staged.NodeID, node.ID)
	}
	if err := store.SaveModelLocality(ctx, staged); err != nil {
		return err
	}
	action.State = staged.State
	action.Error = ""
	plan.Actions[0] = action
	return store.SaveLocalityPlan(ctx, plan)
}

func verifyEngineReadiness(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord) error {
	if task.NodeID == "" {
		return fmt.Errorf("verify engine readiness task requires node id")
	}
	if task.PresetID == "" {
		return fmt.Errorf("verify engine readiness task requires preset id")
	}
	preset, err := store.Preset(ctx, task.PresetID)
	if err != nil {
		return err
	}
	node, err := store.Node(ctx, task.NodeID)
	if err != nil {
		return err
	}
	check, err := engine.NewReadinessChecker(store, domain.EngineReadinessStrict).CheckEngineReadiness(ctx, node, preset)
	if err != nil {
		return err
	}
	if !check.Ready {
		return fmt.Errorf("engine readiness check returned not ready: %s", check.Reason)
	}
	return nil
}

func transitionTask(task domain.ReconciliationTaskRecord, status domain.ReconciliationTaskStatus, errText, message string, at time.Time) domain.ReconciliationTaskRecord {
	task.Status = status
	task.Error = errText
	task.UpdatedAt = at
	task.Events = append(task.Events, domain.ReconciliationTaskEvent{Status: status, Message: message, At: at})
	return task
}

func localityActionID(task domain.ReconciliationTaskRecord) string {
	return strings.Join([]string{"stage", task.NodeID, task.PresetID, task.ID}, ":")
}

func modelLocalityID(nodeID, presetID string) string {
	return nodeID + ":" + presetID
}

func incompatibilityFromTask(task domain.ReconciliationTaskRecord, now time.Time) domain.IncompatibilityRecord {
	return domain.IncompatibilityRecord{
		ID:        incompatibilityID(task),
		TaskID:    task.ID,
		JobID:     task.JobID,
		NodeID:    task.NodeID,
		PresetID:  task.PresetID,
		Model:     task.Model,
		Reason:    task.Reason,
		Source:    "reconciliation_task",
		CreatedAt: now,
	}
}

func incompatibilityID(task domain.ReconciliationTaskRecord) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		task.ID,
		task.JobID,
		task.NodeID,
		task.PresetID,
		task.Model,
		task.Reason,
	}, "\x00")))
	return "incompat-" + hex.EncodeToString(sum[:8])
}

func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
