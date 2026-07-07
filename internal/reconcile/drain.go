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
	"mycelium/internal/modelruntime"
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
	JobID   string
	Limit   int
	Now     time.Time
	Project string
	Prewarm PrewarmExecutor
	Catalog CatalogRefresher
	Kinds   []TaskKind
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

type CatalogRefresher interface {
	RefreshCatalog(ctx context.Context, task domain.ReconciliationTaskRecord) error
}

type CatalogPresetStore interface {
	SavePreset(ctx context.Context, preset domain.Preset) error
}

type ConfigCatalogRefresher struct {
	Store    CatalogPresetStore
	Presets  []domain.Preset
	StoreDir string
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
			result, err := catalog.NewInstaller(r.StoreDir).MaterializeSourcePreset(ctx, catalog.SourcePresetRequest{
				Source:        strings.TrimSpace(task.ModelSource),
				ID:            task.PresetID,
				Model:         task.Model,
				Backend:       task.Backend,
				ContextLength: task.ContextRequest,
				Contract:      task.ModelSourceContract,
			})
			if err != nil {
				return err
			}
			return r.Store.SavePreset(ctx, result.Preset)
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
		return r.Store.SavePreset(ctx, result.Preset)
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
		if err := r.Store.SavePreset(ctx, preset); err != nil {
			return err
		}
	}
	return nil
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
		result := drainTask(ctx, store, task, opts, now)
		report.Drained++
		report.Results = append(report.Results, result)
	}
	return report, nil
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
		if err := planModelArtifactStage(ctx, store, running, now); err != nil {
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err.Error(), "failed to plan model artifact staging", now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err.Error()}
		}
		done := transitionTask(running, domain.ReconciliationTaskDone, "", "planned model artifact staging", now)
		_ = store.SaveReconciliationTask(ctx, done)
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: done.Status, Action: "plan_model_artifact_stage"}
	case TaskVerifyEngineReadiness:
		if err := verifyEngineReadiness(ctx, store, running); err != nil {
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err.Error(), "engine readiness verification failed", now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err.Error()}
		}
		done := transitionTask(running, domain.ReconciliationTaskDone, "", "verified saved engine readiness", now)
		_ = store.SaveReconciliationTask(ctx, done)
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: done.Status, Action: "verify_engine_readiness"}
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

func planModelArtifactStage(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, now time.Time) error {
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
	return store.SaveModelLocality(ctx, domain.ModelLocality{
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
	})
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
