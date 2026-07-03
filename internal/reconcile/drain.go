package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

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

type DrainOptions struct {
	JobID                   string
	Limit                   int
	Now                     time.Time
	Project                 string
	Prewarm                 PrewarmExecutor
	Catalog                 CatalogRefresher
	ArtifactStager          ArtifactStager
	KeepStageDemandEligible bool
	TaskIDs                 []string
	Kinds                   []TaskKind
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
	PresetID   string                          `json:"preset_id,omitempty"`
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

type ArtifactStager interface {
	StageModel(ctx context.Context, peer domain.Peer, preset domain.Preset) (domain.ModelLocality, error)
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
		if !drainTaskIDAllowed(task.ID, opts.TaskIDs) {
			report.Skipped++
			continue
		}
		if !drainKindAllowed(TaskKind(task.Kind), opts.Kinds) {
			report.Skipped++
			continue
		}
		if opts.Limit > 0 && report.Drained >= opts.Limit {
			report.Skipped++
			continue
		}
		result := drainTask(ctx, store, task, opts, now)
		report.Drained++
		report.Results = append(report.Results, result)
	}
	return report, nil
}

func drainTaskIDAllowed(id string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, candidate := range allowed {
		if id == candidate {
			return true
		}
	}
	return false
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
		action := "plan_model_artifact_stage"
		var err error
		if opts.ArtifactStager != nil {
			action = "stage_model_artifact"
			err = executeModelArtifactStage(ctx, store, opts.ArtifactStager, running, now)
		} else {
			err = planModelArtifactStage(ctx, store, running, now)
		}
		if err != nil {
			failed := transitionTask(running, domain.ReconciliationTaskFailed, err.Error(), "failed to "+strings.ReplaceAll(action, "_", " "), now)
			_ = store.SaveReconciliationTask(ctx, failed)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, NodeID: task.NodeID, PresetID: task.PresetID, Error: err.Error()}
		}
		if opts.KeepStageDemandEligible {
			eligible := transitionTask(running, domain.ReconciliationTaskAllowed, "", "staged model artifact; demand remains eligible for autoscale", now)
			eligible.Kind = string(TaskPrewarmReplica)
			eligible.Next = "retry autoscale after staged artifact is visible"
			_ = store.SaveReconciliationTask(ctx, eligible)
			return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: eligible.Status, Action: action, NodeID: task.NodeID, PresetID: task.PresetID}
		}
		done := transitionTask(running, domain.ReconciliationTaskDone, "", "planned model artifact staging", now)
		if opts.ArtifactStager != nil {
			done = transitionTask(running, domain.ReconciliationTaskDone, "", "staged model artifact", now)
		}
		_ = store.SaveReconciliationTask(ctx, done)
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: done.Status, Action: action, NodeID: task.NodeID, PresetID: task.PresetID}
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
		Project:             firstNonEmpty(opts.Project, "reconcile-prewarm"),
		Priority:            domain.PriorityBackground,
		SpeedPref:           domain.SpeedThroughput,
		ContextRequest:      modelruntime.EffectiveContextLength(preset),
		ExpectedConcurrency: 1,
		Preemption:          domain.PreemptSoft,
		Status:              domain.JobQueued,
	}
	if task.NodeID != "" {
		job.NodeSelector = map[string]string{domain.LabelNodeID: task.NodeID}
	}
	return opts.Prewarm.PrewarmReplica(ctx, job)
}

func planModelArtifactStage(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, now time.Time) error {
	preset, node, action, plan, err := modelArtifactStagePlan(ctx, store, task, now)
	if err != nil {
		return err
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
		Source:         action.Source,
		ArtifactSizeMB: preset.ArtifactSizeMB,
		Managed:        true,
		Reason:         "planned by reconciliation task " + task.ID,
		UpdatedAt:      now,
	})
}

func executeModelArtifactStage(ctx context.Context, store TaskStore, stager ArtifactStager, task domain.ReconciliationTaskRecord, now time.Time) error {
	if isDiskFloorRefusal(task.Reason) {
		return fmt.Errorf("staging refused by placement disk floor evidence: %s", task.Reason)
	}
	preset, node, action, plan, err := modelArtifactStagePlan(ctx, store, task, now)
	if err != nil {
		return err
	}
	if node.Address == "" {
		return fmt.Errorf("node %q has no reachable address for catalog staging", node.ID)
	}
	action.State = domain.ModelLocalityStaging
	plan.Actions[0] = action
	if err := store.SaveLocalityPlan(ctx, plan); err != nil {
		return err
	}
	locality, err := stager.StageModel(ctx, domain.Peer{ID: node.ID, Addresses: []string{node.Address}, Compute: true}, preset)
	if err != nil {
		action.State = domain.ModelLocalityFailed
		action.Error = err.Error()
		plan.Actions[0] = action
		_ = store.SaveLocalityPlan(ctx, plan)
		_ = store.SaveModelLocality(ctx, domain.ModelLocality{
			ID:             modelLocalityID(node.ID, preset.ID),
			PresetID:       preset.ID,
			NodeID:         node.ID,
			State:          domain.ModelLocalityFailed,
			ModelRef:       preset.ModelRef,
			Source:         action.Source,
			ArtifactSizeMB: preset.ArtifactSizeMB,
			Managed:        true,
			Reason:         err.Error(),
			UpdatedAt:      now,
		})
		return err
	}
	if locality.ID == "" {
		locality.ID = modelLocalityID(node.ID, preset.ID)
	}
	if locality.PresetID == "" {
		locality.PresetID = preset.ID
	}
	if locality.NodeID == "" {
		locality.NodeID = node.ID
	}
	if locality.State == "" {
		locality.State = domain.ModelLocalityReady
	}
	if locality.UpdatedAt.IsZero() {
		locality.UpdatedAt = now
	}
	if err := store.SaveModelLocality(ctx, locality); err != nil {
		return err
	}
	action.State = locality.State
	action.Error = ""
	plan.Actions[0] = action
	return store.SaveLocalityPlan(ctx, plan)
}

func modelArtifactStagePlan(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, now time.Time) (domain.Preset, domain.Node, domain.LocalityAction, domain.LocalityPlan, error) {
	if task.NodeID == "" {
		return domain.Preset{}, domain.Node{}, domain.LocalityAction{}, domain.LocalityPlan{}, fmt.Errorf("stage model artifact task requires node id")
	}
	if task.PresetID == "" {
		return domain.Preset{}, domain.Node{}, domain.LocalityAction{}, domain.LocalityPlan{}, fmt.Errorf("stage model artifact task requires preset id")
	}
	preset, err := store.Preset(ctx, task.PresetID)
	if err != nil {
		return domain.Preset{}, domain.Node{}, domain.LocalityAction{}, domain.LocalityPlan{}, err
	}
	node, err := store.Node(ctx, task.NodeID)
	if err != nil {
		return domain.Preset{}, domain.Node{}, domain.LocalityAction{}, domain.LocalityPlan{}, err
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
	return preset, node, action, plan, nil
}

func isDiskFloorRefusal(reason string) bool {
	return reason == "disk.free" || reason == "disk.free_after_model" || strings.HasPrefix(reason, "disk.")
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
