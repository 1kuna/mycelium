package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"mycelium/internal/domain"
)

type TaskStore interface {
	ListReconciliationTasks(ctx context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error)
	SaveReconciliationTask(ctx context.Context, task domain.ReconciliationTaskRecord) error
	SaveIncompatibility(ctx context.Context, rec domain.IncompatibilityRecord) error
	Preset(ctx context.Context, id string) (domain.Preset, error)
	Node(ctx context.Context, id string) (domain.Node, error)
	SaveLocalityPlan(ctx context.Context, plan domain.LocalityPlan) error
	SaveModelLocality(ctx context.Context, locality domain.ModelLocality) error
}

type DrainOptions struct {
	JobID string
	Limit int
	Now   time.Time
}

type DrainReport struct {
	Drained int               `json:"drained"`
	Skipped int               `json:"skipped"`
	Results []DrainTaskResult `json:"results,omitempty"`
}

type DrainTaskResult struct {
	TaskID string                          `json:"task_id"`
	Kind   string                          `json:"kind"`
	Status domain.ReconciliationTaskStatus `json:"status"`
	Action string                          `json:"action,omitempty"`
	Error  string                          `json:"error,omitempty"`
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
		result := drainTask(ctx, store, task, now)
		report.Drained++
		report.Results = append(report.Results, result)
	}
	return report, nil
}

func drainTask(ctx context.Context, store TaskStore, task domain.ReconciliationTaskRecord, now time.Time) DrainTaskResult {
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
	default:
		err := fmt.Sprintf("reconciliation executor for %q is not implemented", task.Kind)
		failed := transitionTask(running, domain.ReconciliationTaskFailed, err, "executor unavailable", now)
		_ = store.SaveReconciliationTask(ctx, failed)
		return DrainTaskResult{TaskID: task.ID, Kind: task.Kind, Status: failed.Status, Error: err}
	}
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
