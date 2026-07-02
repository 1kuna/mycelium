package reconcile

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
)

func TestDrainAllowedTasksExecutesIncompatibilityAndSkipsBlocked(t *testing.T) {
	store := &fakeTaskStore{tasks: []domain.ReconciliationTaskRecord{
		taskRecord("allowed-incompat", string(TaskRecordIncompatibility), domain.ReconciliationTaskAllowed),
		taskRecord("blocked-repair", string(TaskRepairOwner), domain.ReconciliationTaskBlocked),
	}}
	now := time.Unix(20, 0).UTC()

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: now})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Skipped != 1 || len(report.Results) != 1 {
		t.Fatalf("report = %+v", report)
	}
	if report.Results[0].Status != domain.ReconciliationTaskDone || report.Results[0].Action != "record_incompatibility" {
		t.Fatalf("result = %+v", report.Results[0])
	}
	done := store.taskByID("allowed-incompat")
	if done.Status != domain.ReconciliationTaskDone || done.Error != "" || len(done.Events) != 2 {
		t.Fatalf("done task = %+v", done)
	}
	if done.Events[0].Status != domain.ReconciliationTaskRunning || done.Events[1].Status != domain.ReconciliationTaskDone {
		t.Fatalf("events = %+v", done.Events)
	}
	if len(store.incompatibilities) != 1 || store.incompatibilities[0].TaskID != "allowed-incompat" || store.incompatibilities[0].Reason != "context cap" {
		t.Fatalf("incompatibilities = %+v", store.incompatibilities)
	}
	if blocked := store.taskByID("blocked-repair"); blocked.Status != domain.ReconciliationTaskBlocked || len(blocked.Events) != 0 {
		t.Fatalf("blocked task was mutated: %+v", blocked)
	}

	again, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: now.Add(time.Second)})
	if err != nil {
		t.Fatalf("DrainAllowedTasks again: %v", err)
	}
	if again.Drained != 0 || again.Skipped != 2 || len(store.incompatibilities) != 1 {
		t.Fatalf("idempotent drain report=%+v incompatibilities=%+v", again, store.incompatibilities)
	}
}

func TestDrainAllowedTasksFailsUnsupportedAllowedTaskLoudly(t *testing.T) {
	store := &fakeTaskStore{tasks: []domain.ReconciliationTaskRecord{
		taskRecord("allowed-stage", string(TaskStageModelArtifact), domain.ReconciliationTaskAllowed),
	}}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(30, 0).UTC()})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || len(report.Results) != 1 || report.Results[0].Status != domain.ReconciliationTaskFailed {
		t.Fatalf("report = %+v", report)
	}
	if !strings.Contains(report.Results[0].Error, "not implemented") {
		t.Fatalf("error = %q", report.Results[0].Error)
	}
	failed := store.taskByID("allowed-stage")
	if failed.Status != domain.ReconciliationTaskFailed || !strings.Contains(failed.Error, "not implemented") {
		t.Fatalf("failed task = %+v", failed)
	}
}

func TestDrainAllowedTasksRecordsActionFailure(t *testing.T) {
	store := &fakeTaskStore{
		tasks:           []domain.ReconciliationTaskRecord{taskRecord("allowed-incompat", string(TaskRecordIncompatibility), domain.ReconciliationTaskAllowed)},
		saveIncompatErr: errors.New("store unavailable"),
	}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(40, 0).UTC()})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Results[0].Status != domain.ReconciliationTaskFailed || report.Results[0].Error != "store unavailable" {
		t.Fatalf("report = %+v", report)
	}
	failed := store.taskByID("allowed-incompat")
	if failed.Status != domain.ReconciliationTaskFailed || failed.Error != "store unavailable" {
		t.Fatalf("failed task = %+v", failed)
	}
}

func taskRecord(id, kind string, status domain.ReconciliationTaskStatus) domain.ReconciliationTaskRecord {
	return domain.ReconciliationTaskRecord{
		ID:                 id,
		JobID:              "job-a",
		Kind:               kind,
		Status:             status,
		NodeID:             "node-a",
		PresetID:           "preset-a",
		Model:              "model-a",
		Reason:             "context cap",
		AutomaticCandidate: status == domain.ReconciliationTaskAllowed,
		RequiresApproval:   status == domain.ReconciliationTaskBlocked,
		Next:               "record incompatibility",
		Source:             "placement_decision",
		CreatedAt:          time.Unix(1, 0).UTC(),
		UpdatedAt:          time.Unix(1, 0).UTC(),
	}
}

type fakeTaskStore struct {
	tasks             []domain.ReconciliationTaskRecord
	incompatibilities []domain.IncompatibilityRecord
	saveIncompatErr   error
}

func (s *fakeTaskStore) ListReconciliationTasks(_ context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error) {
	var out []domain.ReconciliationTaskRecord
	for _, task := range s.tasks {
		if jobID == "" || task.JobID == jobID {
			out = append(out, task)
		}
	}
	return out, nil
}

func (s *fakeTaskStore) SaveReconciliationTask(_ context.Context, task domain.ReconciliationTaskRecord) error {
	for i := range s.tasks {
		if s.tasks[i].ID == task.ID {
			s.tasks[i] = task
			return nil
		}
	}
	s.tasks = append(s.tasks, task)
	return nil
}

func (s *fakeTaskStore) SaveIncompatibility(_ context.Context, rec domain.IncompatibilityRecord) error {
	if s.saveIncompatErr != nil {
		return s.saveIncompatErr
	}
	for i := range s.incompatibilities {
		if s.incompatibilities[i].ID == rec.ID {
			s.incompatibilities[i] = rec
			return nil
		}
	}
	s.incompatibilities = append(s.incompatibilities, rec)
	return nil
}

func (s *fakeTaskStore) taskByID(id string) domain.ReconciliationTaskRecord {
	for _, task := range s.tasks {
		if task.ID == id {
			return task
		}
	}
	return domain.ReconciliationTaskRecord{}
}
