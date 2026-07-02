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
		taskRecord("allowed-refresh", string(TaskRefreshCatalog), domain.ReconciliationTaskAllowed),
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
	failed := store.taskByID("allowed-refresh")
	if failed.Status != domain.ReconciliationTaskFailed || !strings.Contains(failed.Error, "not implemented") {
		t.Fatalf("failed task = %+v", failed)
	}
}

func TestDrainAllowedTasksPlansArtifactStaging(t *testing.T) {
	store := &fakeTaskStore{
		tasks:   []domain.ReconciliationTaskRecord{taskRecord("allowed-stage", string(TaskStageModelArtifact), domain.ReconciliationTaskAllowed)},
		presets: map[string]domain.Preset{"preset-a": {ID: "preset-a", ModelRef: "model-a", ArtifactSizeMB: 123}},
		nodes:   map[string]domain.Node{"node-a": {ID: "node-a"}},
	}
	now := time.Unix(50, 0).UTC()

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: now})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Results[0].Status != domain.ReconciliationTaskDone || report.Results[0].Action != "plan_model_artifact_stage" {
		t.Fatalf("report = %+v", report)
	}
	done := store.taskByID("allowed-stage")
	if done.Status != domain.ReconciliationTaskDone || len(done.Events) != 2 {
		t.Fatalf("done task = %+v", done)
	}
	if len(store.localityPlans) != 1 {
		t.Fatalf("locality plans = %+v", store.localityPlans)
	}
	plan := store.localityPlans[0]
	if plan.ID != "reconcile-allowed-stage" || len(plan.Actions) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
	action := plan.Actions[0]
	if action.Kind != domain.LocalityActionStage || action.PresetID != "preset-a" || action.NodeID != "node-a" || action.State != domain.ModelLocalityPlanned || action.ArtifactSizeMB != 123 {
		t.Fatalf("action = %+v", action)
	}
	if len(store.localities) != 1 || store.localities[0].ID != "node-a:preset-a" || store.localities[0].State != domain.ModelLocalityPlanned || !store.localities[0].Managed {
		t.Fatalf("localities = %+v", store.localities)
	}
}

func TestDrainAllowedTasksFailsStageWithoutPreset(t *testing.T) {
	store := &fakeTaskStore{
		tasks: []domain.ReconciliationTaskRecord{taskRecord("allowed-stage", string(TaskStageModelArtifact), domain.ReconciliationTaskAllowed)},
		nodes: map[string]domain.Node{"node-a": {ID: "node-a"}},
	}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(60, 0).UTC()})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Results[0].Status != domain.ReconciliationTaskFailed || !strings.Contains(report.Results[0].Error, "preset") {
		t.Fatalf("report = %+v", report)
	}
	if len(store.localityPlans) != 0 || len(store.localities) != 0 {
		t.Fatalf("stage side effects on failure: plans=%+v localities=%+v", store.localityPlans, store.localities)
	}
}

func TestDrainAllowedTasksVerifiesEngineReadiness(t *testing.T) {
	store := &fakeTaskStore{
		tasks: []domain.ReconciliationTaskRecord{taskRecord("allowed-engine", string(TaskVerifyEngineReadiness), domain.ReconciliationTaskAllowed)},
		presets: map[string]domain.Preset{"preset-a": {
			ID:       "preset-a",
			ModelRef: "model-a",
			Backend:  domain.BackendVLLM,
		}},
		nodes: map[string]domain.Node{"node-a": {ID: "node-a", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}},
		profiles: []domain.EngineProfile{{
			ID:          "engine-vllm",
			Backend:     domain.BackendVLLM,
			DisplayName: "vLLM",
			Version:     "1",
			CompatibilityKey: domain.CompatibilityKey{
				OS:                 "linux",
				CPUArch:            "amd64",
				AcceleratorVendor:  "nvidia",
				AcceleratorRuntime: "cuda",
				EngineDistribution: "vLLM",
				EngineVersion:      "1",
				Backend:            domain.BackendVLLM,
			},
			Ready: true,
		}},
	}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(70, 0).UTC()})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Results[0].Status != domain.ReconciliationTaskDone || report.Results[0].Action != "verify_engine_readiness" {
		t.Fatalf("report = %+v", report)
	}
	done := store.taskByID("allowed-engine")
	if done.Status != domain.ReconciliationTaskDone || len(done.Events) != 2 {
		t.Fatalf("done task = %+v", done)
	}
}

func TestDrainAllowedTasksFailsEngineReadinessWithoutSavedProfile(t *testing.T) {
	store := &fakeTaskStore{
		tasks: []domain.ReconciliationTaskRecord{taskRecord("allowed-engine", string(TaskVerifyEngineReadiness), domain.ReconciliationTaskAllowed)},
		presets: map[string]domain.Preset{"preset-a": {
			ID:       "preset-a",
			ModelRef: "model-a",
			Backend:  domain.BackendVLLM,
		}},
		nodes: map[string]domain.Node{"node-a": {ID: "node-a", OS: "linux", Arch: "amd64", Accelerators: []domain.Accelerator{{Vendor: "nvidia"}}}},
	}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(80, 0).UTC()})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Results[0].Status != domain.ReconciliationTaskFailed || !strings.Contains(report.Results[0].Error, "no saved ready engine profile") {
		t.Fatalf("report = %+v", report)
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
	presets           map[string]domain.Preset
	nodes             map[string]domain.Node
	localityPlans     []domain.LocalityPlan
	localities        []domain.ModelLocality
	profiles          []domain.EngineProfile
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

func (s *fakeTaskStore) Preset(_ context.Context, id string) (domain.Preset, error) {
	if preset, ok := s.presets[id]; ok {
		return preset, nil
	}
	return domain.Preset{}, errors.New("preset not found")
}

func (s *fakeTaskStore) Node(_ context.Context, id string) (domain.Node, error) {
	if node, ok := s.nodes[id]; ok {
		return node, nil
	}
	return domain.Node{}, errors.New("node not found")
}

func (s *fakeTaskStore) SaveLocalityPlan(_ context.Context, plan domain.LocalityPlan) error {
	for i := range s.localityPlans {
		if s.localityPlans[i].ID == plan.ID {
			s.localityPlans[i] = plan
			return nil
		}
	}
	s.localityPlans = append(s.localityPlans, plan)
	return nil
}

func (s *fakeTaskStore) SaveModelLocality(_ context.Context, locality domain.ModelLocality) error {
	for i := range s.localities {
		if s.localities[i].ID == locality.ID {
			s.localities[i] = locality
			return nil
		}
	}
	s.localities = append(s.localities, locality)
	return nil
}

func (s *fakeTaskStore) ListEngineProfiles(context.Context) ([]domain.EngineProfile, error) {
	return append([]domain.EngineProfile(nil), s.profiles...), nil
}

func (s *fakeTaskStore) SaveEngineProfile(_ context.Context, profile domain.EngineProfile) error {
	for i := range s.profiles {
		if s.profiles[i].ID == profile.ID {
			s.profiles[i] = profile
			return nil
		}
	}
	s.profiles = append(s.profiles, profile)
	return nil
}

func (s *fakeTaskStore) MarkEngineProfileUnready(_ context.Context, profileID, reason string) error {
	for i := range s.profiles {
		if s.profiles[i].ID == profileID {
			s.profiles[i].Ready = false
			s.profiles[i].UnreadyReason = reason
			return nil
		}
	}
	return errors.New("engine profile not found")
}

func (s *fakeTaskStore) taskByID(id string) domain.ReconciliationTaskRecord {
	for _, task := range s.tasks {
		if task.ID == id {
			return task
		}
	}
	return domain.ReconciliationTaskRecord{}
}
