package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
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
	if !strings.Contains(report.Results[0].Error, "catalog refresh executor is not configured") {
		t.Fatalf("error = %q", report.Results[0].Error)
	}
	failed := store.taskByID("allowed-refresh")
	if failed.Status != domain.ReconciliationTaskFailed || !strings.Contains(failed.Error, "catalog source contract") {
		t.Fatalf("failed task = %+v", failed)
	}
}

func TestDrainAllowedTasksFiltersKindsWithoutMutatingSkippedTasks(t *testing.T) {
	store := &fakeTaskStore{tasks: []domain.ReconciliationTaskRecord{
		taskRecord("allowed-incompat", string(TaskRecordIncompatibility), domain.ReconciliationTaskAllowed),
		taskRecord("allowed-prewarm", string(TaskPrewarmReplica), domain.ReconciliationTaskAllowed),
	}}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{
		Now:   time.Unix(25, 0).UTC(),
		Kinds: []TaskKind{TaskRecordIncompatibility},
	})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Skipped != 1 || len(report.Results) != 1 {
		t.Fatalf("report = %+v", report)
	}
	if report.Results[0].Kind != string(TaskRecordIncompatibility) || report.Results[0].Status != domain.ReconciliationTaskDone {
		t.Fatalf("result = %+v", report.Results[0])
	}
	if prewarm := store.taskByID("allowed-prewarm"); prewarm.Status != domain.ReconciliationTaskAllowed || len(prewarm.Events) != 0 {
		t.Fatalf("filtered prewarm task mutated: %+v", prewarm)
	}
}

func TestDrainAllowedTasksClosesAllowedTaskForTerminalSourceJob(t *testing.T) {
	store := &fakeTaskStore{
		tasks: []domain.ReconciliationTaskRecord{
			taskRecord("allowed-wait", string(TaskWaitCapacity), domain.ReconciliationTaskAllowed),
			taskRecord("allowed-prewarm", string(TaskPrewarmReplica), domain.ReconciliationTaskAllowed),
		},
		jobs: map[string]domain.Job{
			"job-a": {ID: "job-a", Status: domain.JobFailed},
		},
	}
	now := time.Unix(26, 0).UTC()

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{
		Now:   now,
		Limit: 1,
		Kinds: []TaskKind{TaskRecordIncompatibility},
	})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Skipped != 1 || len(report.Results) != 1 {
		t.Fatalf("report = %+v", report)
	}
	if report.Results[0].Action != "source_job_terminal" || report.Results[0].Kind != string(TaskWaitCapacity) {
		t.Fatalf("result = %+v", report.Results[0])
	}
	if done := store.taskByID("allowed-wait"); done.Status != domain.ReconciliationTaskDone || len(done.Events) != 1 || done.Events[0].Message == "" {
		t.Fatalf("done task = %+v", done)
	}
	if prewarm := store.taskByID("allowed-prewarm"); prewarm.Status != domain.ReconciliationTaskAllowed {
		t.Fatalf("limited task was mutated: %+v", prewarm)
	}
}

func TestDrainAllowedTasksLeavesAllowedTaskWhenSourceJobIsActiveOrMissing(t *testing.T) {
	for name, jobs := range map[string]map[string]domain.Job{
		"running": {"job-a": {ID: "job-a", Status: domain.JobRunning}},
		"missing": {},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeTaskStore{
				tasks: []domain.ReconciliationTaskRecord{taskRecord("allowed-wait", string(TaskWaitCapacity), domain.ReconciliationTaskAllowed)},
				jobs:  jobs,
			}
			report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{
				Now:   time.Unix(27, 0).UTC(),
				Kinds: []TaskKind{TaskRecordIncompatibility},
			})
			if err != nil {
				t.Fatalf("DrainAllowedTasks: %v", err)
			}
			if report.Drained != 0 || report.Skipped != 1 {
				t.Fatalf("report = %+v", report)
			}
			if task := store.taskByID("allowed-wait"); task.Status != domain.ReconciliationTaskAllowed || len(task.Events) != 0 {
				t.Fatalf("task mutated = %+v", task)
			}
		})
	}
}

func TestDrainAllowedTasksDoesNotTreatModelServiceIDAsSourceJob(t *testing.T) {
	task := taskRecord("service-wait", string(TaskWaitCapacity), domain.ReconciliationTaskAllowed)
	task.Source = "model_service"
	task.JobID = "service-a"
	store := &fakeTaskStore{
		tasks:  []domain.ReconciliationTaskRecord{task},
		jobErr: errors.New("job lookup should not run"),
	}
	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{
		Now:   time.Unix(27, 30).UTC(),
		Kinds: []TaskKind{TaskRecordIncompatibility},
	})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 0 || report.Skipped != 1 {
		t.Fatalf("report = %+v", report)
	}
	if got := store.taskByID("service-wait"); got.Status != domain.ReconciliationTaskAllowed || len(got.Events) != 0 {
		t.Fatalf("task mutated = %+v", got)
	}
}

func TestDrainAllowedTasksReturnsSourceJobLookupErrors(t *testing.T) {
	store := &fakeTaskStore{
		tasks:  []domain.ReconciliationTaskRecord{taskRecord("allowed-wait", string(TaskWaitCapacity), domain.ReconciliationTaskAllowed)},
		jobErr: errors.New("job store unavailable"),
	}
	if _, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(28, 0).UTC()}); !errors.Is(err, store.jobErr) {
		t.Fatalf("job lookup err = %v", err)
	}
}

func TestDrainAllowedTasksRefreshesCatalogThroughConfiguredSource(t *testing.T) {
	refresher := &fakeCatalogRefresher{}
	store := &fakeTaskStore{tasks: []domain.ReconciliationTaskRecord{
		taskRecord("allowed-refresh", string(TaskRefreshCatalog), domain.ReconciliationTaskAllowed),
	}}
	now := time.Unix(35, 0).UTC()

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: now, Catalog: refresher})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Results[0].Status != domain.ReconciliationTaskDone || report.Results[0].Action != "refresh_catalog" {
		t.Fatalf("report = %+v", report)
	}
	if len(refresher.tasks) != 1 || refresher.tasks[0].ID != "allowed-refresh" || refresher.tasks[0].Status != domain.ReconciliationTaskRunning {
		t.Fatalf("refresher tasks = %+v", refresher.tasks)
	}
	done := store.taskByID("allowed-refresh")
	if done.Status != domain.ReconciliationTaskDone || len(done.Events) != 2 {
		t.Fatalf("done task = %+v", done)
	}
}

func TestConfigCatalogRefresherStoresMatchingPreset(t *testing.T) {
	store := &fakePresetStore{}
	preset := domain.Preset{ID: "preset-a", ModelRef: "model-a", Aliases: []string{"shared-model"}, ContextLength: 4096}
	refresher := ConfigCatalogRefresher{Store: store, Presets: []domain.Preset{
		preset,
		{ID: "other", ModelRef: "other-model"},
	}}

	if err := refresher.RefreshCatalog(context.Background(), domain.ReconciliationTaskRecord{Model: "shared-model"}); err != nil {
		t.Fatalf("RefreshCatalog: %v", err)
	}
	if len(store.presets) != 1 || store.presets[0].ID != preset.ID {
		t.Fatalf("stored presets = %+v", store.presets)
	}
}

func TestConfigCatalogRefresherMaterializesModelSource(t *testing.T) {
	store := &fakePresetStore{}
	model := filepath.Join(t.TempDir(), "tiny.gguf")
	if err := os.WriteFile(model, []byte("weights"), 0o600); err != nil {
		t.Fatalf("write model: %v", err)
	}
	refresher := ConfigCatalogRefresher{Store: store, StoreDir: t.TempDir()}
	task := domain.ReconciliationTaskRecord{
		Model:          "tiny-model",
		ModelSource:    model,
		ContextRequest: 4096,
		Backend:        domain.BackendLlamaCpp,
	}

	if err := refresher.RefreshCatalog(context.Background(), task); err != nil {
		t.Fatalf("RefreshCatalog source: %v", err)
	}
	if len(store.presets) != 1 {
		t.Fatalf("presets = %+v", store.presets)
	}
	got := store.presets[0]
	if got.ModelRef == model || got.Backend != domain.BackendLlamaCpp || got.ContextLength != 4096 || !containsString(got.Aliases, "tiny-model") {
		t.Fatalf("materialized preset = %+v", got)
	}
}

func TestConfigCatalogRefresherMaterializesBackendManagedHFSourceContract(t *testing.T) {
	store := &fakePresetStore{}
	refresher := ConfigCatalogRefresher{Store: store, StoreDir: t.TempDir()}
	task := domain.ReconciliationTaskRecord{
		PresetID:       "vision-vllm",
		Model:          "vision",
		ModelSource:    "hf://owner/repo",
		ContextRequest: 49152,
		Backend:        domain.BackendVLLM,
		ModelSourceContract: &domain.ModelSourceContract{
			Format:        "hf-transformers",
			SizeMB:        24000,
			KVPerTokenMB:  0.25,
			Quant:         "bf16",
			Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityVision},
			EngineProfile: "engine-vllm",
		},
	}

	if err := refresher.RefreshCatalog(context.Background(), task); err != nil {
		t.Fatalf("RefreshCatalog source contract: %v", err)
	}
	if len(store.presets) != 1 {
		t.Fatalf("presets = %+v", store.presets)
	}
	got := store.presets[0]
	if got.ID != "vision-vllm" || got.ModelRef != "owner/repo" || got.Backend != domain.BackendVLLM || got.ContextLength != 49152 {
		t.Fatalf("materialized preset = %+v", got)
	}
	if got.EstWeightsMB != 24000 || got.KVPerTokenMB != 0.25 || got.EngineProfileID != "engine-vllm" || got.EngineReadiness != domain.EngineReadinessReadyProfile {
		t.Fatalf("resource/readiness contract = %+v", got)
	}
	if !containsCapability(got.Capabilities, domain.CapabilityVision) || !containsString(got.Aliases, "vision") {
		t.Fatalf("capabilities/aliases = %+v", got)
	}
}

func TestConfigCatalogRefresherResolvesSourceEngineProfile(t *testing.T) {
	store := &fakePresetStore{profiles: []domain.EngineProfile{{
		ID:              "engine-vllm",
		Backend:         domain.BackendVLLM,
		Ready:           true,
		SupportedModels: []string{"hf-transformers"},
	}}}
	refresher := ConfigCatalogRefresher{Store: store, StoreDir: t.TempDir()}
	task := domain.ReconciliationTaskRecord{
		PresetID:       "vision-vllm",
		Model:          "vision",
		ModelSource:    "hf://owner/repo",
		ContextRequest: 49152,
		Backend:        domain.BackendVLLM,
		ModelSourceContract: &domain.ModelSourceContract{
			Format:       "hf-transformers",
			SizeMB:       24000,
			KVPerTokenMB: 0.25,
			Quant:        "bf16",
		},
	}

	if err := refresher.RefreshCatalog(context.Background(), task); err != nil {
		t.Fatalf("RefreshCatalog source contract: %v", err)
	}
	if len(store.presets) != 1 || store.presets[0].EngineProfileID != "engine-vllm" {
		t.Fatalf("presets = %+v", store.presets)
	}
}

func TestConfigCatalogRefresherRequiresStoreDirForModelSource(t *testing.T) {
	refresher := ConfigCatalogRefresher{Store: &fakePresetStore{}}
	err := refresher.RefreshCatalog(context.Background(), domain.ReconciliationTaskRecord{Model: "tiny", ModelSource: "tiny.gguf"})
	if err == nil || !strings.Contains(err.Error(), "catalog store dir") {
		t.Fatalf("err = %v", err)
	}
}

func TestConfigCatalogRefresherRequiresEngineSourceContractForNonLlamaBackend(t *testing.T) {
	refresher := ConfigCatalogRefresher{Store: &fakePresetStore{}, StoreDir: t.TempDir()}
	err := refresher.RefreshCatalog(context.Background(), domain.ReconciliationTaskRecord{Model: "vision", ModelSource: "hf://owner/repo", Backend: domain.BackendVLLM, ContextRequest: 49152})
	if err == nil || !strings.Contains(err.Error(), "resource contract") {
		t.Fatalf("err = %v", err)
	}
}

func TestConfigCatalogRefresherRequiresEngineProfileListerForSourceResolution(t *testing.T) {
	refresher := ConfigCatalogRefresher{Store: catalogOnlyStore{}, StoreDir: t.TempDir()}
	err := refresher.RefreshCatalog(context.Background(), domain.ReconciliationTaskRecord{
		Model:          "vision",
		ModelSource:    "hf://owner/repo",
		Backend:        domain.BackendVLLM,
		ContextRequest: 49152,
		ModelSourceContract: &domain.ModelSourceContract{
			Format:       "hf-transformers",
			SizeMB:       24000,
			KVPerTokenMB: 0.25,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "catalog store cannot list engine profiles") {
		t.Fatalf("err = %v", err)
	}
}

func TestConfigCatalogRefresherRejectsMissingReadySourceEngineProfile(t *testing.T) {
	store := &fakePresetStore{profiles: []domain.EngineProfile{{
		ID:              "engine-vllm-unready",
		Backend:         domain.BackendVLLM,
		Ready:           false,
		SupportedModels: []string{"hf-transformers"},
	}, {
		ID:              "engine-vllm-gguf",
		Backend:         domain.BackendVLLM,
		Ready:           true,
		SupportedModels: []string{"gguf"},
	}, {
		ID:              "engine-mlx",
		Backend:         domain.BackendMLX,
		Ready:           true,
		SupportedModels: []string{"hf-transformers"},
	}}}
	refresher := ConfigCatalogRefresher{Store: store, StoreDir: t.TempDir()}
	err := refresher.RefreshCatalog(context.Background(), domain.ReconciliationTaskRecord{
		Model:          "vision",
		ModelSource:    "hf://owner/repo",
		Backend:        domain.BackendVLLM,
		ContextRequest: 49152,
		ModelSourceContract: &domain.ModelSourceContract{
			Format:       "hf-transformers",
			SizeMB:       24000,
			KVPerTokenMB: 0.25,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "no saved ready engine profile") {
		t.Fatalf("err = %v", err)
	}
}

func TestConfigCatalogRefresherRejectsAmbiguousSourceEngineProfiles(t *testing.T) {
	store := &fakePresetStore{profiles: []domain.EngineProfile{{
		ID:      "engine-a",
		Backend: domain.BackendVLLM,
		Ready:   true,
	}, {
		ID:      "engine-b",
		Backend: domain.BackendVLLM,
		Ready:   true,
	}}}
	refresher := ConfigCatalogRefresher{Store: store, StoreDir: t.TempDir()}
	err := refresher.RefreshCatalog(context.Background(), domain.ReconciliationTaskRecord{
		Model:          "vision",
		ModelSource:    "hf://owner/repo",
		Backend:        domain.BackendVLLM,
		ContextRequest: 49152,
		ModelSourceContract: &domain.ModelSourceContract{
			Format:       "hf-transformers",
			SizeMB:       24000,
			KVPerTokenMB: 0.25,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "multiple saved ready engine profiles") {
		t.Fatalf("err = %v", err)
	}
}

func TestConfigCatalogRefresherFailsWhenSourceCannotSatisfyTask(t *testing.T) {
	refresher := ConfigCatalogRefresher{
		Store:   &fakePresetStore{},
		Presets: []domain.Preset{{ID: "preset-a", ModelRef: "model-a"}},
	}
	err := refresher.RefreshCatalog(context.Background(), domain.ReconciliationTaskRecord{PresetID: "missing"})
	if err == nil || !strings.Contains(err.Error(), `no preset matching "missing"`) {
		t.Fatalf("RefreshCatalog err = %v", err)
	}
}

func TestDrainAllowedTasksFailsCatalogRefreshActionFailure(t *testing.T) {
	store := &fakeTaskStore{tasks: []domain.ReconciliationTaskRecord{
		taskRecord("allowed-refresh", string(TaskRefreshCatalog), domain.ReconciliationTaskAllowed),
	}}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{
		Now:     time.Unix(36, 0).UTC(),
		Catalog: &fakeCatalogRefresher{err: errors.New("catalog source unavailable")},
	})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Results[0].Status != domain.ReconciliationTaskFailed || report.Results[0].Error != "catalog source unavailable" {
		t.Fatalf("report = %+v", report)
	}
	failed := store.taskByID("allowed-refresh")
	if failed.Status != domain.ReconciliationTaskFailed || failed.Error != "catalog source unavailable" {
		t.Fatalf("failed task = %+v", failed)
	}
}

func TestDrainAllowedTasksStagesArtifactThroughExecutor(t *testing.T) {
	store := &fakeTaskStore{
		tasks:   []domain.ReconciliationTaskRecord{taskRecord("allowed-stage", string(TaskStageModelArtifact), domain.ReconciliationTaskAllowed)},
		presets: map[string]domain.Preset{"preset-a": {ID: "preset-a", ModelRef: "model-a", ArtifactSizeMB: 123}},
		nodes:   map[string]domain.Node{"node-a": {ID: "node-a", Address: "http://node-a.test"}},
	}
	stager := &fakeStageExecutor{locality: domain.ModelLocality{
		ID:             "node-a:preset-a",
		PresetID:       "preset-a",
		NodeID:         "node-a",
		State:          domain.ModelLocalityReady,
		ModelRef:       "model-a",
		ArtifactSizeMB: 123,
		Managed:        true,
		Reason:         "catalog stage committed",
		UpdatedAt:      time.Unix(51, 0).UTC(),
	}}
	now := time.Unix(50, 0).UTC()

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: now, Stage: stager})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Results[0].Status != domain.ReconciliationTaskDone || report.Results[0].Action != "stage_model_artifact" {
		t.Fatalf("report = %+v", report)
	}
	if stager.calls != 1 || stager.peer.ID != "node-a" || stager.preset.ID != "preset-a" {
		t.Fatalf("stager call count=%d peer=%+v preset=%+v", stager.calls, stager.peer, stager.preset)
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
	if action.Kind != domain.LocalityActionStage || action.PresetID != "preset-a" || action.NodeID != "node-a" || action.State != domain.ModelLocalityReady || action.ArtifactSizeMB != 123 {
		t.Fatalf("action = %+v", action)
	}
	if len(store.localities) != 1 || store.localities[0].ID != "node-a:preset-a" || store.localities[0].State != domain.ModelLocalityReady || !store.localities[0].Managed {
		t.Fatalf("localities = %+v", store.localities)
	}
}

func TestDrainAllowedTasksFailsStageWithoutExecutorAfterRecordingPlan(t *testing.T) {
	store := &fakeTaskStore{
		tasks:   []domain.ReconciliationTaskRecord{taskRecord("allowed-stage", string(TaskStageModelArtifact), domain.ReconciliationTaskAllowed)},
		presets: map[string]domain.Preset{"preset-a": {ID: "preset-a", ModelRef: "model-a", ArtifactSizeMB: 123}},
		nodes:   map[string]domain.Node{"node-a": {ID: "node-a", Address: "http://node-a.test"}},
	}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(55, 0).UTC()})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Results[0].Status != domain.ReconciliationTaskFailed || !strings.Contains(report.Results[0].Error, "stage model artifact executor is not configured") {
		t.Fatalf("report = %+v", report)
	}
	failed := store.taskByID("allowed-stage")
	if failed.Status != domain.ReconciliationTaskFailed {
		t.Fatalf("failed task = %+v", failed)
	}
	if len(store.localityPlans) != 1 || store.localityPlans[0].Actions[0].State != domain.ModelLocalityPlanned {
		t.Fatalf("locality plan = %+v", store.localityPlans)
	}
	if len(store.localities) != 1 || store.localities[0].State != domain.ModelLocalityPlanned {
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

func TestDrainAllowedTasksRepairsWaitCapacityThroughQueueDrain(t *testing.T) {
	capacity := &fakeCapacityExecutor{result: CapacityRepairResult{Drained: 1, WorkloadIDs: []string{"job-moved"}}}
	store := &fakeTaskStore{tasks: []domain.ReconciliationTaskRecord{
		taskRecord("allowed-wait", string(TaskWaitCapacity), domain.ReconciliationTaskAllowed),
	}}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(85, 0).UTC(), Capacity: capacity, Limit: 2})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if capacity.limit != 2 {
		t.Fatalf("capacity repair limit = %d, want 2", capacity.limit)
	}
	if report.Drained != 1 || report.Results[0].Status != domain.ReconciliationTaskDone || report.Results[0].Action != "wait_capacity" || report.Results[0].WorkloadID != "job-moved" {
		t.Fatalf("report = %+v", report)
	}
	if done := store.taskByID("allowed-wait"); done.Status != domain.ReconciliationTaskDone || done.Blocker != "" {
		t.Fatalf("done task = %+v", done)
	}
}

func TestDrainAllowedTasksBlocksWaitCapacityWhenQueueStillCannotRun(t *testing.T) {
	capacity := &fakeCapacityExecutor{result: CapacityRepairResult{}}
	store := &fakeTaskStore{tasks: []domain.ReconciliationTaskRecord{
		taskRecord("allowed-wait", string(TaskWaitCapacity), domain.ReconciliationTaskAllowed),
	}}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(86, 0).UTC(), Capacity: capacity})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Results[0].Status != domain.ReconciliationTaskBlocked || report.Results[0].Action != "wait_capacity" || report.Results[0].Error != "capacity_unavailable" {
		t.Fatalf("report = %+v", report)
	}
	blocked := store.taskByID("allowed-wait")
	if blocked.Status != domain.ReconciliationTaskBlocked || blocked.Blocker != "capacity_unavailable" || blocked.Error != "" {
		t.Fatalf("blocked task = %+v", blocked)
	}
}

func TestDrainAllowedTasksFailsWaitCapacityWithoutExecutor(t *testing.T) {
	store := &fakeTaskStore{tasks: []domain.ReconciliationTaskRecord{
		taskRecord("allowed-wait", string(TaskWaitCapacity), domain.ReconciliationTaskAllowed),
	}}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(87, 0).UTC()})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Results[0].Status != domain.ReconciliationTaskFailed || !strings.Contains(report.Results[0].Error, "capacity repair executor is not configured") {
		t.Fatalf("report = %+v", report)
	}
	if failed := store.taskByID("allowed-wait"); failed.Status != domain.ReconciliationTaskFailed || !strings.Contains(failed.Error, "capacity repair executor") {
		t.Fatalf("failed task = %+v", failed)
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

func TestDrainAllowedTasksExecutesPrewarmReplicaThroughExecutor(t *testing.T) {
	prewarm := &fakePrewarmExecutor{result: PrewarmResult{
		WorkloadID: "reconcile-prewarm-allowed-prewarm",
		NodeID:     "node-a",
		InstanceID: "inst-a",
		LeaseID:    "lease-a",
	}}
	store := &fakeTaskStore{
		tasks: []domain.ReconciliationTaskRecord{taskRecord("allowed-prewarm", string(TaskPrewarmReplica), domain.ReconciliationTaskAllowed)},
		presets: map[string]domain.Preset{"preset-a": {
			ID:            "preset-a",
			ModelRef:      "model-ref",
			Aliases:       []string{"model-alias"},
			ContextLength: 4096,
		}},
	}
	store.tasks[0].ContextRequest = 8192
	store.tasks[0].ExpectedConcurrency = 3
	store.tasks[0].Project = "service-project"

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{
		Now:     time.Unix(90, 0).UTC(),
		Project: "operator-reconcile",
		Prewarm: prewarm,
	})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Drained != 1 || report.Results[0].Status != domain.ReconciliationTaskDone || report.Results[0].Action != "prewarm_replica" {
		t.Fatalf("report = %+v", report)
	}
	if report.Results[0].WorkloadID != "reconcile-prewarm-allowed-prewarm" || report.Results[0].InstanceID != "inst-a" || report.Results[0].LeaseID != "lease-a" {
		t.Fatalf("result evidence = %+v", report.Results[0])
	}
	if len(prewarm.jobs) != 1 {
		t.Fatalf("prewarm jobs = %+v", prewarm.jobs)
	}
	job := prewarm.jobs[0]
	if job.ID != "reconcile-prewarm-allowed-prewarm" || job.Model != "model-a" || job.PresetID != "preset-a" || job.Project != "service-project" || job.ContextRequest != 8192 || job.ExpectedConcurrency != 3 {
		t.Fatalf("prewarm job = %+v", job)
	}
	if got := job.NodeSelector[domain.LabelNodeID]; got != "node-a" {
		t.Fatalf("node selector = %+v", job.NodeSelector)
	}
	done := store.taskByID("allowed-prewarm")
	if done.Status != domain.ReconciliationTaskDone || len(done.Events) != 2 {
		t.Fatalf("done task = %+v", done)
	}
}

func TestDrainAllowedTasksFailsPrewarmWithoutExecutor(t *testing.T) {
	store := &fakeTaskStore{
		tasks:   []domain.ReconciliationTaskRecord{taskRecord("allowed-prewarm", string(TaskPrewarmReplica), domain.ReconciliationTaskAllowed)},
		presets: map[string]domain.Preset{"preset-a": {ID: "preset-a", ModelRef: "model-ref"}},
	}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{Now: time.Unix(100, 0).UTC()})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Results[0].Status != domain.ReconciliationTaskFailed || !strings.Contains(report.Results[0].Error, "prewarm executor is not configured") {
		t.Fatalf("report = %+v", report)
	}
	failed := store.taskByID("allowed-prewarm")
	if failed.Status != domain.ReconciliationTaskFailed || !strings.Contains(failed.Error, "workload admission endpoint") {
		t.Fatalf("failed task = %+v", failed)
	}
}

func TestDrainAllowedTasksFailsPrewarmActionFailure(t *testing.T) {
	store := &fakeTaskStore{
		tasks:   []domain.ReconciliationTaskRecord{taskRecord("allowed-prewarm", string(TaskPrewarmReplica), domain.ReconciliationTaskAllowed)},
		presets: map[string]domain.Preset{"preset-a": {ID: "preset-a", ModelRef: "model-ref"}},
	}

	report, err := DrainAllowedTasks(context.Background(), store, DrainOptions{
		Now:     time.Unix(110, 0).UTC(),
		Prewarm: &fakePrewarmExecutor{err: errors.New("owner admission rejected")},
	})
	if err != nil {
		t.Fatalf("DrainAllowedTasks: %v", err)
	}
	if report.Results[0].Status != domain.ReconciliationTaskFailed || report.Results[0].Error != "owner admission rejected" {
		t.Fatalf("report = %+v", report)
	}
	if failed := store.taskByID("allowed-prewarm"); failed.Status != domain.ReconciliationTaskFailed || failed.Error != "owner admission rejected" {
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

type fakePrewarmExecutor struct {
	jobs   []domain.Job
	result PrewarmResult
	err    error
}

func (e *fakePrewarmExecutor) PrewarmReplica(_ context.Context, job domain.Job) (PrewarmResult, error) {
	e.jobs = append(e.jobs, job)
	if e.err != nil {
		return PrewarmResult{}, e.err
	}
	return e.result, nil
}

type fakeCapacityExecutor struct {
	limit  int
	result CapacityRepairResult
	err    error
}

func (e *fakeCapacityExecutor) RepairCapacity(_ context.Context, limit int) (CapacityRepairResult, error) {
	e.limit = limit
	if e.err != nil {
		return CapacityRepairResult{}, e.err
	}
	return e.result, nil
}

type fakeStageExecutor struct {
	calls    int
	peer     domain.Peer
	preset   domain.Preset
	locality domain.ModelLocality
	err      error
}

func (e *fakeStageExecutor) StageModel(_ context.Context, peer domain.Peer, preset domain.Preset) (domain.ModelLocality, error) {
	e.calls++
	e.peer = peer
	e.preset = preset
	if e.err != nil {
		return domain.ModelLocality{}, e.err
	}
	return e.locality, nil
}

type fakeCatalogRefresher struct {
	tasks []domain.ReconciliationTaskRecord
	err   error
}

func (r *fakeCatalogRefresher) RefreshCatalog(_ context.Context, task domain.ReconciliationTaskRecord) error {
	r.tasks = append(r.tasks, task)
	return r.err
}

type fakePresetStore struct {
	presets  []domain.Preset
	profiles []domain.EngineProfile
}

func (s *fakePresetStore) SavePreset(_ context.Context, preset domain.Preset) error {
	s.presets = append(s.presets, preset)
	return nil
}

func (s *fakePresetStore) ListEngineProfiles(context.Context) ([]domain.EngineProfile, error) {
	return append([]domain.EngineProfile(nil), s.profiles...), nil
}

type catalogOnlyStore struct{}

func (catalogOnlyStore) SavePreset(context.Context, domain.Preset) error {
	return nil
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

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsCapability(values []domain.Capability, want domain.Capability) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

type fakeTaskStore struct {
	tasks             []domain.ReconciliationTaskRecord
	incompatibilities []domain.IncompatibilityRecord
	presets           map[string]domain.Preset
	nodes             map[string]domain.Node
	localityPlans     []domain.LocalityPlan
	localities        []domain.ModelLocality
	profiles          []domain.EngineProfile
	jobs              map[string]domain.Job
	jobErr            error
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

func (s *fakeTaskStore) Job(_ context.Context, id string) (domain.Job, error) {
	if s.jobErr != nil {
		return domain.Job{}, s.jobErr
	}
	job, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, sql.ErrNoRows
	}
	return job, nil
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
