package optimizer

import (
	"context"
	"strings"
	"testing"
	"time"

	"mycelium/internal/bench"
	"mycelium/internal/domain"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestRecommendContextCapChoosesSharedContextWithRationale(t *testing.T) {
	project := domain.Project{ID: "project-a", ContextCap: 16000}
	stats := ProjectStats{
		ProjectID:      project.ID,
		AvgTokens:      4000,
		P95Tokens:      12000,
		LifetimeMax:    16000,
		CurrentCap:     16000,
		SharedContexts: []int{6000, 12000},
		ReloadsPerDay:  14,
	}
	rec, ok := RecommendContextCap(project, stats, RecommendationPolicy{})
	if !ok {
		t.Fatal("expected recommendation")
	}
	if rec.Type != RecommendationContextCap || rec.RecommendedCap != 6000 {
		t.Fatalf("rec = %+v", rec)
	}
	if rec.Observed["avg_tokens"] != 4000 || rec.Observed["p95_tokens"] != 12000 || !strings.Contains(rec.Rationale, "shared context") {
		t.Fatalf("rec = %+v", rec)
	}
}

func TestEngineRecommendReturnsTraceStep(t *testing.T) {
	engine := Engine{Stats: staticStats{stats: ProjectStats{
		ProjectID:      "project-a",
		AvgTokens:      4000,
		P95Tokens:      12000,
		CurrentCap:     16000,
		SharedContexts: []int{6000},
	}}}
	steps, err := engine.Recommend(context.Background(), domain.Project{ID: "project-a", ContextCap: 16000})
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	if len(steps) != 1 || steps[0].Step != RecommendationContextCap || steps[0].Data["recommended_cap"].(int) != 6000 {
		t.Fatalf("steps = %+v", steps)
	}
}

func TestApplyRecommendationRespectsAutoApplyGate(t *testing.T) {
	project := domain.Project{ID: "project-a", ContextCap: 16000}
	preset := fixtures.MakePreset(fixtures.WithContextLength(16000))
	rec := Recommendation{Type: RecommendationContextCap, ProjectID: project.ID, RecommendedCap: 6000}

	skipped := ApplyRecommendation(project, preset, rec)
	if skipped.Applied || skipped.Project.ContextCap != 16000 || skipped.Preset.ContextLength != 16000 {
		t.Fatalf("skipped = %+v", skipped)
	}
	project.AutoApply = true
	applied := ApplyRecommendation(project, preset, rec)
	if !applied.Applied || applied.Project.ContextCap != 6000 || applied.Preset.ContextLength != 6000 {
		t.Fatalf("applied = %+v", applied)
	}
	if !strings.Contains(applied.Log.Result, "applied") {
		t.Fatalf("log = %+v", applied.Log)
	}
}

func TestConsolidationCostRecommendsCollapse(t *testing.T) {
	left := fixtures.MakePreset(fixtures.WithContextLength(4096), fixtures.WithKVPerToken(0.01))
	right := fixtures.MakePreset(fixtures.WithPresetID("preset_b"), fixtures.WithContextLength(6000), fixtures.WithKVPerToken(0.01))
	got, err := EvaluateConsolidation(ConsolidationInput{
		Left:          left,
		Right:         right,
		SharedContext: 6000,
		DailyReloads:  14,
	})
	if err != nil {
		t.Fatalf("EvaluateConsolidation: %v", err)
	}
	if !got.ShouldCollapse || got.Savings <= 100 {
		t.Fatalf("got = %+v", got)
	}
	if !strings.Contains(got.Rationale, "reload_cost=140.00") {
		t.Fatalf("rationale = %s", got.Rationale)
	}
}

func TestTelemetryStatsProviderUsesStoredMetricsAndPresetContexts(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if err := store.SavePreset(context.Background(), fixtures.MakePreset(fixtures.WithPresetID("small"), fixtures.WithContextLength(6000))); err != nil {
		t.Fatalf("SavePreset small: %v", err)
	}
	if err := store.SavePreset(context.Background(), fixtures.MakePreset(fixtures.WithPresetID("large"), fixtures.WithContextLength(16000))); err != nil {
		t.Fatalf("SavePreset large: %v", err)
	}
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	for _, metric := range []domain.RunMetric{
		{JobID: "job-a", Project: "project-a", ContextUsed: 3500, At: now},
		{JobID: "job-b", Project: "project-a", ContextUsed: 4000, At: now.Add(time.Second)},
	} {
		if err := store.Record(context.Background(), metric); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	stats, err := (TelemetryStatsProvider{Telemetry: store, Presets: store, CurrentCap: 16000}).Stats(context.Background(), "project-a")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.AvgTokens != 3750 || stats.LifetimeMax != 4000 || len(stats.SharedContexts) != 2 || stats.SharedContexts[0] != 6000 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestRecommendationServicePersistsAndAutoApplies(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	project := domain.Project{ID: "project-a", ContextCap: 16000, ExpectedConcurrency: 3, AutoApply: true}
	preset := fixtures.MakePreset(fixtures.WithPresetID("large"), fixtures.WithContextLength(16000))
	if err := store.SaveProject(context.Background(), project); err != nil {
		t.Fatalf("SaveProject: %v", err)
	}
	if err := store.SaveNode(context.Background(), fixtures.MakeNode(fixtures.WithNodeID("node-a"))); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	if err := store.SavePreset(context.Background(), fixtures.MakePreset(fixtures.WithPresetID("small"), fixtures.WithContextLength(6000))); err != nil {
		t.Fatalf("SavePreset small: %v", err)
	}
	if err := store.SavePreset(context.Background(), preset); err != nil {
		t.Fatalf("SavePreset large: %v", err)
	}
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	for _, metric := range []domain.RunMetric{
		{JobID: "job-a", Project: project.ID, ContextUsed: 3500, At: now},
		{JobID: "job-b", Project: project.ID, ContextUsed: 4000, At: now.Add(time.Second)},
	} {
		if err := store.Record(context.Background(), metric); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
	service := RecommendationService{
		Store:     store,
		Clock:     mocks.NewFakeClock(now),
		Estimator: &mocks.ResourceEstimator{Claim: fixtures.MakeClaim(1, 1)},
	}

	records, err := service.EvaluateProject(context.Background(), project)
	if err != nil {
		t.Fatalf("EvaluateProject: %v", err)
	}
	if len(records) != 1 || !records[0].Applied || records[0].RecommendedValue != 6000 {
		t.Fatalf("records = %+v", records)
	}
	if records[0].Observed["avg_tokens"] != 3750 || records[0].Observed["p95_tokens"] != 3500 {
		t.Fatalf("observed = %+v", records[0].Observed)
	}
	storedRec, err := store.Recommendation(context.Background(), records[0].ID)
	if err != nil {
		t.Fatalf("Recommendation: %v", err)
	}
	if storedRec.Observed["lifetime_max"] != 4000 {
		t.Fatalf("stored observed = %+v", storedRec.Observed)
	}
	estimator := service.Estimator.(*mocks.ResourceEstimator)
	if len(estimator.Calls) != 1 || estimator.Calls[0].Concurrency != 3 {
		t.Fatalf("estimator calls = %+v", estimator.Calls)
	}
	appliedProject, err := store.Project(context.Background(), project.ID)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	appliedPreset, err := store.Preset(context.Background(), preset.ID)
	if err != nil {
		t.Fatalf("Preset: %v", err)
	}
	if appliedProject.ContextCap != 6000 || appliedPreset.ContextLength != 6000 {
		t.Fatalf("project=%+v preset=%+v", appliedProject, appliedPreset)
	}
}

func TestRecommendationServiceRejectsUnfitContextRecommendation(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	project := domain.Project{ID: "project-a", ContextCap: 16000, AutoApply: true}
	large := fixtures.MakePreset(fixtures.WithPresetID("large"), fixtures.WithContextLength(16000))
	mustOptimizer(t, store.SaveProject(context.Background(), project))
	mustOptimizer(t, store.SaveNode(context.Background(), fixtures.MakeNode(fixtures.WithNodeID("tiny"), fixtures.WithVRAM(100))))
	mustOptimizer(t, store.SavePreset(context.Background(), fixtures.MakePreset(fixtures.WithPresetID("small"), fixtures.WithContextLength(6000))))
	mustOptimizer(t, store.SavePreset(context.Background(), large))
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	mustOptimizer(t, store.Record(context.Background(), domain.RunMetric{JobID: "job-a", Project: project.ID, ContextUsed: 3500, At: now}))
	mustOptimizer(t, store.Record(context.Background(), domain.RunMetric{JobID: "job-b", Project: project.ID, ContextUsed: 4000, At: now.Add(time.Second)}))

	records, err := (RecommendationService{Store: store, Clock: mocks.NewFakeClock(now)}).EvaluateProject(context.Background(), project)
	if err != nil {
		t.Fatalf("EvaluateProject: %v", err)
	}
	if len(records) != 1 || !records[0].Rejected || records[0].Applied || !strings.Contains(records[0].RejectReason, "does not fit") {
		t.Fatalf("records = %+v", records)
	}
	appliedProject, err := store.Project(context.Background(), project.ID)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if appliedProject.ContextCap != project.ContextCap {
		t.Fatalf("project should not be auto-applied: %+v", appliedProject)
	}
}

func TestRecommendationServiceRejectsUnprovenLatencyTarget(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	project := domain.Project{ID: "project-a", ContextCap: 16000, LatencyTargetMS: 10, AutoApply: true}
	large := fixtures.MakePreset(fixtures.WithPresetID("large"), fixtures.WithContextLength(16000))
	mustOptimizer(t, store.SaveProject(context.Background(), project))
	mustOptimizer(t, store.SaveNode(context.Background(), fixtures.MakeNode(fixtures.WithNodeID("node-a"))))
	mustOptimizer(t, store.SavePreset(context.Background(), fixtures.MakePreset(fixtures.WithPresetID("small"), fixtures.WithContextLength(6000))))
	mustOptimizer(t, store.SavePreset(context.Background(), large))
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	mustOptimizer(t, store.Record(context.Background(), domain.RunMetric{JobID: "job-a", Project: project.ID, ContextUsed: 3500, TTFTms: 50, At: now}))
	mustOptimizer(t, store.Record(context.Background(), domain.RunMetric{JobID: "job-b", Project: project.ID, ContextUsed: 4000, TTFTms: 60, At: now.Add(time.Second)}))

	records, err := (RecommendationService{Store: store, Clock: mocks.NewFakeClock(now)}).EvaluateProject(context.Background(), project)
	if err != nil {
		t.Fatalf("EvaluateProject: %v", err)
	}
	if len(records) != 1 || !records[0].Rejected || !strings.Contains(records[0].RejectReason, "latency target") || records[0].Observed["p95_ttft_ms"] != 60 {
		t.Fatalf("records = %+v", records)
	}
}

func TestRecommendationServicePersistsEngineRecommendation(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	project := domain.Project{ID: "project-a"}
	slow := fixtures.MakePreset(fixtures.WithPresetID("slow"))
	fast := fixtures.MakePreset(fixtures.WithPresetID("fast"))
	fast.Backend = domain.BackendMLX
	mustOptimizer(t, store.SaveProject(context.Background(), project))
	mustOptimizer(t, store.SaveNode(context.Background(), fixtures.MakeNode(fixtures.WithNodeID("node-a"))))
	mustOptimizer(t, store.SavePreset(context.Background(), slow))
	mustOptimizer(t, store.SavePreset(context.Background(), fast))
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	for _, metric := range []domain.RunMetric{
		{JobID: "slow-a", Project: project.ID, PresetID: slow.ID, Backend: slow.Backend, TokensPerSec: 10, At: now},
		{JobID: "slow-b", Project: project.ID, PresetID: slow.ID, Backend: slow.Backend, TokensPerSec: 10, At: now.Add(time.Second)},
		{JobID: "fast-a", Project: project.ID, PresetID: fast.ID, Backend: fast.Backend, TokensPerSec: 20, At: now.Add(2 * time.Second)},
		{JobID: "fast-b", Project: project.ID, PresetID: fast.ID, Backend: fast.Backend, TokensPerSec: 20, At: now.Add(3 * time.Second)},
	} {
		mustOptimizer(t, store.Record(context.Background(), metric))
	}

	records, err := (RecommendationService{Store: store, Clock: mocks.NewFakeClock(now)}).EvaluateProject(context.Background(), project)
	if err != nil {
		t.Fatalf("EvaluateProject: %v", err)
	}
	if len(records) != 1 || records[0].Type != RecommendationEngineParameter || records[0].RecommendedPresetID != fast.ID || records[0].Applied {
		t.Fatalf("records = %+v", records)
	}
	stored, err := store.Recommendation(context.Background(), records[0].ID)
	if err != nil {
		t.Fatalf("Recommendation: %v", err)
	}
	if stored.RecommendedBackend != domain.BackendMLX || stored.Observed["best_tokens_per_sec"] != 20 {
		t.Fatalf("stored = %+v", stored)
	}
}

func TestCalibrateSpeedClassesFromTelemetry(t *testing.T) {
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	if err := store.SaveNode(context.Background(), node); err != nil {
		t.Fatalf("SaveNode: %v", err)
	}
	for _, metric := range []domain.RunMetric{
		{JobID: "job-a", NodeID: node.ID, Project: "project-a", TokensPerSec: 10, At: now},
		{JobID: "job-b", NodeID: node.ID, Project: "project-a", TokensPerSec: 20, At: now.Add(time.Second)},
		{JobID: "job-c", NodeID: "node-missing", Project: "project-a", TokensPerSec: 100, At: now.Add(2 * time.Second)},
		{JobID: "job-d", NodeID: node.ID, Project: "project-a", At: now.Add(3 * time.Second)},
	} {
		if err := store.Record(context.Background(), metric); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	updated, err := CalibrateSpeedClasses(context.Background(), store, mocks.NewFakeClock(now))
	if err != nil {
		t.Fatalf("CalibrateSpeedClasses: %v", err)
	}
	if len(updated) != 1 || updated[0].SpeedClass.TokensPerSecRef != 15 || updated[0].SpeedClass.Source != "telemetry-calibrated" {
		t.Fatalf("updated = %+v", updated)
	}
	persisted, err := store.Node(context.Background(), node.ID)
	if err != nil {
		t.Fatalf("Node: %v", err)
	}
	if persisted.SpeedClass.TokensPerSecRef != 15 || !persisted.SpeedClass.ProbedAt.Equal(now) {
		t.Fatalf("persisted = %+v", persisted.SpeedClass)
	}
}

func TestRecommendEnginePresetFromTelemetry(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	project := domain.Project{ID: "project-a"}
	slow := fixtures.MakePreset(fixtures.WithPresetID("llama-cpp"), fixtures.WithLaunchProfile("llamacpp-metal"))
	fast := fixtures.MakePreset(fixtures.WithPresetID("mlx"), fixtures.WithLaunchProfile("mlx"), fixtures.WithLaunchArgs("--draft", "2"))
	fast.Backend = domain.BackendMLX
	metrics := []domain.RunMetric{
		{JobID: "a", Project: project.ID, PresetID: slow.ID, Backend: slow.Backend, TokensPerSec: 10, TTFTms: 100, LoadWallClockMS: 1000, At: now},
		{JobID: "b", Project: project.ID, PresetID: slow.ID, Backend: slow.Backend, TokensPerSec: 11, TTFTms: 90, LoadWallClockMS: 900, At: now.Add(time.Second)},
		{JobID: "c", Project: project.ID, PresetID: fast.ID, Backend: fast.Backend, TokensPerSec: 20, TTFTms: 80, LoadWallClockMS: 600, At: now.Add(2 * time.Second)},
		{JobID: "d", Project: project.ID, PresetID: fast.ID, Backend: fast.Backend, TokensPerSec: 22, TTFTms: 70, LoadWallClockMS: 500, At: now.Add(3 * time.Second)},
		{JobID: "other", Project: "other", PresetID: fast.ID, TokensPerSec: 100, At: now.Add(4 * time.Second)},
	}

	rec, ok := RecommendEnginePreset(project, []domain.Preset{slow, fast}, metrics, now, EnginePresetPolicy{})
	if !ok {
		t.Fatal("expected engine recommendation")
	}
	if rec.Type != RecommendationEngineParameter || rec.RecommendedPresetID != fast.ID || rec.RecommendedBackend != domain.BackendMLX {
		t.Fatalf("rec = %+v", rec)
	}
	if rec.Observed["best_tokens_per_sec"] != 21 || rec.Observed["runner_up_tokens_per_sec"] != 10.5 || !strings.Contains(rec.Rationale, "launch_profile=mlx") {
		t.Fatalf("rec = %+v", rec)
	}

	_, ok = RecommendEnginePreset(project, []domain.Preset{slow, fast}, metrics[:3], now, EnginePresetPolicy{})
	if ok {
		t.Fatal("single fast sample should not recommend")
	}
}

func TestRecommendBenchPickUsesOnlyUserJudgment(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	picked := true
	project := domain.Project{ID: "project-a"}
	preset := fixtures.MakePreset(fixtures.WithPresetID("qwen9b"), fixtures.WithModelRef("qwen2.5-9b"))
	results := []bench.Result{{Model: "qwen2.5-9b", TokensPerSec: 44, TTFTms: 12, UserPick: &picked}}

	rec, ok, err := RecommendBenchPick(project, []domain.Preset{preset}, results, now)
	if err != nil {
		t.Fatalf("RecommendBenchPick: %v", err)
	}
	if !ok || rec.RecommendedPresetID != preset.ID || rec.Observed["best_tokens_per_sec"] != 44 || !strings.Contains(rec.Rationale, "user picked") {
		t.Fatalf("rec = %+v ok=%v", rec, ok)
	}

	results = append(results, bench.Result{Model: preset.ID, UserPick: &picked})
	if _, _, err := RecommendBenchPick(project, []domain.Preset{preset}, results, now); err == nil {
		t.Fatal("multiple picks should fail loudly")
	}
}

func TestRecommendationServiceErrorAndNoopPaths(t *testing.T) {
	if _, err := (Engine{}).Recommend(context.Background(), domain.Project{ID: "p"}); err == nil {
		t.Fatal("expected missing stats error")
	}
	if steps, err := (Engine{Stats: staticStats{stats: ProjectStats{ProjectID: "p"}}}).Recommend(context.Background(), domain.Project{ID: "p"}); err != nil || len(steps) != 1 || !strings.Contains(steps[0].Result, "no context") {
		t.Fatalf("noop steps = %+v %v", steps, err)
	}
	if _, ok := RecommendContextCap(domain.Project{ID: "p"}, ProjectStats{CurrentCap: 1000, AvgTokens: 100}, RecommendationPolicy{AvgHeadroom: 2}); ok {
		t.Fatal("recommendation should not grow caps")
	}
	if got := backendReloadCost(domain.BackendCustom); got != 5 {
		t.Fatalf("custom reload cost = %f", got)
	}
	if got := backendReloadCost(domain.BackendVLLM); got != 1 {
		t.Fatalf("vllm reload cost = %f", got)
	}
	if _, err := EvaluateConsolidation(ConsolidationInput{}); err == nil {
		t.Fatal("expected invalid consolidation input")
	}
	if result := ApplyRecommendation(domain.Project{ID: "p", AutoApply: true}, fixtures.MakePreset(), Recommendation{Type: "other"}); result.Applied || !strings.Contains(result.Log.Result, "ignored") {
		t.Fatalf("unsupported apply = %+v", result)
	}
	if _, err := (TelemetryStatsProvider{}).Stats(context.Background(), "p"); err == nil {
		t.Fatal("expected missing telemetry error")
	}
	store, err := storesqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	if _, err := (TelemetryStatsProvider{Telemetry: store}).Stats(context.Background(), "p"); err == nil {
		t.Fatal("expected missing preset source error")
	}
	if _, err := (RecommendationService{}).EvaluateProject(context.Background(), domain.Project{ID: "p"}); err == nil {
		t.Fatal("expected missing store error")
	}
	if _, err := (RecommendationService{Store: store}).EvaluateProject(context.Background(), domain.Project{ID: "p"}); err == nil {
		t.Fatal("expected missing clock error")
	}
	project := domain.Project{ID: "p", ContextCap: 16000, AutoApply: true}
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	mustOptimizer(t, store.Record(context.Background(), domain.RunMetric{JobID: "job-a", Project: project.ID, ContextUsed: 3500, At: now}))
	mustOptimizer(t, store.Record(context.Background(), domain.RunMetric{JobID: "job-b", Project: project.ID, ContextUsed: 4000, At: now.Add(time.Second)}))
	if records, err := (RecommendationService{Store: store, Clock: mocks.NewFakeClock(now)}).EvaluateProject(context.Background(), project); err != nil || len(records) != 0 {
		t.Fatalf("no-preset records = %+v %v", records, err)
	}
	if _, err := CalibrateSpeedClasses(context.Background(), nil, mocks.NewFakeClock(now)); err == nil {
		t.Fatal("expected missing calibration store error")
	}
	if _, err := CalibrateSpeedClasses(context.Background(), store, nil); err == nil {
		t.Fatal("expected missing calibration clock error")
	}
	if _, ok := selectProjectPreset(domain.Project{}, nil); ok {
		t.Fatal("empty preset source selected")
	}
}

type staticStats struct {
	stats ProjectStats
	err   error
}

func (s staticStats) Stats(context.Context, string) (ProjectStats, error) {
	return s.stats, s.err
}

func mustOptimizer(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
