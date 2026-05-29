package optimizer

import (
	"context"
	"strings"
	"testing"
	"time"

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
	project := domain.Project{ID: "project-a", ContextCap: 16000, AutoApply: true}
	preset := fixtures.MakePreset(fixtures.WithPresetID("large"), fixtures.WithContextLength(16000))
	if err := store.SaveProject(context.Background(), project); err != nil {
		t.Fatalf("SaveProject: %v", err)
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
		Store: store,
		Clock: mocks.NewFakeClock(now),
	}

	records, err := service.EvaluateProject(context.Background(), project)
	if err != nil {
		t.Fatalf("EvaluateProject: %v", err)
	}
	if len(records) != 1 || !records[0].Applied || records[0].RecommendedValue != 6000 {
		t.Fatalf("records = %+v", records)
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
