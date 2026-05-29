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

type staticStats struct {
	stats ProjectStats
	err   error
}

func (s staticStats) Stats(context.Context, string) (ProjectStats, error) {
	return s.stats, s.err
}
