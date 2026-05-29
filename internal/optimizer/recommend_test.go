package optimizer

import (
	"context"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
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

type staticStats struct {
	stats ProjectStats
	err   error
}

func (s staticStats) Stats(context.Context, string) (ProjectStats, error) {
	return s.stats, s.err
}
