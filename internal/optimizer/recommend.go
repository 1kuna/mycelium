package optimizer

import (
	"context"
	"fmt"
	"math"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const RecommendationContextCap = "context_cap_recommendation"

type ProjectStats struct {
	ProjectID      string
	AvgTokens      int
	P95Tokens      int
	LifetimeMax    int
	CurrentCap     int
	SharedContexts []int
	ReloadsPerDay  int
}

type Recommendation struct {
	Type           string         `json:"type"`
	ProjectID      string         `json:"project_id"`
	CurrentCap     int            `json:"current_cap"`
	RecommendedCap int            `json:"recommended_cap"`
	Observed       map[string]int `json:"observed"`
	Rationale      string         `json:"rationale"`
}

type RecommendationPolicy struct {
	AvgHeadroom float64
}

type StatsProvider interface {
	Stats(ctx context.Context, projectID string) (ProjectStats, error)
}

type Engine struct {
	Stats  StatsProvider
	Policy RecommendationPolicy
}

func (e Engine) Recommend(ctx context.Context, project domain.Project) ([]domain.TraceStep, error) {
	if e.Stats == nil {
		return nil, fmt.Errorf("optimizer stats provider is not configured")
	}
	stats, err := e.Stats.Stats(ctx, project.ID)
	if err != nil {
		return nil, err
	}
	rec, ok := RecommendContextCap(project, stats, e.Policy)
	if !ok {
		return []domain.TraceStep{{Step: "recommend", Result: "no context shift detected"}}, nil
	}
	return []domain.TraceStep{{
		Step:   RecommendationContextCap,
		Result: fmt.Sprintf("recommend context cap %d", rec.RecommendedCap),
		Data: map[string]any{
			"type":            rec.Type,
			"project_id":      rec.ProjectID,
			"current_cap":     rec.CurrentCap,
			"recommended_cap": rec.RecommendedCap,
			"observed":        rec.Observed,
			"rationale":       rec.Rationale,
		},
	}}, nil
}

func RecommendContextCap(project domain.Project, stats ProjectStats, policy RecommendationPolicy) (Recommendation, bool) {
	currentCap := project.ContextCap
	if currentCap == 0 {
		currentCap = stats.CurrentCap
	}
	if currentCap == 0 || stats.AvgTokens == 0 {
		return Recommendation{}, false
	}
	headroom := policy.AvgHeadroom
	if headroom == 0 {
		headroom = 1.5
	}
	target := int(math.Ceil(float64(stats.AvgTokens) * headroom))
	shared := nearestSharedAtLeast(target, stats.SharedContexts)
	if shared == 0 || shared >= currentCap {
		return Recommendation{}, false
	}
	rec := Recommendation{
		Type:           RecommendationContextCap,
		ProjectID:      project.ID,
		CurrentCap:     currentCap,
		RecommendedCap: shared,
		Observed: map[string]int{
			"avg_tokens":   stats.AvgTokens,
			"p95_tokens":   stats.P95Tokens,
			"lifetime_max": stats.LifetimeMax,
		},
		Rationale: fmt.Sprintf("avg is %d tokens and p95 is %d against cap %d; %d is already a shared context, so using it can collapse presets while larger tail requests continue through reactive requeue", stats.AvgTokens, stats.P95Tokens, currentCap, shared),
	}
	return rec, true
}

func nearestSharedAtLeast(target int, shared []int) int {
	best := 0
	for _, candidate := range shared {
		if candidate >= target && (best == 0 || candidate < best) {
			best = candidate
		}
	}
	return best
}

var _ ports.Optimizer = Engine{}
