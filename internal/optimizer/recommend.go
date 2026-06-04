package optimizer

import (
	"context"
	"fmt"
	"math"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const RecommendationContextCap = "context_cap_recommendation"

const (
	defaultContextCapMinSamples = 20
	defaultContextCapMinWindow  = 24 * time.Hour
)

type ProjectStats struct {
	ProjectID      string
	SampleCount    int
	WindowSeconds  int
	AvgTokens      int
	P95Tokens      int
	LifetimeMax    int
	CurrentCap     int
	SharedContexts []int
	ReloadsPerDay  int
	P95TTFTMS      int
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
	P95Headroom float64
	MinSamples  int
	MinWindow   time.Duration
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
	minSamples := policy.MinSamples
	if minSamples == 0 {
		minSamples = defaultContextCapMinSamples
	}
	if stats.SampleCount < minSamples {
		return Recommendation{}, false
	}
	minWindow := policy.MinWindow
	if minWindow == 0 {
		minWindow = defaultContextCapMinWindow
	}
	if time.Duration(stats.WindowSeconds)*time.Second < minWindow {
		return Recommendation{}, false
	}
	headroom := policy.AvgHeadroom
	if headroom == 0 {
		headroom = 1.5
	}
	p95Headroom := policy.P95Headroom
	if p95Headroom == 0 {
		p95Headroom = 1
	}
	target := int(math.Ceil(float64(stats.AvgTokens) * headroom))
	if stats.P95Tokens > 0 {
		target = maxInt(target, int(math.Ceil(float64(stats.P95Tokens)*p95Headroom)))
	}
	target = maxInt(target, stats.LifetimeMax)
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
			"sample_count":   stats.SampleCount,
			"window_seconds": stats.WindowSeconds,
			"avg_tokens":     stats.AvgTokens,
			"p95_tokens":     stats.P95Tokens,
			"lifetime_max":   stats.LifetimeMax,
		},
		Rationale: fmt.Sprintf("%d samples over %s show avg %d, p95 %d, and max %d tokens against cap %d; %d is a shared context that preserves observed tail usage", stats.SampleCount, time.Duration(stats.WindowSeconds)*time.Second, stats.AvgTokens, stats.P95Tokens, stats.LifetimeMax, currentCap, shared),
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

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

var _ ports.Optimizer = Engine{}
