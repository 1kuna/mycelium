package optimizer

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/internal/telemetry"
)

type PresetSource interface {
	ListPresets(ctx context.Context) ([]domain.Preset, error)
}

type RuntimeStore interface {
	ports.TelemetryStore
	PresetSource
	SaveProject(ctx context.Context, project domain.Project) error
	SavePreset(ctx context.Context, preset domain.Preset) error
	SaveRecommendation(ctx context.Context, rec domain.RecommendationRecord) error
}

type TelemetryStatsProvider struct {
	Telemetry  ports.TelemetryStore
	Presets    PresetSource
	CurrentCap int
}

func (p TelemetryStatsProvider) Stats(ctx context.Context, projectID string) (ProjectStats, error) {
	if p.Telemetry == nil {
		return ProjectStats{}, fmt.Errorf("telemetry store is not configured")
	}
	if p.Presets == nil {
		return ProjectStats{}, fmt.Errorf("preset source is not configured")
	}
	rollup, err := telemetry.RollupContext(ctx, p.Telemetry, projectID)
	if err != nil {
		return ProjectStats{}, err
	}
	presets, err := p.Presets.ListPresets(ctx)
	if err != nil {
		return ProjectStats{}, err
	}
	return ProjectStats{
		ProjectID:      projectID,
		AvgTokens:      int(math.Ceil(rollup.Average)),
		P95Tokens:      rollup.P95,
		LifetimeMax:    rollup.LifetimeMax,
		CurrentCap:     p.CurrentCap,
		SharedContexts: sharedContexts(presets),
	}, nil
}

type RecommendationService struct {
	Store  RuntimeStore
	Clock  ports.Clock
	Policy RecommendationPolicy
}

func (s RecommendationService) EvaluateProject(ctx context.Context, project domain.Project) ([]domain.RecommendationRecord, error) {
	if s.Store == nil {
		return nil, fmt.Errorf("optimizer store is not configured")
	}
	if s.Clock == nil {
		return nil, fmt.Errorf("optimizer clock is not configured")
	}
	presets, err := s.Store.ListPresets(ctx)
	if err != nil {
		return nil, err
	}
	stats, err := (TelemetryStatsProvider{Telemetry: s.Store, Presets: staticPresetSource(presets), CurrentCap: project.ContextCap}).Stats(ctx, project.ID)
	if err != nil {
		return nil, err
	}
	rec, ok := RecommendContextCap(project, stats, s.Policy)
	if !ok {
		return nil, nil
	}
	preset, hasPreset := selectProjectPreset(project, presets)
	if project.AutoApply && !hasPreset {
		return nil, fmt.Errorf("cannot auto-apply recommendation for project %q: no preset is available", project.ID)
	}
	record := recommendationRecord(project, preset, rec, s.Clock.Now())
	if err := s.Store.SaveRecommendation(ctx, record); err != nil {
		return nil, err
	}
	if project.AutoApply {
		applied := ApplyRecommendation(project, preset, rec)
		if applied.Applied {
			if err := s.Store.SaveProject(ctx, applied.Project); err != nil {
				return nil, err
			}
			if err := s.Store.SavePreset(ctx, applied.Preset); err != nil {
				return nil, err
			}
			record.Applied = true
			record.AppliedAt = s.Clock.Now().UTC()
			if err := s.Store.SaveRecommendation(ctx, record); err != nil {
				return nil, err
			}
		}
	}
	return []domain.RecommendationRecord{record}, nil
}

type staticPresetSource []domain.Preset

func (s staticPresetSource) ListPresets(context.Context) ([]domain.Preset, error) {
	return append([]domain.Preset(nil), s...), nil
}

func recommendationRecord(project domain.Project, preset domain.Preset, rec Recommendation, now time.Time) domain.RecommendationRecord {
	id := fmt.Sprintf("%s-%s-%d", project.ID, rec.Type, rec.RecommendedCap)
	return domain.RecommendationRecord{
		ID:               id,
		Type:             rec.Type,
		ProjectID:        project.ID,
		PresetID:         preset.ID,
		CurrentValue:     rec.CurrentCap,
		RecommendedValue: rec.RecommendedCap,
		Rationale:        rec.Rationale,
		CreatedAt:        now.UTC(),
	}
}

func sharedContexts(presets []domain.Preset) []int {
	seen := map[int]struct{}{}
	for _, preset := range presets {
		if preset.ContextLength > 0 {
			seen[preset.ContextLength] = struct{}{}
		}
	}
	out := make([]int, 0, len(seen))
	for ctx := range seen {
		out = append(out, ctx)
	}
	sort.Ints(out)
	return out
}

func selectProjectPreset(project domain.Project, presets []domain.Preset) (domain.Preset, bool) {
	if len(presets) == 0 {
		return domain.Preset{}, false
	}
	sorted := append([]domain.Preset(nil), presets...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	if project.ContextCap > 0 {
		for _, preset := range sorted {
			if preset.ContextLength == project.ContextCap {
				return preset, true
			}
		}
	}
	return sorted[0], true
}
