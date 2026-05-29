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

type SpeedCalibrationStore interface {
	ports.TelemetryStore
	ListNodes(ctx context.Context) ([]domain.Node, error)
	SaveNode(ctx context.Context, node domain.Node) error
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
	Store        RuntimeStore
	Clock        ports.Clock
	Policy       RecommendationPolicy
	EnginePolicy EnginePresetPolicy
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
	records := []domain.RecommendationRecord{}
	if rec, ok := RecommendContextCap(project, stats, s.Policy); ok {
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
		records = append(records, record)
	}
	metrics, err := s.Store.Metrics(ctx, project.ID)
	if err != nil {
		return nil, err
	}
	if record, ok := RecommendEnginePreset(project, presets, metrics, s.Clock.Now(), s.EnginePolicy); ok {
		if err := s.Store.SaveRecommendation(ctx, record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
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
		Observed:         observedFloats(rec.Observed),
		Rationale:        rec.Rationale,
		CreatedAt:        now.UTC(),
	}
}

func observedFloats(observed map[string]int) map[string]float64 {
	if len(observed) == 0 {
		return nil
	}
	out := make(map[string]float64, len(observed))
	for key, value := range observed {
		out[key] = float64(value)
	}
	return out
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

func CalibrateSpeedClasses(ctx context.Context, store SpeedCalibrationStore, clk ports.Clock) ([]domain.Node, error) {
	if store == nil {
		return nil, fmt.Errorf("speed calibration store is not configured")
	}
	if clk == nil {
		return nil, fmt.Errorf("speed calibration clock is not configured")
	}
	metrics, err := store.Metrics(ctx, "")
	if err != nil {
		return nil, err
	}
	type aggregate struct {
		sum   float64
		count int
	}
	byNode := map[string]aggregate{}
	for _, metric := range metrics {
		if metric.NodeID == "" || metric.TokensPerSec <= 0 {
			continue
		}
		agg := byNode[metric.NodeID]
		agg.sum += metric.TokensPerSec
		agg.count++
		byNode[metric.NodeID] = agg
	}
	if len(byNode) == 0 {
		return nil, nil
	}
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	now := clk.Now().UTC()
	updated := make([]domain.Node, 0, len(nodes))
	for _, node := range nodes {
		agg, ok := byNode[node.ID]
		if !ok || agg.count == 0 {
			continue
		}
		node.SpeedClass = domain.SpeedClass{
			TokensPerSecRef: math.Round((agg.sum/float64(agg.count))*100) / 100,
			Source:          "telemetry-calibrated",
			ProbedAt:        now,
		}
		if err := store.SaveNode(ctx, node); err != nil {
			return nil, err
		}
		updated = append(updated, node)
	}
	return updated, nil
}
