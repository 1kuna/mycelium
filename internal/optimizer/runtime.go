package optimizer

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
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
	ListNodes(ctx context.Context) ([]domain.Node, error)
	ListInstances(ctx context.Context) ([]domain.ModelInstance, error)
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
	metrics, err := p.Telemetry.Metrics(ctx, projectID)
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
		P95TTFTMS:      p95TTFT(metrics),
	}, nil
}

type RecommendationService struct {
	Store        RuntimeStore
	Clock        ports.Clock
	Policy       RecommendationPolicy
	EnginePolicy EnginePresetPolicy
	Estimator    ports.ResourceEstimator
	Allocator    ports.Allocator
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
		record := recommendationRecord(project, preset, rec, s.Clock.Now())
		proof := s.proveRecommendationFit(ctx, project, preset, rec, stats, hasPreset)
		record.Observed = mergeObserved(record.Observed, proof.Observed)
		if !proof.Safe {
			record = rejectRecommendation(record, proof.Reason)
			if err := s.Store.SaveRecommendation(ctx, record); err != nil {
				return nil, err
			}
			records = append(records, record)
		} else {
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
	}
	metrics, err := s.Store.Metrics(ctx, project.ID)
	if err != nil {
		return nil, err
	}
	if record, ok := RecommendEnginePreset(project, presets, metrics, s.Clock.Now(), s.EnginePolicy); ok {
		if preset, hasPreset := presetByID(presets, record.RecommendedPresetID); hasPreset {
			proof := s.provePresetFit(ctx, project, preset)
			record.Observed = mergeObserved(record.Observed, proof.Observed)
			if !proof.Safe {
				record = rejectRecommendation(record, proof.Reason)
			}
		} else {
			record = rejectRecommendation(record, fmt.Sprintf("recommended preset %q is not available for fit proof", record.RecommendedPresetID))
		}
		if err := s.Store.SaveRecommendation(ctx, record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

type fitProof struct {
	Safe     bool
	Reason   string
	Observed map[string]float64
}

func (s RecommendationService) proveRecommendationFit(ctx context.Context, project domain.Project, preset domain.Preset, rec Recommendation, stats ProjectStats, hasPreset bool) fitProof {
	observed := map[string]float64{}
	if project.LatencyTargetMS > 0 {
		observed["latency_target_ms"] = float64(project.LatencyTargetMS)
		if stats.P95TTFTMS == 0 {
			return fitProof{Safe: false, Reason: "latency target cannot be proven: no TTFT telemetry", Observed: observed}
		}
		observed["p95_ttft_ms"] = float64(stats.P95TTFTMS)
		if stats.P95TTFTMS > project.LatencyTargetMS {
			return fitProof{Safe: false, Reason: fmt.Sprintf("latency target %dms is not preserved: observed p95 TTFT is %dms", project.LatencyTargetMS, stats.P95TTFTMS), Observed: observed}
		}
	}
	if !hasPreset {
		return fitProof{Safe: false, Reason: "fit proof requires an existing project preset", Observed: observed}
	}
	if rec.Type == RecommendationContextCap {
		preset.ContextLength = rec.RecommendedCap
	}
	proof := s.provePresetFit(ctx, project, preset)
	proof.Observed = mergeObserved(observed, proof.Observed)
	return proof
}

func (s RecommendationService) provePresetFit(ctx context.Context, project domain.Project, preset domain.Preset) fitProof {
	estimator := s.Estimator
	if estimator == nil {
		estimator = estimate.NewInMemory()
	}
	concurrency := project.ExpectedConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	claim, err := estimator.Estimate(ctx, preset, preset.ContextLength, concurrency)
	if err != nil {
		return fitProof{Safe: false, Reason: fmt.Sprintf("fit proof estimate failed: %v", err)}
	}
	nodes, err := s.Store.ListNodes(ctx)
	if err != nil {
		return fitProof{Safe: false, Reason: fmt.Sprintf("fit proof node list failed: %v", err)}
	}
	instances, err := s.Store.ListInstances(ctx)
	if err != nil {
		return fitProof{Safe: false, Reason: fmt.Sprintf("fit proof instance list failed: %v", err)}
	}
	allocator := s.Allocator
	if allocator == nil {
		allocator = lease.NewAllocator()
	}
	observed := map[string]float64{
		"fit_weights_mb": float64(claim.WeightsMB),
		"fit_kv_mb":      float64(claim.KVReservedMB),
	}
	checked := 0
	for _, node := range nodes {
		if project.ReservedNodeID != "" && node.ID != project.ReservedNodeID {
			continue
		}
		if node.Status != "" && node.Status != domain.NodeReady {
			continue
		}
		for _, unit := range recommendationUnits(node) {
			checked++
			if allocator.Fits(node, unit, instances, claim) {
				observed["fit_nodes"] = 1
				observed["fit_checked_units"] = float64(checked)
				return fitProof{Safe: true, Observed: observed}
			}
		}
	}
	observed["fit_checked_units"] = float64(checked)
	return fitProof{Safe: false, Reason: fmt.Sprintf("fit proof failed: preset %q context %d does not fit any ready compatible unit under max_util", preset.ID, preset.ContextLength), Observed: observed}
}

func recommendationUnits(node domain.Node) [][]int {
	if len(node.Accelerators) == 0 {
		return nil
	}
	units := make([][]int, 0, len(node.Accelerators)+1)
	all := make([]int, 0, len(node.Accelerators))
	for _, acc := range node.Accelerators {
		units = append(units, []int{acc.Index})
		all = append(all, acc.Index)
	}
	if len(all) > 1 {
		units = append(units, all)
	}
	return units
}

func rejectRecommendation(rec domain.RecommendationRecord, reason string) domain.RecommendationRecord {
	rec.Rejected = true
	rec.RejectReason = reason
	rec.Rationale = rec.Rationale + "; rejected: " + reason
	return rec
}

func mergeObserved(left, right map[string]float64) map[string]float64 {
	if len(left) == 0 {
		return right
	}
	out := make(map[string]float64, len(left)+len(right))
	for key, value := range left {
		out[key] = value
	}
	for key, value := range right {
		out[key] = value
	}
	return out
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
	if project.DefaultModel != "" {
		return presetByModel(sorted, project.DefaultModel)
	}
	if project.ContextCap > 0 {
		for _, preset := range sorted {
			if preset.ContextLength == project.ContextCap {
				return preset, true
			}
		}
	}
	return sorted[0], true
}

func presetByModel(presets []domain.Preset, model string) (domain.Preset, bool) {
	for _, preset := range presets {
		if preset.ID == model || preset.ModelRef == model {
			return preset, true
		}
		for _, alias := range preset.Aliases {
			if alias == model {
				return preset, true
			}
		}
	}
	return domain.Preset{}, false
}

func presetByID(presets []domain.Preset, id string) (domain.Preset, bool) {
	for _, preset := range presets {
		if preset.ID == id {
			return preset, true
		}
	}
	return domain.Preset{}, false
}

func p95TTFT(metrics []domain.RunMetric) int {
	values := []int{}
	for _, metric := range metrics {
		if metric.TTFTms > 0 {
			values = append(values, metric.TTFTms)
		}
	}
	if len(values) == 0 {
		return 0
	}
	sort.Ints(values)
	idx := int(math.Ceil(float64(len(values))*0.95)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
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
