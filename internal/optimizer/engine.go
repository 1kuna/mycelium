package optimizer

import (
	"fmt"
	"math"
	"sort"
	"time"

	"mycelium/internal/bench"
	"mycelium/internal/domain"
)

const RecommendationEngineParameter = "engine_parameter_recommendation"

type EnginePresetPolicy struct {
	MinSamples int
	MinSpeedup float64
}

type presetAggregate struct {
	Preset domain.Preset
	Count  int
	TPS    float64
	TTFT   float64
	LoadMS float64
}

func RecommendEnginePreset(project domain.Project, presets []domain.Preset, metrics []domain.RunMetric, now time.Time, policy EnginePresetPolicy) (domain.RecommendationRecord, bool) {
	minSamples := policy.MinSamples
	if minSamples == 0 {
		minSamples = 2
	}
	minSpeedup := policy.MinSpeedup
	if minSpeedup == 0 {
		minSpeedup = 0.10
	}
	byPreset := presetsByID(presets)
	aggregates := map[string]*presetAggregate{}
	for _, metric := range metrics {
		if project.ID != "" && metric.Project != project.ID {
			continue
		}
		if metric.PresetID == "" || metric.TokensPerSec <= 0 {
			continue
		}
		preset, ok := byPreset[metric.PresetID]
		if !ok {
			continue
		}
		agg := aggregates[preset.ID]
		if agg == nil {
			agg = &presetAggregate{Preset: preset}
			aggregates[preset.ID] = agg
		}
		agg.Count++
		agg.TPS += metric.TokensPerSec
		agg.TTFT += float64(metric.TTFTms)
		agg.LoadMS += float64(metric.LoadWallClockMS)
	}
	ranked := make([]presetAggregate, 0, len(aggregates))
	for _, agg := range aggregates {
		if agg.Count < minSamples {
			continue
		}
		agg.TPS = round2(agg.TPS / float64(agg.Count))
		agg.TTFT = round2(agg.TTFT / float64(agg.Count))
		agg.LoadMS = round2(agg.LoadMS / float64(agg.Count))
		ranked = append(ranked, *agg)
	}
	if len(ranked) < 2 {
		return domain.RecommendationRecord{}, false
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].TPS != ranked[j].TPS {
			return ranked[i].TPS > ranked[j].TPS
		}
		if ranked[i].TTFT != ranked[j].TTFT {
			return ranked[i].TTFT < ranked[j].TTFT
		}
		if ranked[i].LoadMS != ranked[j].LoadMS {
			return ranked[i].LoadMS < ranked[j].LoadMS
		}
		return ranked[i].Preset.ID < ranked[j].Preset.ID
	})
	best := ranked[0]
	runnerUp := ranked[1]
	if best.TPS < runnerUp.TPS*(1+minSpeedup) {
		return domain.RecommendationRecord{}, false
	}
	return engineRecommendationRecord(project, best, runnerUp, now, "telemetry"), true
}

func RecommendBenchPick(project domain.Project, presets []domain.Preset, results []bench.Result, now time.Time) (domain.RecommendationRecord, bool, error) {
	byModel := presetsByModel(presets)
	var picked *bench.Result
	for i := range results {
		result := &results[i]
		if result.UserPick == nil || !*result.UserPick {
			continue
		}
		if picked != nil {
			return domain.RecommendationRecord{}, false, fmt.Errorf("benchmark results contain multiple user picks")
		}
		if result.Error != "" {
			return domain.RecommendationRecord{}, false, fmt.Errorf("benchmark user pick %q failed: %s", result.Model, result.Error)
		}
		picked = result
	}
	if picked == nil {
		return domain.RecommendationRecord{}, false, nil
	}
	preset, ok := byModel[picked.Model]
	if !ok {
		return domain.RecommendationRecord{}, false, fmt.Errorf("benchmark user pick %q has no matching preset", picked.Model)
	}
	best := presetAggregate{
		Preset: preset,
		Count:  1,
		TPS:    picked.TokensPerSec,
		TTFT:   float64(picked.TTFTms),
	}
	record := engineRecommendationRecord(project, best, presetAggregate{}, now, "benchmark-user-pick")
	record.Rationale = fmt.Sprintf("user picked %s in benchmark output; Mycelium records the chosen preset/backend and objective metrics but does not judge output quality", preset.ID)
	return record, true, nil
}

func engineRecommendationRecord(project domain.Project, best, runnerUp presetAggregate, now time.Time, source string) domain.RecommendationRecord {
	id := fmt.Sprintf("%s-%s-%s", project.ID, RecommendationEngineParameter, best.Preset.ID)
	observed := map[string]float64{
		"best_tokens_per_sec": best.TPS,
		"best_samples":        float64(best.Count),
		"best_ttft_ms":        best.TTFT,
	}
	if runnerUp.Preset.ID != "" {
		observed["runner_up_tokens_per_sec"] = runnerUp.TPS
		observed["runner_up_samples"] = float64(runnerUp.Count)
	}
	return domain.RecommendationRecord{
		ID:                  id,
		Type:                RecommendationEngineParameter,
		ProjectID:           project.ID,
		PresetID:            best.Preset.ID,
		RecommendedPresetID: best.Preset.ID,
		RecommendedBackend:  best.Preset.Backend,
		Observed:            observed,
		Rationale: fmt.Sprintf(
			"%s recommends preset %s on backend %s: %.2f tok/s over %d samples; launch_profile=%s launch_args=%v; execution remains explicit opt-in",
			source,
			best.Preset.ID,
			best.Preset.Backend,
			best.TPS,
			best.Count,
			best.Preset.LaunchProfile,
			best.Preset.LaunchArgs,
		),
		CreatedAt: now.UTC(),
	}
}

func presetsByID(presets []domain.Preset) map[string]domain.Preset {
	out := make(map[string]domain.Preset, len(presets))
	for _, preset := range presets {
		out[preset.ID] = preset
	}
	return out
}

func presetsByModel(presets []domain.Preset) map[string]domain.Preset {
	out := make(map[string]domain.Preset, len(presets)*2)
	for _, preset := range presets {
		out[preset.ID] = preset
		out[preset.ModelRef] = preset
		for _, alias := range preset.Aliases {
			if alias != "" {
				out[alias] = preset
			}
		}
	}
	return out
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
