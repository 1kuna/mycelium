package optimizer

import (
	"fmt"

	"mycelium/internal/domain"
)

type ApplyResult struct {
	Project domain.Project
	Preset  domain.Preset
	Applied bool
	Log     domain.TraceStep
}

func ApplyRecommendation(project domain.Project, preset domain.Preset, rec Recommendation) ApplyResult {
	result := ApplyResult{Project: project, Preset: preset}
	if rec.Type != RecommendationContextCap {
		result.Log = domain.TraceStep{Step: "auto_apply", Result: fmt.Sprintf("ignored unsupported recommendation %q", rec.Type)}
		return result
	}
	if !project.AutoApply {
		result.Log = domain.TraceStep{Step: "auto_apply", Result: "skipped: project auto_apply is off", Data: map[string]any{"recommended_cap": rec.RecommendedCap}}
		return result
	}
	result.Project.ContextCap = rec.RecommendedCap
	result.Preset.ContextLength = rec.RecommendedCap
	result.Applied = true
	result.Log = domain.TraceStep{
		Step:   "auto_apply",
		Result: fmt.Sprintf("applied context cap %d", rec.RecommendedCap),
		Data: map[string]any{
			"project_id":      project.ID,
			"preset_id":       preset.ID,
			"recommended_cap": rec.RecommendedCap,
		},
	}
	return result
}
