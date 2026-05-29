package optimizer

import (
	"fmt"
	"math"

	"mycelium/internal/domain"
)

type ConsolidationInput struct {
	Left              domain.Preset
	Right             domain.Preset
	SharedContext     int
	DailyReloads      int
	TokensPerSecDelta float64
}

type ConsolidationResult struct {
	ShouldCollapse bool
	BeforeCost     float64
	AfterCost      float64
	Savings        float64
	Rationale      string
}

func EvaluateConsolidation(in ConsolidationInput) (ConsolidationResult, error) {
	if in.SharedContext <= 0 {
		return ConsolidationResult{}, fmt.Errorf("shared context is required")
	}
	if in.Left.Backend != in.Right.Backend {
		return ConsolidationResult{}, fmt.Errorf("cannot consolidate presets with different backends")
	}
	beforeContexts := 2.0
	if in.Left.ContextLength == in.Right.ContextLength {
		beforeContexts = 1
	}
	reload := backendReloadCost(in.Left.Backend) * float64(in.DailyReloads)
	fragmentation := beforeContexts * 5
	beforeReserve := reserveCost(in.Left) + reserveCost(in.Right)
	afterPreset := in.Left
	afterPreset.ContextLength = in.SharedContext
	afterReserve := reserveCost(afterPreset) * 2
	latencyPenalty := math.Max(0, -in.TokensPerSecDelta) / 10
	before := reload + fragmentation + beforeReserve
	after := 5 + afterReserve + latencyPenalty
	savings := before - after
	return ConsolidationResult{
		ShouldCollapse: savings > 0,
		BeforeCost:     before,
		AfterCost:      after,
		Savings:        savings,
		Rationale:      fmt.Sprintf("before=%.2f after=%.2f reload_cost=%.2f fragmentation_contexts=%.0f", before, after, reload, beforeContexts),
	}, nil
}

func backendReloadCost(backend domain.Backend) float64 {
	switch backend {
	case domain.BackendLlamaCpp, domain.BackendMLX:
		return 10
	case domain.BackendVLLM:
		return 1
	default:
		return 5
	}
}

func reserveCost(preset domain.Preset) float64 {
	return float64(preset.ContextLength) * preset.KVPerTokenMB / 100
}
