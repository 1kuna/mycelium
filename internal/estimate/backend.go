package estimate

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type BackendAware struct {
	LlamaCpp ports.ResourceEstimator
	Explicit ports.ResourceEstimator
}

func NewBackendAware(llamaCpp, explicit ports.ResourceEstimator) *BackendAware {
	if explicit == nil {
		explicit = NewInMemory()
	}
	return &BackendAware{LlamaCpp: llamaCpp, Explicit: explicit}
}

func (e *BackendAware) Estimate(ctx context.Context, p domain.Preset, contextLen, concurrency int) (domain.Claim, error) {
	if err := ctx.Err(); err != nil {
		return domain.Claim{}, err
	}
	switch p.Backend {
	case domain.BackendLlamaCpp:
		if e.LlamaCpp != nil {
			return e.LlamaCpp.Estimate(ctx, p, contextLen, concurrency)
		}
		return domain.Claim{}, fmt.Errorf("llamacpp preset %q requires GGUF preflight estimator", p.ID)
	case domain.BackendVLLM:
		return domain.Claim{}, fmt.Errorf("vllm preset %q requires unit-aware reservation estimation", p.ID)
	case domain.BackendMLX, domain.BackendCustom:
		return e.Explicit.Estimate(ctx, p, contextLen, concurrency)
	default:
		return domain.Claim{}, fmt.Errorf("unsupported backend %q for preset %q estimation", p.Backend, p.ID)
	}
}

func (e *BackendAware) EstimateForUnit(ctx context.Context, p domain.Preset, contextLen, concurrency int, node domain.Node, acceleratorSet []int) (domain.Claim, error) {
	if err := ctx.Err(); err != nil {
		return domain.Claim{}, err
	}
	switch p.Backend {
	case domain.BackendVLLM:
		return estimateVLLMReservation(ctx, p, node, acceleratorSet)
	case domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendCustom:
		return e.Estimate(ctx, p, contextLen, concurrency)
	default:
		return domain.Claim{}, fmt.Errorf("unsupported backend %q for preset %q estimation", p.Backend, p.ID)
	}
}

var _ ports.ResourceEstimator = (*BackendAware)(nil)
var _ ports.UnitResourceEstimator = (*BackendAware)(nil)
