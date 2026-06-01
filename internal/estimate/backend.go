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
	switch p.Backend {
	case domain.BackendLlamaCpp:
		if e.LlamaCpp != nil {
			return e.LlamaCpp.Estimate(ctx, p, contextLen, concurrency)
		}
		return e.Explicit.Estimate(ctx, p, contextLen, concurrency)
	case domain.BackendMLX, domain.BackendVLLM, domain.BackendCustom:
		return e.Explicit.Estimate(ctx, p, contextLen, concurrency)
	default:
		return domain.Claim{}, fmt.Errorf("unsupported backend %q for preset %q estimation", p.Backend, p.ID)
	}
}

var _ ports.ResourceEstimator = (*BackendAware)(nil)
