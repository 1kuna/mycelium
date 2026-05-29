package estimate

import (
	"context"
	"fmt"
	"math"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type InMemoryEstimator struct{}

func NewInMemory() *InMemoryEstimator {
	return &InMemoryEstimator{}
}

func (e *InMemoryEstimator) Estimate(ctx context.Context, p domain.Preset, contextLen, concurrency int) (domain.Claim, error) {
	if err := ctx.Err(); err != nil {
		return domain.Claim{}, err
	}
	if p.EstWeightsMB <= 0 {
		return domain.Claim{}, fmt.Errorf("preset %q has invalid weights: %dMB", p.ID, p.EstWeightsMB)
	}
	if p.KVPerTokenMB < 0 {
		return domain.Claim{}, fmt.Errorf("preset %q has invalid kv_per_token: %f", p.ID, p.KVPerTokenMB)
	}
	if contextLen <= 0 {
		return domain.Claim{}, fmt.Errorf("context length must be positive: %d", contextLen)
	}
	if concurrency <= 0 {
		return domain.Claim{}, fmt.Errorf("concurrency must be positive: %d", concurrency)
	}

	kv := int(math.Ceil(float64(contextLen*concurrency) * p.KVPerTokenMB))
	return domain.Claim{WeightsMB: p.EstWeightsMB, KVReservedMB: kv}, nil
}

var _ ports.ResourceEstimator = (*InMemoryEstimator)(nil)
