package mocks

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type EstimateCall struct {
	Preset      domain.Preset
	ContextLen  int
	Concurrency int
}

type ResourceEstimator struct {
	Claim domain.Claim
	Err   error
	Calls []EstimateCall
}

func (m *ResourceEstimator) Estimate(ctx context.Context, p domain.Preset, contextLen, concurrency int) (domain.Claim, error) {
	if err := ctx.Err(); err != nil {
		return domain.Claim{}, err
	}
	m.Calls = append(m.Calls, EstimateCall{Preset: p, ContextLen: contextLen, Concurrency: concurrency})
	if m.Err != nil {
		return domain.Claim{}, m.Err
	}
	if contextLen <= 0 {
		return domain.Claim{}, fmt.Errorf("context length must be positive: %d", contextLen)
	}
	if concurrency <= 0 {
		return domain.Claim{}, fmt.Errorf("concurrency must be positive: %d", concurrency)
	}
	return m.Claim, nil
}

var _ ports.ResourceEstimator = (*ResourceEstimator)(nil)
