package mocks

import (
	"context"

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

func (m *ResourceEstimator) Estimate(_ context.Context, p domain.Preset, contextLen, concurrency int) (domain.Claim, error) {
	m.Calls = append(m.Calls, EstimateCall{Preset: p, ContextLen: contextLen, Concurrency: concurrency})
	if m.Err != nil {
		return domain.Claim{}, m.Err
	}
	return m.Claim, nil
}

var _ ports.ResourceEstimator = (*ResourceEstimator)(nil)
