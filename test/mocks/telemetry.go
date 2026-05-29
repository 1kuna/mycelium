package mocks

import (
	"context"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type TelemetrySink struct {
	Err     error
	Metrics []domain.RunMetric
}

func (m *TelemetrySink) Record(_ context.Context, metric domain.RunMetric) error {
	if m.Err != nil {
		return m.Err
	}
	m.Metrics = append(m.Metrics, metric)
	return nil
}

var _ ports.TelemetrySink = (*TelemetrySink)(nil)
