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

type TelemetryPeerClient struct {
	MetricsByPeer          map[string][]domain.RunMetric
	RecommendationsByPeer  map[string][]domain.RecommendationRecord
	MetricsErr             error
	PushMetricsErr         error
	RecommendationsErr     error
	PushRecommendationsErr error
	PushedMetrics          map[string][]domain.RunMetric
	PushedRecommendations  map[string][]domain.RecommendationRecord
	Calls                  []string
}

func (m *TelemetryPeerClient) Metrics(_ context.Context, peer domain.Peer) ([]domain.RunMetric, error) {
	m.Calls = append(m.Calls, "metrics:"+peer.ID)
	if m.MetricsErr != nil {
		return nil, m.MetricsErr
	}
	return append([]domain.RunMetric(nil), m.MetricsByPeer[peer.ID]...), nil
}

func (m *TelemetryPeerClient) PushMetrics(_ context.Context, peer domain.Peer, metrics []domain.RunMetric) error {
	m.Calls = append(m.Calls, "push-metrics:"+peer.ID)
	if m.PushMetricsErr != nil {
		return m.PushMetricsErr
	}
	if m.PushedMetrics == nil {
		m.PushedMetrics = map[string][]domain.RunMetric{}
	}
	m.PushedMetrics[peer.ID] = append([]domain.RunMetric(nil), metrics...)
	return nil
}

func (m *TelemetryPeerClient) Recommendations(_ context.Context, peer domain.Peer) ([]domain.RecommendationRecord, error) {
	m.Calls = append(m.Calls, "recommendations:"+peer.ID)
	if m.RecommendationsErr != nil {
		return nil, m.RecommendationsErr
	}
	return append([]domain.RecommendationRecord(nil), m.RecommendationsByPeer[peer.ID]...), nil
}

func (m *TelemetryPeerClient) PushRecommendations(_ context.Context, peer domain.Peer, recs []domain.RecommendationRecord) error {
	m.Calls = append(m.Calls, "push-recommendations:"+peer.ID)
	if m.PushRecommendationsErr != nil {
		return m.PushRecommendationsErr
	}
	if m.PushedRecommendations == nil {
		m.PushedRecommendations = map[string][]domain.RecommendationRecord{}
	}
	m.PushedRecommendations[peer.ID] = append([]domain.RecommendationRecord(nil), recs...)
	return nil
}

var _ ports.TelemetryPeerClient = (*TelemetryPeerClient)(nil)
