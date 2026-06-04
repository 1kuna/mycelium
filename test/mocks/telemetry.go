package mocks

import (
	"context"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type TelemetrySink struct {
	Err        error
	SampleErr  error
	Metrics    []domain.RunMetric
	SamplesOut []domain.SessionMetric
	Calls      []string
}

func (m *TelemetrySink) Record(_ context.Context, metric domain.RunMetric) error {
	m.Calls = append(m.Calls, "record:"+metric.JobID)
	if m.Err != nil {
		return m.Err
	}
	m.Metrics = append(m.Metrics, metric)
	return nil
}

func (m *TelemetrySink) RecordSample(_ context.Context, sample domain.SessionMetric) error {
	m.Calls = append(m.Calls, "sample:"+sample.JobID+":"+string(sample.Phase))
	if m.SampleErr != nil {
		return m.SampleErr
	}
	m.SamplesOut = append(m.SamplesOut, sample)
	return nil
}

var _ ports.TelemetrySink = (*TelemetrySink)(nil)

type TelemetryPeerClient struct {
	MetricsByPeer          map[string][]domain.RunMetric
	SamplesByPeer          map[string][]domain.SessionMetric
	RecommendationsByPeer  map[string][]domain.RecommendationRecord
	MetricsErr             error
	PushMetricsErr         error
	SamplesErr             error
	PushSamplesErr         error
	RecommendationsErr     error
	PushRecommendationsErr error
	PushedMetrics          map[string][]domain.RunMetric
	PushedSamples          map[string][]domain.SessionMetric
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

func (m *TelemetryPeerClient) Samples(_ context.Context, peer domain.Peer, _ domain.SessionMetricQuery) ([]domain.SessionMetric, error) {
	m.Calls = append(m.Calls, "samples:"+peer.ID)
	if m.SamplesErr != nil {
		return nil, m.SamplesErr
	}
	return append([]domain.SessionMetric(nil), m.SamplesByPeer[peer.ID]...), nil
}

func (m *TelemetryPeerClient) PushSamples(_ context.Context, peer domain.Peer, samples []domain.SessionMetric) error {
	m.Calls = append(m.Calls, "push-samples:"+peer.ID)
	if m.PushSamplesErr != nil {
		return m.PushSamplesErr
	}
	if m.PushedSamples == nil {
		m.PushedSamples = map[string][]domain.SessionMetric{}
	}
	m.PushedSamples[peer.ID] = append(m.PushedSamples[peer.ID], samples...)
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
