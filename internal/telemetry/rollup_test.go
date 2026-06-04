package telemetry

import (
	"context"
	"testing"
	"time"

	"mycelium/internal/domain"
)

func TestRollupContextUsesCeilingP95AndIgnoresZeroContext(t *testing.T) {
	store := staticMetricStore{metrics: []domain.RunMetric{
		{Project: "project-a", ContextUsed: 0, At: time.Unix(1, 0).UTC()},
		{Project: "project-a", ContextUsed: 10, At: time.Unix(2, 0).UTC()},
		{Project: "project-a", ContextUsed: 20, At: time.Unix(3, 0).UTC()},
	}}

	stats, err := RollupContext(context.Background(), store, "project-a")
	if err != nil {
		t.Fatalf("RollupContext: %v", err)
	}
	if stats.Count != 2 || stats.P95 != 20 || stats.LifetimeMax != 20 || stats.Average != 15 {
		t.Fatalf("stats = %+v", stats)
	}
}

type staticMetricStore struct {
	metrics []domain.RunMetric
}

func (s staticMetricStore) Record(context.Context, domain.RunMetric) error {
	return nil
}

func (s staticMetricStore) RecordSample(context.Context, domain.SessionMetric) error {
	return nil
}

func (s staticMetricStore) Metrics(_ context.Context, project string) ([]domain.RunMetric, error) {
	if project == "" {
		return append([]domain.RunMetric(nil), s.metrics...), nil
	}
	out := []domain.RunMetric{}
	for _, metric := range s.metrics {
		if metric.Project == project {
			out = append(out, metric)
		}
	}
	return out, nil
}

func (s staticMetricStore) Samples(context.Context, domain.SessionMetricQuery) ([]domain.SessionMetric, error) {
	return nil, nil
}
