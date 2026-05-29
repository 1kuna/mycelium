package telemetry

import (
	"context"
	"sort"

	"mycelium/internal/ports"
)

type ContextStats struct {
	Count       int
	Average     float64
	P95         int
	LifetimeMax int
}

func RollupContext(ctx context.Context, store ports.TelemetryStore, project string) (ContextStats, error) {
	metrics, err := store.Metrics(ctx, project)
	if err != nil {
		return ContextStats{}, err
	}
	if len(metrics) == 0 {
		return ContextStats{}, nil
	}

	contexts := make([]int, 0, len(metrics))
	var total int
	var max int
	for _, metric := range metrics {
		contexts = append(contexts, metric.ContextUsed)
		total += metric.ContextUsed
		if metric.ContextUsed > max {
			max = metric.ContextUsed
		}
	}
	sort.Ints(contexts)
	p95Index := int(float64(len(contexts)-1) * 0.95)
	return ContextStats{
		Count:       len(contexts),
		Average:     float64(total) / float64(len(contexts)),
		P95:         contexts[p95Index],
		LifetimeMax: max,
	}, nil
}
