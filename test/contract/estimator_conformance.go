package contract

import (
	"context"
	"errors"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

func RunResourceEstimatorConformance(t *testing.T, name string, newEstimator func() ports.ResourceEstimator, p domain.Preset) {
	t.Run(name+"/returns_nonzero_claim", func(t *testing.T) {
		estimator := newEstimator()
		claim, err := estimator.Estimate(context.Background(), p, p.ContextLength, 1)
		assert.NoError(t, "Estimate", err)
		assert.True(t, claim.WeightsMB > 0, "weights must be positive: %+v", claim)
		assert.True(t, claim.KVReservedMB >= 0, "kv must not be negative: %+v", claim)
	})

	t.Run(name+"/rejects_bad_request_shape", func(t *testing.T) {
		estimator := newEstimator()
		_, err := estimator.Estimate(context.Background(), p, 0, 1)
		assert.Error(t, "zero context", err)
		_, err = estimator.Estimate(context.Background(), p, p.ContextLength, 0)
		assert.Error(t, "zero concurrency", err)
	})

	t.Run(name+"/respects_context_cancellation", func(t *testing.T) {
		estimator := newEstimator()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := estimator.Estimate(ctx, p, p.ContextLength, 1)
		assert.True(t, errors.Is(err, context.Canceled), "Estimate err = %v, want context.Canceled", err)
	})
}
