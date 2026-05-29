package contract

import (
	"context"
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
}
