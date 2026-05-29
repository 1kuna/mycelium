package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func RunResourceEstimatorConformance(t *testing.T, name string, newEstimator func() ports.ResourceEstimator, p domain.Preset) {
	t.Run(name+"/returns_nonzero_claim", func(t *testing.T) {
		estimator := newEstimator()
		claim, err := estimator.Estimate(context.Background(), p, p.ContextLength, 1)
		if err != nil {
			t.Fatalf("Estimate: %v", err)
		}
		if claim.WeightsMB <= 0 {
			t.Fatalf("weights must be positive: %+v", claim)
		}
		if claim.KVReservedMB < 0 {
			t.Fatalf("kv must not be negative: %+v", claim)
		}
	})
}
