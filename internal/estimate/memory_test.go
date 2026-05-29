package estimate

import (
	"context"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract"
	"mycelium/test/fixtures"
)

func TestInMemoryEstimatorConformance(t *testing.T) {
	contract.RunResourceEstimatorConformance(t, "inmemory",
		func() ports.ResourceEstimator { return NewInMemory() },
		fixtures.MakePreset())
}

func TestInMemoryEstimatorComputesKVReservation(t *testing.T) {
	estimator := NewInMemory()
	preset := fixtures.MakePreset(fixtures.WithWeights(5600), fixtures.WithKVPerToken(0.18))

	claim, err := estimator.Estimate(context.Background(), preset, 8000, 2)
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	if claim.WeightsMB != 5600 || claim.KVReservedMB != 2880 {
		t.Fatalf("claim = %+v", claim)
	}
}

func TestInMemoryEstimatorFailsOnInvalidInputs(t *testing.T) {
	tests := []struct {
		name   string
		preset domain.Preset
		ctxLen int
		conc   int
		want   string
	}{
		{
			name:   "weights",
			preset: fixtures.MakePreset(fixtures.WithWeights(0)),
			ctxLen: 8000,
			conc:   1,
			want:   "invalid weights",
		},
		{
			name:   "kv",
			preset: fixtures.MakePreset(fixtures.WithKVPerToken(-1)),
			ctxLen: 8000,
			conc:   1,
			want:   "invalid kv_per_token",
		},
		{
			name:   "context",
			preset: fixtures.MakePreset(),
			ctxLen: 0,
			conc:   1,
			want:   "context length",
		},
		{
			name:   "concurrency",
			preset: fixtures.MakePreset(),
			ctxLen: 8000,
			conc:   0,
			want:   "concurrency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewInMemory().Estimate(context.Background(), tt.preset, tt.ctxLen, tt.conc)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestInMemoryEstimatorRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewInMemory().Estimate(ctx, fixtures.MakePreset(), 8000, 1)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}
