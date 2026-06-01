package estimate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
)

func TestBackendAwareUsesBackendSpecificEstimator(t *testing.T) {
	llama := &recordingEstimator{claim: domain.Claim{WeightsMB: 10}}
	explicit := &recordingEstimator{claim: domain.Claim{WeightsMB: 20}}
	estimator := NewBackendAware(llama, explicit)

	claim, err := estimator.Estimate(context.Background(), fixtures.MakePreset(fixtures.WithPresetID("llama")), 100, 1)
	if err != nil {
		t.Fatalf("llama estimate: %v", err)
	}
	if claim.WeightsMB != 10 || llama.calls != 1 || explicit.calls != 0 {
		t.Fatalf("llama claim=%+v llama=%d explicit=%d", claim, llama.calls, explicit.calls)
	}
	mlx := fixtures.MakePreset(fixtures.WithPresetID("mlx"))
	mlx.Backend = domain.BackendMLX
	claim, err = estimator.Estimate(context.Background(), mlx, 100, 1)
	if err != nil {
		t.Fatalf("mlx estimate: %v", err)
	}
	if claim.WeightsMB != 20 || explicit.calls != 1 {
		t.Fatalf("mlx claim=%+v explicit=%d", claim, explicit.calls)
	}
}

func TestBackendAwareFailsLoudlyForUnknownBackendAndPropagatesErrors(t *testing.T) {
	boom := errors.New("metadata missing")
	estimator := NewBackendAware(nil, &recordingEstimator{err: boom})
	preset := fixtures.MakePreset(fixtures.WithPresetID("vllm"))
	preset.Backend = domain.BackendVLLM
	if _, err := estimator.Estimate(context.Background(), preset, 100, 1); err == nil || !strings.Contains(err.Error(), "unit-aware") {
		t.Fatalf("vllm global err = %v", err)
	}
	mlx := fixtures.MakePreset(fixtures.WithPresetID("mlx"))
	mlx.Backend = domain.BackendMLX
	if _, err := estimator.Estimate(context.Background(), mlx, 100, 1); !errors.Is(err, boom) {
		t.Fatalf("explicit err = %v", err)
	}
	unknown := fixtures.MakePreset(fixtures.WithPresetID("mystery"))
	unknown.Backend = domain.Backend("mystery")
	if _, err := estimator.Estimate(context.Background(), unknown, 100, 1); err == nil || !strings.Contains(err.Error(), "unsupported backend") {
		t.Fatalf("unknown err = %v", err)
	}
}

func TestBackendAwareVLLMUsesUnitReservationClaim(t *testing.T) {
	preset := fixtures.MakePreset(fixtures.WithPresetID("vllm"))
	preset.Backend = domain.BackendVLLM
	preset.LaunchArgs = []string{"serve", "{model}", "--gpu-memory-utilization", "0.25"}
	node := fixtures.MakeNode()
	node.Accelerators = []domain.Accelerator{
		{Index: 0, VRAMTotalMB: 1000},
		{Index: 1, VRAMTotalMB: 3000},
	}

	claim, err := NewBackendAware(nil, NewInMemory()).EstimateForUnit(context.Background(), preset, 100, 1, node, []int{0, 1})
	if err != nil {
		t.Fatalf("EstimateForUnit: %v", err)
	}
	if claim != (domain.Claim{WeightsMB: 1000}) {
		t.Fatalf("claim = %+v", claim)
	}

	preset.LaunchArgs = []string{"serve", "{model}"}
	if _, err := NewBackendAware(nil, NewInMemory()).EstimateForUnit(context.Background(), preset, 100, 1, node, []int{0}); err == nil || !strings.Contains(err.Error(), "gpu-memory-utilization") {
		t.Fatalf("missing reservation err = %v", err)
	}
}

type recordingEstimator struct {
	claim domain.Claim
	err   error
	calls int
}

func (e *recordingEstimator) Estimate(context.Context, domain.Preset, int, int) (domain.Claim, error) {
	e.calls++
	if e.err != nil {
		return domain.Claim{}, e.err
	}
	return e.claim, nil
}
