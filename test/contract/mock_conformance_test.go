package contract

import (
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestMockBackendAdapterConformance(t *testing.T) {
	RunBackendAdapterConformance(t, "mock",
		func() ports.BackendAdapter { return mocks.NewBackendAdapter() },
		fixtures.MakePreset())
}

func TestMockNodeAgentConformance(t *testing.T) {
	RunNodeAgentConformance(t, "mock",
		func() ports.NodeAgent { return mocks.NewNodeAgent(fixtures.MakeNode()) },
		fixtures.MakePreset())
}

func TestMockResourceEstimatorConformance(t *testing.T) {
	RunResourceEstimatorConformance(t, "mock",
		func() ports.ResourceEstimator {
			return &mocks.ResourceEstimator{Claim: fixtures.MakeClaim(5600, 1476)}
		},
		fixtures.MakePreset())
}

func TestMockAllocatorConformance(t *testing.T) {
	RunAllocatorConformance(t, "mock",
		func() ports.Allocator { return &mocks.Allocator{FitsVal: true, CanStackLoadVal: true} },
		fixtures.MakeNode(),
		domain.Claim{WeightsMB: 1, KVReservedMB: 1})
}
