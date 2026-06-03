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

func TestMockAdmissionControllerConformance(t *testing.T) {
	RunAdmissionControllerConformance(t, "mock",
		func() ports.AdmissionController { return &mocks.AdmissionController{} },
		fixtures.MakeJob(),
		fixtures.MakePreset(),
		fixtures.MakeClaim(1, 1))
}

func TestMockJobRegistryConformance(t *testing.T) {
	RunJobRegistryConformance(t, "mock",
		func() ports.JobRegistry { return &mocks.JobRegistry{} },
		fixtures.MakeJobRecord())
}

func TestMockPeerDiscoveryConformance(t *testing.T) {
	RunPeerDiscoveryConformance(t, "mock",
		func() ports.PeerDiscovery { return &mocks.PeerDiscovery{} },
		fixtures.MakePeer())
}

func TestMockTunnelConformance(t *testing.T) {
	RunTunnelConformance(t, "mock",
		func() ports.Tunnel { return &mocks.Tunnel{Addr: "127.0.0.1:6000"} },
		fixtures.MakeNode())
}

func TestMockHardwareDetectorConformance(t *testing.T) {
	RunHardwareDetectorConformance(t, "mock",
		func() ports.HardwareDetector { return &mocks.HardwareDetector{} },
		fixtures.MakeNode())
}
