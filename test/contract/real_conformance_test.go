package contract

import (
	"testing"

	peercoord "mycelium/internal/peer"
	"mycelium/internal/ports"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/test/fixtures"
)

func TestRealJobRegistryConformance(t *testing.T) {
	RunJobRegistryConformance(t, "peer-memory",
		func() ports.JobRegistry { return peercoord.NewJobRegistry() },
		fixtures.MakeJobRecord())
	RunJobRegistryConformance(t, "sqlite",
		func() ports.JobRegistry {
			store, err := storesqlite.Open(":memory:")
			if err != nil {
				t.Fatalf("Open sqlite registry: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })
			return store
		},
		fixtures.MakeJobRecord())
}
