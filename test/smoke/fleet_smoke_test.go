//go:build smoke

package smoke

import (
	"context"
	"os"
	"testing"

	"mycelium/internal/domain"
	nodeagent "mycelium/internal/node"
	"mycelium/test/fixtures"
)

func TestFleetMacMiniSmokeRequiresAddress(t *testing.T) {
	addr := os.Getenv("MYCELIUM_REMOTE_PEER_ADDR")
	if addr == "" {
		t.Skip("set MYCELIUM_REMOTE_PEER_ADDR for Phase 1 fleet smoke")
	}
	client := nodeagent.NewHTTPClient("http://" + addr)
	snap, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("second peer node agent %s is not reachable: %v", addr, err)
	}
	if snap.Node.Status != domain.NodeReady {
		t.Fatalf("second peer node is not ready: %+v", snap.Node)
	}

	model := os.Getenv("MYCELIUM_REMOTE_PEER_MODEL")
	if model == "" {
		t.Skip("set MYCELIUM_REMOTE_PEER_MODEL to run a remote load/unload smoke")
	}
	inst, err := client.Load(context.Background(), fixtures.MakePreset(
		fixtures.WithModelRef(model),
		fixtures.WithPresetNode(snap.Node.ID),
		fixtures.WithWeights(1),
		fixtures.WithKVPerToken(0.01),
	))
	if err != nil {
		t.Fatalf("remote load: %v", err)
	}
	if err := client.Unload(context.Background(), inst.ID); err != nil {
		t.Fatalf("remote unload: %v", err)
	}
}
