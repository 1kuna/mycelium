package membership

import (
	"context"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestOverlayDiscoveryAndTunnelUseConfiguredBackend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tunnel := &mocks.Tunnel{Addr: "127.0.0.1:7000"}
	backend := NewMemoryOverlayBackend(tunnel)
	discovery := NewOverlayDiscovery(backend)
	overlayTunnel := NewOverlayTunnel(backend)

	watch, err := discovery.WatchPeers(ctx)
	if err != nil {
		t.Fatalf("WatchPeers: %v", err)
	}
	peer := fixtures.MakePeer(fixtures.WithPeerID("peer-b"))
	if err := discovery.Advertise(ctx, peer); err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	seen := <-watch
	if seen.ID != peer.ID {
		t.Fatalf("watch peer = %+v", seen)
	}
	peers, err := discovery.Peers(ctx)
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 || peers[0].ID != peer.ID {
		t.Fatalf("peers = %+v", peers)
	}
	addr, err := overlayTunnel.Open(ctx, domain.Node{ID: "peer-b", Address: "10.0.0.2:51846"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if addr != tunnel.Addr {
		t.Fatalf("addr = %s", addr)
	}
	if err := overlayTunnel.Close(ctx, "peer-b"); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOverlayDiscoveryAndTunnelFailWithoutBackend(t *testing.T) {
	discovery := OverlayDiscovery{}
	if err := discovery.Advertise(context.Background(), fixtures.MakePeer()); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("Advertise err = %v", err)
	}
	if _, err := discovery.Peers(context.Background()); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("Peers err = %v", err)
	}
	if _, err := discovery.WatchPeers(context.Background()); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("WatchPeers err = %v", err)
	}

	tunnel := OverlayTunnel{}
	if _, err := tunnel.Open(context.Background(), fixtures.MakeNode()); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("Open err = %v", err)
	}
	if err := tunnel.Close(context.Background(), "node-a"); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("Close err = %v", err)
	}
}

func TestMemoryOverlayRejectsBadInputAndCanceledContext(t *testing.T) {
	backend := NewMemoryOverlayBackend(nil)
	if err := backend.Advertise(context.Background(), domain.Peer{}); err == nil {
		t.Fatal("missing peer id accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backend.Advertise(ctx, fixtures.MakePeer()); err != context.Canceled {
		t.Fatalf("advertise canceled err = %v", err)
	}
	if _, err := backend.Peers(ctx); err != context.Canceled {
		t.Fatalf("peers canceled err = %v", err)
	}
	if _, err := backend.WatchPeers(ctx); err != context.Canceled {
		t.Fatalf("watch canceled err = %v", err)
	}
	if _, err := backend.Open(context.Background(), fixtures.MakeNode()); err == nil || !strings.Contains(err.Error(), "tunnel backend") {
		t.Fatalf("open err = %v", err)
	}
	if err := backend.Close(context.Background(), "node-a"); err == nil || !strings.Contains(err.Error(), "tunnel backend") {
		t.Fatalf("close err = %v", err)
	}
}
