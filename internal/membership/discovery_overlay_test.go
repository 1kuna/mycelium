package membership

import (
	"context"
	"errors"
	"testing"

	"mycelium/test/fixtures"
)

func TestOverlayDiscoveryAndTunnelAreRoadmapStubs(t *testing.T) {
	discovery := OverlayDiscovery{}
	if err := discovery.Advertise(context.Background(), fixtures.MakePeer()); !errors.Is(err, ErrOverlayRoadmap) {
		t.Fatalf("Advertise err = %v", err)
	}
	if _, err := discovery.Peers(context.Background()); !errors.Is(err, ErrOverlayRoadmap) {
		t.Fatalf("Peers err = %v", err)
	}
	if _, err := discovery.WatchPeers(context.Background()); !errors.Is(err, ErrOverlayRoadmap) {
		t.Fatalf("WatchPeers err = %v", err)
	}

	tunnel := OverlayTunnel{}
	if _, err := tunnel.Open(context.Background(), fixtures.MakeNode()); !errors.Is(err, ErrOverlayRoadmap) {
		t.Fatalf("Open err = %v", err)
	}
	if err := tunnel.Close(context.Background(), "node-a"); !errors.Is(err, ErrOverlayRoadmap) {
		t.Fatalf("Close err = %v", err)
	}
}
