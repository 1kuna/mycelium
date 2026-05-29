package membership

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

var ErrOverlayRoadmap = fmt.Errorf("cross-NAT overlay discovery is roadmap-only")

type OverlayDiscovery struct{}

func (d OverlayDiscovery) Advertise(context.Context, domain.Peer) error {
	return ErrOverlayRoadmap
}

func (d OverlayDiscovery) Peers(context.Context) ([]domain.Peer, error) {
	return nil, ErrOverlayRoadmap
}

func (d OverlayDiscovery) WatchPeers(context.Context) (<-chan domain.Peer, error) {
	return nil, ErrOverlayRoadmap
}

type OverlayTunnel struct{}

func (t OverlayTunnel) Open(context.Context, domain.Node) (string, error) {
	return "", ErrOverlayRoadmap
}

func (t OverlayTunnel) Close(context.Context, string) error {
	return ErrOverlayRoadmap
}

var _ ports.PeerDiscovery = OverlayDiscovery{}
var _ ports.Tunnel = OverlayTunnel{}
