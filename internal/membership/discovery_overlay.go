package membership

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type OverlayDiscovery struct{}

func (OverlayDiscovery) Announce(context.Context, domain.Node) error {
	return fmt.Errorf("overlay discovery is not implemented in Phase 4")
}

func (OverlayDiscovery) Discover(context.Context) ([]domain.Node, error) {
	return nil, fmt.Errorf("overlay discovery is not implemented in Phase 4")
}

var _ ports.Discovery = OverlayDiscovery{}
