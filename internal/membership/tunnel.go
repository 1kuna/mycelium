package membership

import (
	"context"
	"fmt"
	"sync"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type LANTunnel struct {
	mu    sync.Mutex
	known map[string]string
}

func NewLANTunnel() *LANTunnel {
	return &LANTunnel{known: map[string]string{}}
}

func (t *LANTunnel) Open(_ context.Context, node domain.Node) (string, error) {
	if node.ID == "" {
		return "", fmt.Errorf("node id is required")
	}
	if node.Address == "" {
		return "", fmt.Errorf("node address is required")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.known[node.ID] = node.Address
	return node.Address, nil
}

func (t *LANTunnel) Close(_ context.Context, nodeID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.known, nodeID)
	return nil
}

var _ ports.Tunnel = (*LANTunnel)(nil)
