package mocks

import (
	"context"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Tunnel struct {
	Addr  string
	Err   error
	Calls []string
}

func (t *Tunnel) Open(_ context.Context, node domain.Node) (string, error) {
	t.Calls = append(t.Calls, "open:"+node.ID)
	if t.Err != nil {
		return "", t.Err
	}
	if t.Addr != "" {
		return t.Addr, nil
	}
	return node.Address, nil
}

func (t *Tunnel) Close(_ context.Context, nodeID string) error {
	t.Calls = append(t.Calls, "close:"+nodeID)
	return t.Err
}

var _ ports.Tunnel = (*Tunnel)(nil)
