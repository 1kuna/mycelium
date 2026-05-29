package mocks

import (
	"context"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Discovery struct {
	Nodes []domain.Node
	Err   error
	Calls []string
}

func (d *Discovery) Announce(_ context.Context, node domain.Node) error {
	d.Calls = append(d.Calls, "announce:"+node.ID)
	if d.Err != nil {
		return d.Err
	}
	d.Nodes = append(d.Nodes, node)
	return nil
}

func (d *Discovery) Discover(context.Context) ([]domain.Node, error) {
	d.Calls = append(d.Calls, "discover")
	if d.Err != nil {
		return nil, d.Err
	}
	return append([]domain.Node(nil), d.Nodes...), nil
}

var _ ports.Discovery = (*Discovery)(nil)
