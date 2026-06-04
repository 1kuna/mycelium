package mocks

import (
	"context"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type HardwareDetector struct {
	Node  domain.Node
	Err   error
	Calls []string
}

func (d *HardwareDetector) Detect(_ context.Context, seed domain.Node) (domain.Node, error) {
	d.Calls = append(d.Calls, "detect:"+seed.ID)
	if d.Err != nil {
		return domain.Node{}, d.Err
	}
	node := d.Node
	if node.ID == "" {
		node = seed
	}
	if node.DiskTotalMB == 0 {
		node.DiskTotalMB = 1000
		node.DiskFreeMB = 500
	}
	if node.DiskMinFreeRatio == 0 {
		node.DiskMinFreeRatio = domain.DefaultDiskMinFreeRatio
	}
	return node, nil
}

var _ ports.HardwareDetector = (*HardwareDetector)(nil)
