package fixtures

import (
	"time"

	"mycelium/internal/domain"
)

func MakeNode(opts ...func(*domain.Node)) domain.Node {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	n := domain.Node{
		ID:            "node_test",
		Name:          "test-node",
		Address:       "127.0.0.1:50000",
		OS:            "linux",
		Labels:        map[string]string{"gpu.vendor": "nvidia"},
		MaxUtil:       0.90,
		OOMSeverity:   domain.OOMSoft,
		Status:        domain.NodeReady,
		HeartbeatAt:   now,
		UnifiedMemory: false,
		Accelerators: []domain.Accelerator{{
			Index:       0,
			Vendor:      "nvidia",
			Kind:        "rtx4090",
			VRAMTotalMB: 24576,
		}},
		SpeedClass: domain.SpeedClass{TokensPerSecRef: 90, Source: "class-default", ProbedAt: now},
	}
	for _, opt := range opts {
		opt(&n)
	}
	return n
}

func WithNodeID(id string) func(*domain.Node) {
	return func(n *domain.Node) { n.ID = id }
}

func WithVRAM(mb int) func(*domain.Node) {
	return func(n *domain.Node) { n.Accelerators[0].VRAMTotalMB = mb }
}

func WithUsedVRAM(mb int) func(*domain.Node) {
	return func(n *domain.Node) { n.Accelerators[0].VRAMUsedMB = mb }
}

func WithMaxUtil(u float64) func(*domain.Node) {
	return func(n *domain.Node) { n.MaxUtil = u }
}

func Catastrophic(n *domain.Node) {
	n.OOMSeverity = domain.OOMCatastrophic
}

func Maintenance(n *domain.Node) {
	n.Status = domain.NodeMaintenance
}

func MakeSparkNode(opts ...func(*domain.Node)) domain.Node {
	base := []func(*domain.Node){
		func(n *domain.Node) {
			n.ID = "node_spark"
			n.Name = "dgx-spark"
			n.OOMSeverity = domain.OOMCatastrophic
			n.UnifiedMemory = true
			n.Labels = map[string]string{
				"gpu.vendor":   "nvidia",
				"gpu.kind":     "gb10",
				"memory.class": "huge",
			}
			n.Accelerators = []domain.Accelerator{{
				Index:         0,
				Vendor:        "nvidia",
				Kind:          "gb10",
				VRAMTotalMB:   131072,
				UnifiedMemory: true,
			}}
			n.SpeedClass.TokensPerSecRef = 145
		},
	}
	return MakeNode(append(base, opts...)...)
}

func Make4090Node(opts ...func(*domain.Node)) domain.Node {
	base := []func(*domain.Node){
		func(n *domain.Node) {
			n.ID = "node_4090a"
			n.Name = "rtx4090-box"
			n.Labels = map[string]string{"gpu.vendor": "nvidia", "gpu.kind": "rtx4090"}
			n.SpeedClass.TokensPerSecRef = 90
		},
	}
	return MakeNode(append(base, opts...)...)
}
