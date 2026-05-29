package contract

import (
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func RunAllocatorConformance(t *testing.T, name string, newAllocator func() ports.Allocator, node domain.Node, want domain.Claim) {
	t.Run(name+"/empty_unit_fits_small_claim", func(t *testing.T) {
		allocator := newAllocator()
		if !allocator.Fits(node, []int{0}, nil, want) {
			t.Fatalf("empty unit should fit %+v on %+v", want, node.Accelerators)
		}
	})
}
