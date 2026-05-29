package contract

import (
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

func RunAllocatorConformance(t *testing.T, name string, newAllocator func() ports.Allocator, node domain.Node, want domain.Claim) {
	t.Run(name+"/empty_unit_fits_small_claim", func(t *testing.T) {
		allocator := newAllocator()
		assert.True(t, allocator.Fits(node, []int{0}, nil, want), "empty unit should fit %+v on %+v", want, node.Accelerators)
	})
}
