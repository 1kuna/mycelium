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

	t.Run(name+"/rejects_invalid_mechanical_inputs", func(t *testing.T) {
		allocator := newAllocator()
		assert.True(t, !allocator.Fits(node, nil, nil, want), "empty accelerator set should not fit")
		assert.True(t, !allocator.Fits(node, []int{99}, nil, want), "unknown accelerator should not fit")
		assert.True(t, !allocator.Fits(node, []int{0, 0}, nil, want), "duplicate accelerator set should not fit")
		assert.True(t, !allocator.Fits(node, []int{0}, nil, domain.Claim{WeightsMB: -1}), "negative claim should not fit")
	})

	t.Run(name+"/catastrophic_loading_unit_does_not_stack", func(t *testing.T) {
		allocator := newAllocator()
		catastrophic := node
		catastrophic.OOMSeverity = domain.OOMCatastrophic
		loading := domain.ModelInstance{
			ID:             "inst-loading",
			NodeID:         node.ID,
			AcceleratorSet: []int{0},
			Claim:          want,
			State:          domain.InstLoading,
			Loading:        true,
		}
		assert.True(t, !allocator.CanStackLoad(catastrophic, []int{0}, []domain.ModelInstance{loading}), "catastrophic loading unit should not stack")
	})
}
