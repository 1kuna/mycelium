package mocks

import (
	"sort"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Allocator struct {
	FitsVal         bool
	CanStackLoadVal bool
	FitsCalls       int
	CanStackCalls   int
}

func (m *Allocator) Fits(node domain.Node, acc []int, _ []domain.ModelInstance, want domain.Claim) bool {
	m.FitsCalls++
	if !m.FitsVal {
		return false
	}
	return validAllocatorRequest(node, acc, &want)
}

func (m *Allocator) CanStackLoad(node domain.Node, acc []int, existing []domain.ModelInstance) bool {
	m.CanStackCalls++
	if !m.CanStackLoadVal {
		return false
	}
	if node.OOMSeverity != domain.OOMCatastrophic {
		return true
	}
	for _, inst := range existing {
		if inst.NodeID == node.ID && overlapsMock(inst.AcceleratorSet, acc) && (inst.Loading || inst.State == domain.InstLoading) {
			return false
		}
	}
	return true
}

var _ ports.Allocator = (*Allocator)(nil)

func validAllocatorRequest(node domain.Node, acc []int, want *domain.Claim) bool {
	if want != nil && (want.WeightsMB < 0 || want.KVReservedMB < 0) {
		return false
	}
	if len(acc) == 0 {
		return false
	}
	seen := map[int]bool{}
	available := map[int]bool{}
	for _, unit := range node.Accelerators {
		available[unit.Index] = true
	}
	for _, index := range acc {
		if seen[index] || !available[index] {
			return false
		}
		seen[index] = true
	}
	return true
}

func overlapsMock(a, b []int) bool {
	left := append([]int(nil), a...)
	right := append([]int(nil), b...)
	sort.Ints(left)
	sort.Ints(right)
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		switch {
		case left[i] == right[j]:
			return true
		case left[i] < right[j]:
			i++
		default:
			j++
		}
	}
	return false
}
