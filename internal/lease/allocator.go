package lease

import (
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const catastrophicMargin = 0.05

type Allocator struct {
	headroomByNode map[string]domain.Claim
}

type Option func(*Allocator)

func NewAllocator(opts ...Option) *Allocator {
	a := &Allocator{headroomByNode: map[string]domain.Claim{}}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *Allocator) Fits(node domain.Node, acc []int, existing []domain.ModelInstance, want domain.Claim) bool {
	total, used, ok := selectedCapacity(node, acc)
	if !ok || total <= 0 || node.MaxUtil <= 0 || node.MaxUtil > 1 {
		return false
	}
	if want.WeightsMB < 0 || want.KVReservedMB < 0 {
		return false
	}

	claimUsed := claimTotal(a.headroomByNode[node.ID])
	for _, inst := range existing {
		if inst.NodeID == node.ID && overlaps(inst.AcceleratorSet, acc) {
			claimUsed += claimTotal(inst.Claim)
		}
	}

	usable := int(float64(total) * node.MaxUtil)
	if node.OOMSeverity == domain.OOMCatastrophic {
		usable -= int(float64(total) * catastrophicMargin)
	}
	return used+claimUsed+claimTotal(want) <= usable
}

func (a *Allocator) CanStackLoad(node domain.Node, acc []int, existing []domain.ModelInstance) bool {
	if node.OOMSeverity != domain.OOMCatastrophic {
		return true
	}
	for _, inst := range existing {
		if inst.NodeID == node.ID && overlaps(inst.AcceleratorSet, acc) && (inst.Loading || inst.State == domain.InstLoading) {
			return false
		}
	}
	return true
}

var _ ports.Allocator = (*Allocator)(nil)

func claimTotal(c domain.Claim) int {
	return c.WeightsMB + c.KVReservedMB
}
