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
	units, ok := selectedAccelerators(node, acc)
	if !ok || node.MaxUtil <= 0 || node.MaxUtil > 1 {
		return false
	}
	if want.WeightsMB < 0 || want.KVReservedMB < 0 {
		return false
	}

	usedByAccelerator := map[int]int{}
	limitByAccelerator := map[int]int{}
	for _, unit := range units {
		if unit.VRAMTotalMB <= 0 {
			return false
		}
		usable := int(float64(unit.VRAMTotalMB) * node.MaxUtil)
		if node.OOMSeverity == domain.OOMCatastrophic {
			usable -= int(float64(unit.VRAMTotalMB) * catastrophicMargin)
		}
		usedByAccelerator[unit.Index] = unit.VRAMUsedMB
		limitByAccelerator[unit.Index] = usable
	}
	if !addClaimShares(usedByAccelerator, acc, a.headroomByNode[node.ID]) {
		return false
	}
	for _, inst := range existing {
		if inst.NodeID == node.ID && overlaps(inst.AcceleratorSet, acc) {
			if !addClaimShares(usedByAccelerator, inst.AcceleratorSet, inst.Claim) {
				return false
			}
		}
	}
	if !addClaimShares(usedByAccelerator, acc, want) {
		return false
	}
	for index, used := range usedByAccelerator {
		if used > limitByAccelerator[index] {
			return false
		}
	}
	return true
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

func addClaimShares(used map[int]int, acc []int, claim domain.Claim) bool {
	shares, ok := splitClaim(claimTotal(claim), acc)
	if !ok {
		return false
	}
	for index, share := range shares {
		if _, selected := used[index]; selected {
			used[index] += share
		}
	}
	return true
}
