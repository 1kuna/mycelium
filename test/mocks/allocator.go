package mocks

import (
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Allocator struct {
	FitsVal         bool
	CanStackLoadVal bool
	FitsCalls       int
	CanStackCalls   int
}

func (m *Allocator) Fits(domain.Node, []int, []domain.ModelInstance, domain.Claim) bool {
	m.FitsCalls++
	return m.FitsVal
}

func (m *Allocator) CanStackLoad(domain.Node, []int, []domain.ModelInstance) bool {
	m.CanStackCalls++
	return m.CanStackLoadVal
}

var _ ports.Allocator = (*Allocator)(nil)
