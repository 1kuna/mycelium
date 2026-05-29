package lease

import "mycelium/internal/domain"

func WithReservedHeadroom(nodeID string, claim domain.Claim) Option {
	return func(a *Allocator) {
		a.headroomByNode[nodeID] = claim
	}
}
