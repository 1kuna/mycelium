package estimate

import "mycelium/internal/domain"

func unifiedMemoryPressureMB(claim domain.Claim) int {
	return claim.WeightsMB + claim.KVReservedMB
}
