package lease

import "mycelium/internal/domain"

func selectedCapacity(node domain.Node, acc []int) (totalMB int, usedMB int, ok bool) {
	if len(acc) == 0 {
		return 0, 0, false
	}
	for _, want := range acc {
		found := false
		for _, got := range node.Accelerators {
			if got.Index == want {
				totalMB += got.VRAMTotalMB
				usedMB += got.VRAMUsedMB
				found = true
				break
			}
		}
		if !found {
			return 0, 0, false
		}
	}
	return totalMB, usedMB, true
}

func overlaps(left, right []int) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	seen := make(map[int]struct{}, len(left))
	for _, v := range left {
		seen[v] = struct{}{}
	}
	for _, v := range right {
		if _, ok := seen[v]; ok {
			return true
		}
	}
	return false
}

func reservationClaim(r domain.Reservation) domain.Claim {
	if r.Kind != domain.ReservationHeadroom {
		return domain.Claim{}
	}
	return r.Headroom
}
