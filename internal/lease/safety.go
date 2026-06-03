package lease

import (
	"sort"

	"mycelium/internal/domain"
)

func selectedCapacity(node domain.Node, acc []int) (totalMB int, usedMB int, ok bool) {
	units, ok := selectedAccelerators(node, acc)
	if !ok {
		return 0, 0, false
	}
	for _, unit := range units {
		totalMB += unit.VRAMTotalMB
		usedMB += unit.VRAMUsedMB
	}
	return totalMB, usedMB, true
}

func selectedAccelerators(node domain.Node, acc []int) ([]domain.Accelerator, bool) {
	if len(acc) == 0 {
		return nil, false
	}
	byIndex := map[int]domain.Accelerator{}
	for _, accelerator := range node.Accelerators {
		byIndex[accelerator.Index] = accelerator
	}
	seen := map[int]struct{}{}
	units := make([]domain.Accelerator, 0, len(acc))
	for _, want := range acc {
		if _, duplicate := seen[want]; duplicate {
			return nil, false
		}
		unit, ok := byIndex[want]
		if !ok {
			return nil, false
		}
		seen[want] = struct{}{}
		units = append(units, unit)
	}
	sort.Slice(units, func(i, j int) bool { return units[i].Index < units[j].Index })
	return units, true
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

func splitClaim(total int, acc []int) (map[int]int, bool) {
	if total < 0 || len(acc) == 0 {
		return nil, false
	}
	sorted := append([]int(nil), acc...)
	sort.Ints(sorted)
	seen := map[int]struct{}{}
	for _, index := range sorted {
		if _, duplicate := seen[index]; duplicate {
			return nil, false
		}
		seen[index] = struct{}{}
	}
	shares := map[int]int{}
	base := total / len(sorted)
	remainder := total % len(sorted)
	for i, index := range sorted {
		shares[index] = base
		if i < remainder {
			shares[index]++
		}
	}
	return shares, true
}

func reservationClaim(r domain.Reservation) domain.Claim {
	if r.Kind != domain.ReservationHeadroom {
		return domain.Claim{}
	}
	return r.Headroom
}
