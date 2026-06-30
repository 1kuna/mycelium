package lease

import (
	"sort"

	"mycelium/internal/domain"
)

const maxInt = int(^uint(0) >> 1)

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

func splitClaim(total int, acc []int, capacityByAccelerator map[int]int) (map[int]int, bool) {
	if total < 0 || len(acc) == 0 {
		return nil, false
	}
	sorted := append([]int(nil), acc...)
	sort.Ints(sorted)
	seen := map[int]struct{}{}
	totalCapacity := 0
	for _, index := range sorted {
		if _, duplicate := seen[index]; duplicate {
			return nil, false
		}
		capacity := capacityByAccelerator[index]
		if capacity <= 0 {
			return nil, false
		}
		if totalCapacity > maxInt-capacity {
			return nil, false
		}
		totalCapacity += capacity
		seen[index] = struct{}{}
	}
	shares := map[int]int{}
	remainders := make([]claimRemainder, 0, len(sorted))
	assigned := 0
	for _, index := range sorted {
		capacity := capacityByAccelerator[index]
		if total != 0 && capacity > maxInt/total {
			return nil, false
		}
		product := total * capacity
		share := product / totalCapacity
		shares[index] = share
		assigned += share
		remainders = append(remainders, claimRemainder{
			index:     index,
			remainder: product % totalCapacity,
		})
	}
	sort.Slice(remainders, func(i, j int) bool {
		if remainders[i].remainder == remainders[j].remainder {
			return remainders[i].index < remainders[j].index
		}
		return remainders[i].remainder > remainders[j].remainder
	})
	for remaining, i := total-assigned, 0; remaining > 0; remaining, i = remaining-1, i+1 {
		shares[remainders[i%len(remainders)].index]++
	}
	return shares, true
}

type claimRemainder struct {
	index     int
	remainder int
}

func reservationClaim(r domain.Reservation) domain.Claim {
	if r.Kind != domain.ReservationHeadroom {
		return domain.Claim{}
	}
	return r.Headroom
}
