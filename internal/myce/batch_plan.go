package myce

import (
	"math"
	"slices"
)

// These are already eligible configurations, not permissions. The caller must
// recheck live admission before dispatch. Rates are aggregate units/second.
type batchPoint struct {
	count           int
	rate, cpu, host float64
}
type batchComponent struct {
	id, host                     string
	ready, cpuBudget, hostBudget float64
	points                       []batchPoint
}
type batchAllocation struct {
	component     string
	pieces, count int
	seconds       float64
}

// A fixed measured count supplies full waves; a measured smaller count handles
// the tail. Never extrapolate an unmeasured occupancy from isolated speed.
func batchCapacity(m batchComponent, p batchPoint, units, horizon float64, limit int) int {
	available := horizon - m.ready
	if available < 0 {
		return 0
	}
	service := units * float64(p.count) / p.rate
	waves := math.Floor((available + 1e-9) / service)
	if waves >= float64(limit) {
		return limit
	}
	capacity := int(waves) * p.count
	remaining := available - waves*service
	for _, tail := range m.points {
		if tail.count < p.count && units*float64(tail.count)/tail.rate <= remaining+1e-9 {
			capacity = max(capacity, int(waves)*p.count+tail.count)
		}
	}
	return min(limit, capacity)
}

type batchChoice struct {
	cpu, host   float64
	capacity    int
	allocations []batchAllocation
}

// Sparse host enumeration keeps only nondominated CPU/RAM/capacity choices.
// This counts a shared host once even when it supplies several components.
func batchAtHorizon(components []batchComponent, n int, units, horizon float64) []batchAllocation {
	byHost := map[string][]batchComponent{}
	for _, m := range components {
		byHost[m.host] = append(byHost[m.host], m)
	}
	hosts := make([]string, 0, len(byHost))
	for host := range byHost {
		hosts = append(hosts, host)
	}
	slices.Sort(hosts)
	var result []batchAllocation
	for _, host := range hosts {
		choices := []batchChoice{{}}
		for _, m := range byHost[host] {
			next := append([]batchChoice(nil), choices...) // zero capacity is legal
			for _, old := range choices {
				for _, p := range m.points {
					capacity := batchCapacity(m, p, units, horizon, n)
					if capacity == 0 || old.cpu+p.cpu > m.cpuBudget+1e-9 || old.host+p.host > m.hostBudget+1e-9 {
						continue
					}
					candidate := batchChoice{cpu: old.cpu + p.cpu, host: old.host + p.host, capacity: min(n, old.capacity+capacity), allocations: append(slices.Clone(old.allocations), batchAllocation{component: m.id, pieces: capacity, count: p.count})}
					dominated := false
					for _, other := range next {
						if other.cpu <= candidate.cpu && other.host <= candidate.host && other.capacity >= candidate.capacity {
							dominated = true
							break
						}
					}
					if !dominated {
						next = slices.DeleteFunc(next, func(other batchChoice) bool {
							return candidate.cpu <= other.cpu && candidate.host <= other.host && candidate.capacity >= other.capacity
						})
						next = append(next, candidate)
					}
				}
			}
			choices = next
		}
		best := choices[0]
		for _, choice := range choices {
			if choice.capacity > best.capacity || choice.capacity == best.capacity && (choice.cpu < best.cpu || choice.cpu == best.cpu && choice.host < best.host) {
				best = choice
			}
		}
		result = append(result, best.allocations...)
	}
	capacity := 0
	for _, a := range result {
		capacity += a.pieces
	}
	if capacity < n {
		return nil
	}
	return result
}

func batchFinish(m batchComponent, count, n int, units float64) (float64, int) {
	best, next := math.Inf(1), 0
	for _, p := range m.points {
		if p.count != count {
			continue
		}
		waves, tail := n/p.count, n%p.count
		seconds := float64(waves) * units * float64(p.count) / p.rate
		first := p.count
		if tail > 0 {
			tailSeconds, tailCount := math.Inf(1), 0
			// A partial last wave must itself be an allowed measured count.
			// When no exact point exists, several serial items remain legal.
			for _, t := range m.points {
				if t.count <= tail && tail%t.count == 0 {
					s := units * float64(tail) / t.rate
					if s < tailSeconds {
						tailSeconds, tailCount = s, t.count
					}
				}
			}
			seconds += tailSeconds
			if waves == 0 {
				first = tailCount
			}
		}
		if seconds < best {
			best, next = seconds, first
		}
	}
	return m.ready + best, next
}

// Discrete water-filling finds the smallest feasible completion horizon. The
// bounded binary search is over time, but capacities and assignments are integer.
func planBatch(components []batchComponent, n int, units float64) []batchAllocation {
	if n < 1 || !validRate(units) {
		return nil
	}
	components = slices.Clone(components)
	slices.SortFunc(components, func(a, b batchComponent) int {
		if a.id < b.id {
			return -1
		}
		if a.id > b.id {
			return 1
		}
		return 0
	})
	upper := math.Inf(1)
	for _, m := range components {
		for _, p := range m.points {
			if p.count == 1 && p.cpu <= m.cpuBudget && p.host <= m.hostBudget {
				upper = min(upper, m.ready+float64(n)*units/p.rate)
			}
		}
	}
	if math.IsInf(upper, 1) {
		return nil
	}
	lower := 0.0
	for range 50 {
		middle := (lower + upper) / 2
		if batchAtHorizon(components, n, units, middle) == nil {
			lower = middle
		} else {
			upper = middle
		}
	}
	allocations := batchAtHorizon(components, n, units, upper+1e-8)
	remaining := n
	for i := range allocations {
		a := &allocations[i]
		a.pieces = min(remaining, a.pieces)
		remaining -= a.pieces
		if a.pieces == 0 {
			continue
		}
		for _, m := range components {
			if m.id == a.component {
				// Re-evaluate count after integer allocation, particularly short tails.
				a.seconds = math.Inf(1)
				ceiling := a.count
				for _, p := range m.points {
					if p.count > ceiling {
						continue
					}
					seconds, next := batchFinish(m, p.count, a.pieces, units)
					if seconds < a.seconds {
						a.seconds, a.count = seconds, next
					}
				}
			}
		}
	}
	return slices.DeleteFunc(allocations, func(a batchAllocation) bool { return a.pieces == 0 })
}
