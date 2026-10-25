package myce

import (
	"cmp"
	"fmt"
	"slices"
)

// Values belong to physical hosts, not worker rows, devices or free capacity.
// This read-only inventory projection does no I/O and uses the queue Clock.
type hostValue struct {
	ram          float64
	accelerators map[string]float64
	bands        map[capabilityBand]bool
	known        bool
	vector       []int
}
type capabilityBand struct {
	backend  string
	capacity float64
}

func (q *Queue) hostValues() map[string]*hostValue {
	hosts := map[string]*hostValue{}
	for _, m := range q.s.Machines {
		// Temporary availability affects admission, not the measured supply.
		// Replacement records no longer represent a participating host.
		if m.Replacement != nil {
			continue
		}
		id := hostID(m)
		h := hosts[id]
		if h == nil {
			h = &hostValue{known: true, accelerators: map[string]float64{}, bands: map[capabilityBand]bool{}}
			hosts[id] = h
		}
		p := m.CapabilityProfile
		if p == nil || p.SystemMemoryBytes == 0 || (p.MemoryLayout != "unified" && p.MemoryLayout != "separate") || m.HostMemoryPolicyMismatch {
			h.known = false
			continue
		}
		ram := float64(p.SystemMemoryBytes) / (1 << 30)
		for _, allowance := range []float64{m.HostMemoryBudgetGB, m.EffectiveHostMemoryBudgetGB} {
			if allowance > 0 {
				ram = min(ram, allowance)
			}
		}
		if h.ram == 0 {
			h.ram = ram
		} else {
			h.ram = min(h.ram, ram)
		}
		if m.Devices != nil {
			for _, d := range m.Devices {
				if d.Platform == "cpu" {
					continue
				}
				// Reuse the inventory view: uncapped unified BudgetGB is live
				// free RAM, not a stable allowance or a capability tier.
				target := d.machine(*m)
				capacity := target.TotalMemoryGB
				if !d.UnifiedMemory {
					capacity = min(capacity, max(0, d.TotalMemoryGB-d.ReserveMemoryGB))
				}
				if d.UnifiedMemory || p.MemoryLayout == "unified" {
					capacity = min(capacity, ram)
				}
				if capacity <= 0 {
					h.known = false
					continue
				}
				h.accelerators[d.Platform] = max(h.accelerators[d.Platform], capacity)
				h.bands[capabilityBand{d.Platform, capacity}] = true
			}
		} else if m.GPU != "cpu" {
			capacity, _, ok := capabilityCapacity(m)
			if m.TotalMemoryGB > 0 {
				capacity = min(capacity, max(0, m.TotalMemoryGB-m.ReservedMemoryGB))
			}
			if p.MemoryLayout == "unified" {
				capacity = min(capacity, ram)
			}
			if !ok || capacity <= 0 {
				h.known = false
				continue
			}
			h.accelerators[m.GPU] = max(h.accelerators[m.GPU], capacity)
			h.bands[capabilityBand{m.GPU, capacity}] = true
		}
	}
	bands := map[capabilityBand]bool{}
	minimum := 0.0
	for _, h := range hosts {
		if !h.known {
			continue
		}
		if minimum == 0 || h.ram < minimum {
			minimum = h.ram
		}
		for band := range h.bands {
			bands[band] = true
		}
	}
	for _, h := range hosts {
		if h.known && h.ram > minimum {
			bands[capabilityBand{"", h.ram}] = true
		}
	}
	supplies := func(h *hostValue, b capabilityBand) bool {
		if !h.known {
			return false
		}
		if b.backend == "" {
			return h.ram >= b.capacity
		}
		return h.accelerators[b.backend] >= b.capacity
	}
	for _, h := range hosts {
		h.vector = make([]int, len(hosts))
	}
	for band := range bands {
		count := 0
		for _, h := range hosts {
			if supplies(h, band) {
				count++
			}
		}
		for _, h := range hosts {
			if supplies(h, band) {
				h.vector[count-1]++
			}
		}
	}
	return hosts
}

// Equal known values leave measured completion/affinity to the existing planner.
// Unknown inventory is never treated as an empty (cheap) vector.
func compareHostValue(values map[string]*hostValue, a, b *Machine) (int, bool) {
	av, bv := values[hostID(a)], values[hostID(b)]
	ak, bk := av != nil && av.known, bv != nil && bv.known
	if ak != bk {
		if ak {
			return -1, true
		}
		return 1, true
	}
	if !ak {
		return 0, false
	}
	if c := slices.Compare(av.vector, bv.vector); c != 0 {
		return c, true
	}
	return cmp.Compare(av.ram, bv.ram), true
}

// placementScan is discarded after any placement/inventory/policy mutation.
// Scarcity and peer fair-turn winners are computed at most once in this pass.
// Peer scans omit steering, so admission checks cannot recurse into placement.
type placementScan struct {
	values   map[string]*hostValue
	machines []*Machine
	winners  map[string]*Job
}

func (q *Queue) placementScan(scans ...*placementScan) *placementScan {
	if len(scans) > 0 {
		return scans[0]
	}
	return &placementScan{machines: q.plannerMachines(), winners: map[string]*Job{}}
}

func (q *Queue) placementValues(scan *placementScan) map[string]*hostValue {
	if scan.values == nil {
		scan.values = q.hostValues()
	}
	return scan.values
}

func (q *Queue) valueCompare(a, b *Machine, scans ...*placementScan) (int, bool) {
	return compareHostValue(q.placementValues(q.placementScan(scans...)), a, b)
}

func (q *Queue) peerWinner(m *Machine, scan *placementScan) *Job {
	key := targetKey(m)
	if winner, ok := scan.winners[key]; ok {
		return winner
	}
	winner := q.candidatesForPlacement(m, scan, false).pick(nil)
	scan.winners[key] = winner
	return winner
}

func valueReason(h *hostValue, m *Machine) string {
	if h == nil || !h.known {
		return "host value unknown; normal admission"
	}
	return fmt.Sprintf("preserve scarce machines: host %s scarcity %v (1-host bands first), usable RAM %g GiB; currently admissible targets only", hostID(m), h.vector, h.ram)
}
func (q *Queue) recordValuePlacement(j *Job, m *Machine, values map[string]*hostValue) {
	reason := valueReason(values[hostID(m)], m)
	if j.Timing == nil || j.Timing.Execution.Attempt != j.Attempt {
		j.Timing = &WorkTiming{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: m.ID, Session: m.Session}
	}
	if j.Timing.PlacementReason != "" {
		reason += "; " + j.Timing.PlacementReason
	}
	j.Timing.PlacementReason = reason
}
