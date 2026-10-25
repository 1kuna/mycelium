package myce

import (
	"cmp"
	"fmt"
	"slices"
	"time"
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
// Candidate sets, liveness, scarcity and the global next-pull plan belong to
// this one read-only pass; no job asks what a peer would pick.
type placementScan struct {
	values          map[string]*hostValue
	machines        []*Machine
	sets            map[string]*candidateSet
	live            map[string]bool
	assignments     map[string]*Job
	planBuilds      int
	rankingBuilds   int
	livenessBuilds  int
	candidateBuilds int
}

func (q *Queue) placementScan(scans ...*placementScan) *placementScan {
	if len(scans) > 0 {
		return scans[0]
	}
	return &placementScan{machines: q.plannerMachines(), sets: map[string]*candidateSet{}}
}

func (q *Queue) placementValues(scan *placementScan) map[string]*hostValue {
	if scan.values == nil {
		scan.values = q.hostValues()
		scan.rankingBuilds++
	}
	return scan.values
}

func (q *Queue) valueCompare(a, b *Machine, scans ...*placementScan) (int, bool) {
	return compareHostValue(q.placementValues(q.placementScan(scans...)), a, b)
}

// One queue walk collects the shortest workload-specific loss threshold for
// each worker. Temporary occupancy affects admission, never the value ranking.
func (q *Queue) placementLiveness(scan *placementScan) map[string]bool {
	if scan.live != nil {
		return scan.live
	}
	scan.livenessBuilds++
	timeouts := map[string]time.Duration{}
	add := func(id, priority string) {
		d := q.lossTimeouts[min(2, legacyRank(priority))]
		if old := timeouts[id]; old == 0 || d < old {
			timeouts[id] = d
		}
	}
	for _, j := range q.s.Jobs {
		if j.State == "running" || j.State == "stopping" {
			add(j.Machine, j.Spec.Priority)
		}
	}
	for _, c := range q.calls {
		if c.Machine != "" && c.Error == "" {
			add(c.Machine, c.Priority)
		}
	}
	for _, s := range q.s.ModelSlots {
		if s.State == "loading" || s.InFlight > 0 {
			add(s.Machine, s.Priority)
		}
	}
	scan.live = map[string]bool{}
	now := q.clock.Now()
	for _, m := range scan.machines {
		d := timeouts[m.ID]
		if d == 0 {
			d = q.lossTimeouts[2]
		}
		scan.live[m.ID] = !m.Gone && !m.Leaving && now.Sub(m.LastSeen) < d
	}
	return scan.live
}

func (q *Queue) assignmentPlan(scan *placementScan) map[string]*Job {
	if scan.assignments != nil {
		return scan.assignments
	}
	scan.planBuilds++
	scan.assignments = map[string]*Job{}
	q.placementValues(scan)
	q.placementLiveness(scan)
	sets := make([]*candidateSet, 0, len(scan.machines))
	for _, m := range scan.machines {
		sets = append(sets, q.candidates(m, scan))
	}
	excluded := map[string]bool{}
	for {
		j, selected := q.nextProjection(sets, excluded, scan, true)
		if j == nil {
			break
		}
		scan.assignments[targetKey(selected.m)] = j
		excluded[j.ID] = true
		remaining := sets[:0]
		for _, s := range sets {
			// A check-in offers one next attempt, even on a multi-device worker.
			if s.m.ID != selected.m.ID {
				remaining = append(remaining, s)
			}
		}
		sets = remaining
	}
	return scan.assignments
}

// Shared with queueProjection: every target offers its next admissible fair
// winner after earlier jobs have been claimed. Value only compares targets
// offering that same job. Dispatch requires live free capacity; existing strict
// completion/affinity
// choices can select preparation instead of an immediate start.
func (q *Queue) nextProjection(sets []*candidateSet, excluded map[string]bool, scan *placementScan, immediate bool) (*Job, *candidateSet) {
	var best *Job
	var selected *candidateSet
	for _, set := range sets {
		winner := set.pick(excluded)
		if winner == nil {
			continue
		}
		// Background copies yield to foreground commands in the existing worker.
		// Builds and stopping preparations still fence admission.
		if immediate && (!scan.live[set.m.ID] || !q.workerBatchIdle(set.m) || q.preparationActive(set.m) || q.preparationStopping(set.m) || q.calls[winner.ID] != nil || !fits(winner, set.m, set.m.FreeMemoryGB)) {
			continue
		}
		if best == nil || winner.ID != best.ID && set.before(winner, best) || winner.ID == best.ID && q.projectionTargetBetter(winner, set.m, selected.m, scan, immediate) {
			best, selected = winner, set
		}
	}
	return best, selected
}

// Immediate capacity wins over an unready host's value or identity. Keep the
// existing strict completion/affinity choice within an equal-value band (for
// example preparing software beside an already resident model).
func (q *Queue) projectionTargetBetter(j *Job, a, b *Machine, scan *placementScan, immediate bool) bool {
	if !immediate || environmentReady(j, a) && environmentReady(j, b) {
		return q.targetPlacementBetter(j, a, b, scan)
	}
	ar, br := environmentReady(j, a), environmentReady(j, b)
	if ar != br {
		if value, _ := q.valueCompare(a, b, scan); value != 0 {
			return ar
		}
		if j.Spec.extensionJob() {
			if q.capabilityPlacementBetterWithTie(j, a, b, false, scan) {
				return true
			}
			if q.capabilityPlacementBetterWithTie(j, b, a, false, scan) {
				return false
			}
		} else {
			if q.placementBetterWithTie(j, a, b, false, scan) {
				return true
			}
			if q.placementBetterWithTie(j, b, a, false, scan) {
				return false
			}
		}
		return ar
	}
	return q.targetPlacementBetter(j, a, b, scan)
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
