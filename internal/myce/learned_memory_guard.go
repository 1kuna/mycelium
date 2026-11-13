package myce

import (
	"maps"
	"time"
)

const learnedMemoryGuardCapability = "learned_cpu_memory_growth_v1"
const learnedMemoryGuardEnforcement = "adaptive native CPU memory guard"

// Learned and bounded exploratory CPU reservations are predictions, not authored
// limits. Cold CPU probes use the same physical guard: residual admission
// headroom is not a measured upper bound on an unknown command.
// The kernel still contains the attempt within physical RAM and the operator's
// explicit allowance. Member limits and declared thresholds are unchanged.
func adaptiveMemoryCeilingGB(j *Job, m *Machine) float64 {
	if j == nil || m == nil || !j.Spec.Needs.cpuOnly() || j.Spec.Needs.Memory != "auto" || j.MemoryEnvelope == nil || j.MemoryEnvelope.Enforcement != learnedMemoryGuardEnforcement {
		return 0
	}
	e := j.MemoryEnvelope
	if e.Basis != "learned" && e.Basis != "exploratory" {
		return 0
	}
	cap := m.HostMemoryBudgetGB
	if p := m.CapabilityProfile; p != nil && p.SystemMemoryBytes > 0 {
		physical := float64(p.SystemMemoryBytes) / (1 << 30)
		if cap <= 0 || physical < cap {
			cap = physical
		}
	}
	if !validMemoryValue(cap) || cap <= hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) {
		return 0
	}
	return cap
}

// Metal buffers are invisible to RSS but charged to phys_footprint (#1044), so
// Metal work gets the same sampled guard over the owned tree's footprint. The
// limit is the whole declared envelope in unified memory: device plus host plus
// slack. Auto-sized Metal envelopes were learned from RSS, which never saw
// the buffers, so only declared sizes are enforced.
const footprintGuardQuality = "owned_session_footprint"
const metalFootprintGuardCapability = "metal_footprint_guard_v1"

func (w Worker) footprintGuarded(j Job) bool {
	n := j.Spec.Needs
	return w.processMemory != nil && w.processMemory.OwnedFootprint != nil && backendFor(w.Machine.GPU).processFootprint &&
		!n.cpuOnly() && n.Memory != "auto" && n.HostMemoryGB > 0
}

func (w Worker) footprintGuard(j Job) learnedMemoryGuard {
	m := w.Machine
	m.UnifiedMemory, m.HostMemoryMeasured = true, true
	limit, err := memoryBytes(hostNeed(&m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB))
	if err != nil {
		limit = 0
	}
	return learnedMemoryGuard{rssGuard: rssGuard{limit: limit}, reserve: w.HostMemoryReserveGB}
}

// Without a cgroup, the sampled guard's own measurement is the attempt's
// current memory. Report it wherever the queue can charge the job by use:
// auto CPU work, declared CPU work under the Darwin RSS guard (#1296) and
// guarded Metal work. Other jobs keep sending none, so unguarded RSS never
// earns resident credit.
func (w Worker) reportsProcessSample(j Job) bool {
	n := j.Spec.Needs
	return adaptiveMemoryCeilingGB(&j, &w.Machine) > 0 || w.footprintGuarded(j) ||
		w.Machine.MemoryEnforcement == darwinRSSGuardStatus && n.cpuOnly() && n.Memory != "auto" && n.HostMemoryGB > 0
}

func (w Worker) jobCeilingBytes(j Job) (int64, error) {
	bytes, err := w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	if err != nil {
		return 0, err
	}
	if cap := adaptiveMemoryCeilingGB(&j, &w.Machine); cap > 0 {
		if _, err := MemoryCeilingConfig(cap, 0, w.MemorySwapMaxBytes); err != nil {
			return 0, err
		}
		return memoryBytes(cap)
	}
	return bytes, nil
}

type learnedMemoryGuard struct {
	rssGuard
	ceiling         int64
	hard            int64
	reserve         float64
	growth          memoryGrowth
	previous        *gpuHostState
	identity        map[int]string
	lastObservation time.Time
	lastBytes       uint64
	lastMembers     int
	room            *firstRunFloorRoom
}

func (w Worker) learnedGuard(j Job) learnedMemoryGuard {
	limit, _ := w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	cap, _ := memoryBytes(adaptiveMemoryCeilingGB(&j, &w.Machine))
	hard, _ := memoryBytes(w.Machine.HostMemoryBudgetGB)
	g := learnedMemoryGuard{rssGuard: rssGuard{limit: limit}, ceiling: cap, hard: hard, reserve: w.HostMemoryReserveGB}
	if cap > 0 && floorCharged(j.MemoryEnvelope) {
		key, others := jobKey(j), w.hostChargesExcept
		g.room = &firstRunFloorRoom{others: func() (float64, float64) {
			if others == nil {
				return 0, 0
			}
			return others(key)
		}}
	}
	return g
}

// Use current pressure, rather than latching unrelated pressure for the rest of
// a long attempt. The measured growth horizon protects the next sample; the
// configured reserve protects the owner's RAM. No workload-size multiplier is
// used. Missing pressure cannot prove contention; the physical ceiling remains.
func (g *learnedMemoryGuard) observeMemory(now time.Time, roots map[int]string, bytes uint64, members int, err error, host *gpuHostState) *RSSLimitEvidence {
	valid := len(roots) > 0 && members > 0 && err == nil
	for pid, birth := range roots {
		valid = valid && pid > 0 && birth != ""
	}
	if !valid {
		g.growth, g.previous = memoryGrowth{}, nil
		g.room.reset()
		g.lastBytes, g.lastMembers = 0, 0
		return g.observe(now, roots, bytes, members, err)
	}
	// A new attempt identity or backward clock restarts the baselines. A late
	// sample does not: a swap or headroom delta across a longer interval is
	// still evidence, and the sampler runs late precisely when the host thrashes.
	if !maps.Equal(roots, g.identity) || now.Before(g.lastObservation) {
		g.growth, g.previous = memoryGrowth{}, nil
		g.room.reset()
		g.identity = maps.Clone(roots)
	}
	// A fork/growth burst cannot safely spend the ordinary spike-filter grace
	// when measured physical headroom is disappearing before it can end. RSS
	// and child growth establish attempt activity; only actual host headroom
	// loss supplies the consumption rate. Sharing alone cannot trigger this.
	elapsed := now.Sub(g.lastObservation)
	urgent := false
	if before := g.previous; before != nil && host != nil && elapsed > 0 &&
		validMemoryValue(before.available) && validMemoryValue(host.available) &&
		host.available < before.available && bytes > g.lastBytes && members > g.lastMembers {
		lossRate := (before.available - host.available) / elapsed.Seconds()
		// Budget one observed sampling interval plus the existing grace. These
		// are the delays this forecast prevents us from waiting through.
		horizon := elapsed + processMemoryGrace
		urgent = host.available <= g.reserve+lossRate*horizon.Seconds()
	}
	g.lastObservation, g.lastBytes, g.lastMembers = now, bytes, members
	// Keep pressure and timestamp baselines consecutive even when a hard
	// allowance bypasses the adaptive pressure branch. A missing read must
	// clear the baseline rather than bridge an unobserved interval.
	defer func() { g.previous = host }()
	if urgent && !g.exceeded && g.limit > 0 && bytes > uint64(g.limit) {
		g.exceeded = true
		return &RSSLimitEvidence{ObservedBytes: bytes, LimitBytes: g.limit, Quality: "owned_session_rss"}
	}
	if g.ceiling <= 0 {
		return g.observe(now, roots, bytes, members, err)
	}
	limit := g.limit
	defer func() { g.limit = limit }()
	if g.hard > 0 && bytes > uint64(g.hard) {
		g.limit = g.hard
	} else if g.room != nil {
		// The floor is charged and never a stop; the room is the limit.
		g.limit = max(limit, g.room.limitBytes(now, bytes, host, g.reserve, float64(g.ceiling)/(1<<30)))
	} else {
		pressure := false
		if host != nil && validMemoryValue(host.available) {
			// Measure the loss of actual host headroom. RSS growth may be shared
			// mappings becoming resident in many tasks at once, not physical use.
			margin := g.growth.observe(now, max(0, float64(g.ceiling)/(1<<30)-host.available))
			pressure = host.level >= 2 || host.available <= g.reserve+margin
			if before := g.previous; before != nil {
				pressure = pressure || host.oom > before.oom || host.swapOut > before.swapOut || host.swap > before.swap || host.psiTotalUS > before.psiTotalUS
			}
		} else {
			g.growth = memoryGrowth{}
		}
		if !pressure {
			// Reset the sustained-overage window while measured room is ample,
			// or pressure is unknown. A later pressure sample starts a new window.
			return g.observe(now, roots, 0, members, err)
		}
	}
	return g.observe(now, roots, bytes, members, err)
}

type processResidentSample struct {
	gb, growth, rate float64
	at               time.Time
	observed         bool
}

// Reuse the probe forecast without granting anonymous-resident credit to RSS.
// Shared pages may be counted repeatedly: they can increase a charge, never
// reduce live growth room. The actor ages this sample through the common path.
func (p *process) currentProcessHostMemory() *CurrentHostMemory {
	s := p.rssMemory.Load()
	if s == nil {
		return nil
	}
	gb := s.gb
	return &CurrentHostMemory{GrowthObserved: s.observed, GrowthRateGB: s.rate,
		FootprintGB: &gb, GrowthGB: s.growth, sampledAt: s.at}
}
