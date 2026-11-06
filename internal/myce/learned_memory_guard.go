package myce

import (
	"maps"
	"time"
)

const learnedMemoryGuardCapability = "learned_cpu_memory_growth_v1"
const learnedMemoryGuardEnforcement = "adaptive native CPU memory guard"

// A learned native CPU reservation is a prediction, not an authored limit.
// The kernel still contains the attempt within physical RAM and the operator's
// explicit allowance. Member limits, Docker and declared thresholds are unchanged.
func learnedMemoryCeilingGB(j *Job, m *Machine) float64 {
	if j == nil || m == nil || j.Spec.Run.Docker || !j.Spec.Needs.cpuOnly() || j.Spec.Needs.Memory != "auto" || j.MemoryEnvelope == nil || j.MemoryEnvelope.Basis != "learned" || j.MemoryEnvelope.Enforcement != learnedMemoryGuardEnforcement {
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

func (w Worker) jobCeilingBytes(j Job) (int64, error) {
	bytes, err := w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	if err != nil {
		return 0, err
	}
	if cap := learnedMemoryCeilingGB(&j, &w.Machine); cap > 0 {
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
}

func (w Worker) learnedGuard(j Job) learnedMemoryGuard {
	limit, _ := w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	cap, _ := memoryBytes(learnedMemoryCeilingGB(&j, &w.Machine))
	hard, _ := memoryBytes(w.Machine.HostMemoryBudgetGB)
	return learnedMemoryGuard{rssGuard: rssGuard{limit: limit}, ceiling: cap, hard: hard, reserve: w.HostMemoryReserveGB}
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
		g.lastBytes, g.lastMembers = 0, 0
		return g.observe(now, roots, bytes, members, err)
	}
	if !maps.Equal(roots, g.identity) || now.Before(g.lastObservation) || now.Sub(g.lastObservation) > 2*processMemorySamplePeriod {
		g.growth, g.previous = memoryGrowth{}, nil
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
	if g.ceiling <= 0 {
		g.previous = host
	}
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
		g.previous = host
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
