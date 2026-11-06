package myce

import "time"

const learnedMemoryGuardCapability = "learned_cpu_memory_growth_v1"
const learnedMemoryGuardEnforcement = "adaptive native CPU memory guard"

// A learned native CPU reservation is a prediction, not an authored limit.
// The kernel still contains the attempt within physical RAM and the operator's
// explicit allowance. Member limits, Docker and declared jobs are unchanged.
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
	if cap := learnedMemoryCeilingGB(&j, &w.Machine); cap > 0 {
		return memoryBytes(cap)
	}
	return w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
}

type learnedMemoryGuard struct {
	rssGuard
	ceiling  int64
	hard     int64
	reserve  float64
	growth   memoryGrowth
	previous *gpuHostState
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
	if g.ceiling <= 0 {
		return g.observe(now, roots, bytes, members, err)
	}
	if err != nil || members <= 0 {
		g.growth, g.previous = memoryGrowth{}, nil
		return g.observe(now, roots, bytes, members, err)
	}
	gb := float64(bytes) / (1 << 30)
	margin := g.growth.observe(now, gb)
	limit := g.limit
	defer func() { g.limit = limit }()
	if g.hard > 0 && bytes > uint64(g.hard) {
		g.limit = g.hard
	} else {
		pressure := false
		if host != nil && validMemoryValue(host.available) {
			pressure = host.level >= 2 || host.available <= g.reserve+margin
			if before := g.previous; before != nil {
				pressure = pressure || host.oom > before.oom || host.swapOut > before.swapOut || host.swap > before.swap || host.psiTotalUS > before.psiTotalUS
			}
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
