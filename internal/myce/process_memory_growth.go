package myce

import "time"

// RSS has no kernel containment boundary. Pair each positive increment with
// its own elapsed time: a past burst's rate cannot be multiplied by a later
// flat observation's delay. Live flat samples report zero growth/rate; the
// independent AttemptMemoryPeak merge retains the largest observed increment
// for learning. No increment can exceed the RSS actually observed.
//
// The latest slope still extends linearly through sample age at admission.
// This cannot predict an unseen burst; missing/stale samples keep the existing
// conservative charge and the sampled RSS guard remains necessary.
type processMemoryGrowth struct {
	last         time.Time
	lastGB, rate float64
	horizon      time.Duration
}

func (g *processMemoryGrowth) observe(now time.Time, gb float64) float64 {
	if !validMemoryValue(gb) {
		*g = processMemoryGrowth{}
		return 0
	}
	g.rate, g.horizon = 0, 0
	margin := 0.0
	if !g.last.IsZero() && now.After(g.last) {
		g.horizon = now.Sub(g.last)
		margin = max(0, gb-g.lastGB)
		g.rate = margin / g.horizon.Seconds()
	}
	g.last, g.lastGB = now, gb
	return margin
}

// A forecast cannot allocate more physical RAM than the measured host has.
// Hitting this bound still reserves the whole host (RSS receives no resident
// credit), preventing neighbors from spending a high-slope job's headroom.
// Keep actual RSS even when shared-page double counting exceeds physical RAM,
// and keep every known floor. Estimates are deliberately not hard ceilings.
// Missing inventory leaves the existing conservative forecast unchanged;
// Linux's hard-cgroup accounting and estimator are independent.
func boundedProcessMemoryCharge(m *Machine, floor, footprint, forecastCharge float64) float64 {
	if m == nil || m.MemoryEnforcement == "memory ceilings enforced (cgroup v2)" || m.CapabilityProfile == nil || m.CapabilityProfile.OS != "darwin" || m.CapabilityProfile.SystemMemoryBytes == 0 {
		return forecastCharge
	}
	physical := float64(m.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
	return max(floor, footprint, min(forecastCharge, physical))
}
