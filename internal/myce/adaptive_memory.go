package myce

import (
	"math"
	"time"
)

// Growth is a rate measured over elapsed observation time, projected over the
// longest observed sample interval. A delayed sampler therefore buys more
// headroom. No assumption about fleet size, host RAM or payload type is used.
// This is an empirical error allowance, not a promise about unseen bursts;
// kernel ceilings and the existing OOM floors remain the containment boundary.
type memoryGrowth struct {
	last         time.Time
	lastGB, rate float64
	horizon      time.Duration
	// The largest growth measured within one interval and the latest
	// interval's rate. Unlike the fastest rate times the longest interval,
	// they never project an ended burst across an unrelated long gap.
	increment, current float64
}

func (g *memoryGrowth) observe(now time.Time, gb float64) float64 {
	if !validMemoryValue(gb) {
		return g.rate * g.horizon.Seconds()
	}
	if !g.last.IsZero() && now.After(g.last) {
		elapsed := now.Sub(g.last)
		g.horizon = max(g.horizon, elapsed)
		g.rate = max(g.rate, (gb-g.lastGB)/elapsed.Seconds())
		g.increment = max(g.increment, gb-g.lastGB)
		g.current = max(0, (gb-g.lastGB)/elapsed.Seconds())
	}
	g.last, g.lastGB = now, gb
	return g.rate * g.horizon.Seconds()
}

func memoryGrowthGB(p *AttemptMemoryPeak) float64 {
	if p != nil && p.GrowthGB != nil && validMemoryValue(*p.GrowthGB) {
		return *p.GrowthGB
	}
	return 0
}

func (q *Queue) acceptedMemoryPoint(p *WorkObservation) bool {
	age := q.clock.Now().Sub(p.MemoryRecordedAt)
	peak := p.MemoryPeak
	return memoryIdentityUsable(*p) && !p.MemoryRecordedAt.IsZero() && age >= 0 && peak != nil && !peak.constrained() && !peak.PressureUnavailable && peak.HostGB != nil && validRate(*peak.HostGB) && validRate(hostMemoryPeakGB(peak))
}

// Complete local host ranges bound non-reclaimable demand; incomplete rows
// retain total peaks. Variation and sampling growth share the forecast margin.
func observedMemoryBound(points []*WorkObservation, device bool) (float64, int) {
	peak, margin, samples := observedMemoryPeakAndMargin(points, device)
	return peak + margin, samples
}

func observedMemoryPeakAndMargin(points []*WorkObservation, device bool) (float64, float64, int) {
	return observedMemoryPeakAndMarginWithDrift(points, device, minimumHostMemoryGB)
}

// A range belongs to the observations that measured it. Its upper forecast is
// range max + max(range spread, sampled growth, launch drift). Older cumulative
// peaks remain independent floors: adding the new range's spread to an older
// maximum counts already-covered variation again. Likewise, differences between
// allowance-key maxima are not a measured within-key range.
//
// Each incomplete row independently keeps its total-cgroup bound. A complete
// neighbour cannot discount it, nor must its cache force every complete row back
// to total sizing. Device growth and paired transfer error remain separate.
func observedMemoryPeakAndMarginWithDrift(points []*WorkObservation, device bool, drift float64) (float64, float64, int) {
	peak, bound, samples := 0.0, 0.0, 0
	deviceLow, deviceGrowth := math.Inf(1), 0.0
	legacyLow, unmeasured := math.Inf(1), true
	for _, point := range points {
		p := point.MemoryPeak
		if p == nil {
			continue
		}
		value := hostMemoryPeakGB(p)
		rangePeak := point.MemoryRange
		if point.Key.Component != "cpu" {
			rangePeak = point.GPUHostRange
		}
		rangeComplete := !device && rangePeak.valid() && rangePeak.Samples >= 2 && p.NonreclaimableGB != nil && validMemoryValue(*p.NonreclaimableGB)
		if point.Key.Component != "cpu" {
			_, complete := p.gpuHostPeakGB()
			rangeComplete = rangeComplete && complete
		}
		if rangeComplete {
			value = max(rangePeak.MaxGB, *p.HostGB, *p.NonreclaimableGB)
			if combined, ok := p.gpuHostPeakGB(); ok {
				value = max(value, combined)
			}
		}
		if device {
			if p.DeviceGB == nil || !validRate(*p.DeviceGB) {
				continue
			}
			value = *p.DeviceGB
			deviceLow = min(deviceLow, value)
			deviceGrowth = max(deviceGrowth, deviceMemoryGrowthGB(p))
		}
		peak = max(peak, value)
		legacyLow = min(legacyLow, value)
		unmeasured = unmeasured && !rangeComplete && (p.GrowthGB == nil || !validMemoryValue(*p.GrowthGB))
		if rangeComplete {
			// The cumulative resident/driver floor may predate this range.
			// Never discard it, including when coverage restarted after a gap.
			bound = max(bound, value+drift, rangePeak.MaxGB+max(rangePeak.MaxGB-rangePeak.MinGB, memoryGrowthGB(p), drift))
		} else if !device {
			// Preserve the legacy fallback margin. Configured ceiling slack is
			// added separately by hostNeed, not again to this total-cgroup bound.
			bound = max(bound, value+max(memoryGrowthGB(p), minimumHostMemoryGB))
		}
		samples += max(1, point.MemorySamples)
	}
	if samples == 0 {
		return 0, 0, 0
	}
	if device {
		return peak, max(peak-deviceLow, deviceGrowth, minimumHostMemoryGB), samples
	}
	if unmeasured {
		// No measured range or sampling growth yet: preserve legacy uncertainty
		// until new accepted sensor evidence can calibrate it.
		bound = max(bound, peak+(peak-legacyLow))
	}
	return peak, max(0, bound-peak), samples
}

func pointsForExact(points []*WorkObservation, key ObservationKey) []*WorkObservation {
	var exact []*WorkObservation
	for _, p := range points {
		if p.Key.Host == key.Host && p.Key.Component == key.Component && p.Key.Hardware == key.Hardware && p.Key.Fingerprint == key.Fingerprint && p.Key.Environment == key.Environment && p.Key.Target == key.Target {
			exact = append(exact, p)
		}
	}
	return exact
}
func observedMemoryBoundForHost(points []*WorkObservation, key ObservationKey, m *Machine) (float64, int) {
	var local []*WorkObservation
	for _, p := range points {
		if sameMemoryHost(p.Key, key, m) && p.Key.Target == key.Target {
			local = append(local, p)
		}
	}
	peak, margin, samples := observedMemoryPeakAndMarginWithDrift(local, false, exploratoryDriftMarginGB(m))
	return peak + margin, samples
}

type memoryTransferTarget struct {
	host, component, hardware, fingerprint, environment, layout string
	target                                                      ExecutionTarget
}

func transferTarget(k ObservationKey, layout string) memoryTransferTarget {
	return memoryTransferTarget{k.Host, k.Component, k.Hardware, k.Fingerprint, k.Environment, layout, k.Target}
}

// Ratios are calibrated only by paired accepted executions of identical work
// on these exact target identities. Two independent pairs are the minimum
// that can expose transfer error. Their maximum ratio plus observed ratio
// spread covers this installation's errors without a universal multiplier.
type memoryTransferKey struct {
	from, to memoryTransferTarget
	device   bool
}
type memoryTransferResult struct {
	factor float64
	known  bool
}

func (q *Queue) transferMemoryFactor(from, to memoryTransferTarget, device bool) (float64, bool) {
	key := memoryTransferKey{from, to, device}
	catalog := q.memoryObservationCatalog()
	catalog.prepareDerivations(q.clock.Now())
	cache := catalog.transfers
	if result, ok := cache[key]; ok {
		return result.factor, result.known
	}
	factor, known := q.transferMemoryFactorUncached(from, to, device)
	cache[key] = memoryTransferResult{factor, known}
	return factor, known
}
func (q *Queue) transferMemoryFactorUncached(from, to memoryTransferTarget, device bool) (float64, bool) {
	low, high, pairs := math.Inf(1), 0.0, 0
	catalog := q.memoryObservationCatalog()
	for work, sourcePoints := range catalog.targets[from] {
		sources := acceptedMemoryPoints(q, sourcePoints)
		targets := acceptedMemoryPoints(q, catalog.targets[to][work])
		// Only measured demand belongs in the denominator. Dividing by a
		// source uncertainty allowance would make greater uncertainty shrink
		// the target reservation. Target uncertainty stays in the numerator;
		// the new source workload carries its own uncertainty when transferred.
		source := memoryTransferMeasurement(sources, device)
		target, _ := observedMemoryBound(targets, device)
		if source <= 0 || target <= 0 {
			continue
		}
		ratio := target / source
		if !validRate(ratio) {
			continue
		}
		low, high, pairs = min(low, ratio), max(high, ratio), pairs+1
	}
	if pairs < 2 {
		return 0, false
	}
	return high + (high - low), true
}

// Calibration denominators use measured resident/device demand, never sizing
// allowances or a total-charge fallback that includes reclaimable cache. Every
// new-work bound is at least these measured components; learning a complete
// range later cannot shrink the source basis underneath its calibration.
func memoryTransferMeasurement(points []*WorkObservation, device bool) float64 {
	peak := 0.0
	for _, point := range points {
		p := point.MemoryPeak
		if device {
			if p.DeviceGB != nil && validRate(*p.DeviceGB) {
				peak = max(peak, *p.DeviceGB)
			}
			continue
		}
		peak = max(peak, *p.HostGB)
		if p.NonreclaimableGB != nil && validMemoryValue(*p.NonreclaimableGB) {
			peak = max(peak, *p.NonreclaimableGB)
		}
		if combined, ok := p.gpuHostPeakGB(); ok {
			peak = max(peak, combined)
		}
	}
	return peak
}

func (q *Queue) adaptMemoryTransfer(e *memoryEstimate, j *Job, m *Machine, points []*WorkObservation) {
	target := transferTarget(*e.key, memoryLayout(j, m))
	bound, deviceBound, evidence := 0.0, 0.0, 0
	for _, point := range points {
		from := transferTarget(point.Key, point.MemoryLayout)
		factor, ok := q.transferMemoryFactor(from, target, false)
		if !ok {
			continue
		}
		peak, n := observedMemoryBound([]*WorkObservation{point}, false)
		bound, evidence = max(bound, peak*factor), evidence+n
		if factor, ok := q.transferMemoryFactor(from, target, true); ok {
			peak, _ := observedMemoryBound([]*WorkObservation{point}, true)
			deviceBound = max(deviceBound, peak*factor)
		}
	}
	if evidence > 0 && validRate(bound) && (j.Spec.Needs.cpuOnly() || deviceBound > 0) {
		e.measuredHostGB, e.samples, e.source = bound, evidence, "accepted transfer + measured error"
		if e.device != nil && e.device.DeviceGB != nil && *e.device.DeviceGB > 0 {
			e.deviceFactor = deviceBound / *e.device.DeviceGB
		}
		return
	}
	// Unknown transfer error is explicitly exploratory. The source peak remains
	// a conservative prior; only the existing durable, bounded exception may
	// authorize a smaller probe when that prior cannot fit the target.
	room, why := q.exploratoryMemory(j, m)
	e.refusal = why
	e.measuredHostGB = max(room, hostMemoryPeakGB(e.host), q.constrainedMemoryLowerFor(j, *e, !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory))
	e.uncalibrated, e.source = true, "exploratory (transfer error unavailable)"
}

func acceptedMemoryPoints(q *Queue, points []*WorkObservation) []*WorkObservation {
	var accepted []*WorkObservation
	correctedXPU := false
	for _, p := range points {
		if q.acceptedMemoryPoint(p) {
			accepted = append(accepted, p)
			correctedXPU = correctedXPU || p.MemoryPeak.GPUHost != nil && p.MemoryPeak.GPUHost.SiblingAdjusted
		}
	}
	if correctedXPU {
		// Preserve corrected per-attempt attribution across sibling/allowance
		// keys. A legacy sample must not restore a known inflated host peak.
		filtered := accepted[:0]
		for _, p := range accepted {
			if backendFor(p.Key.Target.Platform).deviceHostMemory && p.MemoryPeak.GPUHost != nil && !p.MemoryPeak.GPUHost.SiblingAdjusted {
				continue
			}
			filtered = append(filtered, p)
		}
		accepted = filtered
	}
	return accepted
}

func (q *Queue) adaptDeviceTransfer(e *memoryEstimate, m *Machine, points []*WorkObservation) {
	target := transferTarget(*e.key, func() string {
		if m.UnifiedMemory {
			return "unified"
		}
		return "separate"
	}())
	bound := 0.0
	for _, p := range points {
		if factor, ok := q.transferMemoryFactor(transferTarget(p.Key, p.MemoryLayout), target, true); ok {
			value, _ := observedMemoryBound([]*WorkObservation{p}, true)
			bound = max(bound, value*factor)
		}
	}
	if bound > 0 && e.device != nil && e.device.DeviceGB != nil && *e.device.DeviceGB > 0 {
		e.deviceFactor = bound / *e.device.DeviceGB
		return
	}
	e.deviceUncalibrated = true
	e.source += "; exploratory device transfer"
}

func deviceMemoryGrowthGB(p *AttemptMemoryPeak) float64 {
	if p != nil && p.DeviceGrowthGB != nil && validMemoryValue(*p.DeviceGrowthGB) {
		return *p.DeviceGrowthGB
	}
	return 0
}
