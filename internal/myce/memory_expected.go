package myce

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

const memoryV2Prefix = "v2/"
const ownersRowPrefix = "owners-v2/"
const firstOwnersWindow = 24 * time.Hour

// A raise retains both stopping thresholds so a pool that cannot improve on
// the old attempt can explain the wait and recover if its reach grows.
type memoryRaise struct {
	PeakGB     float64
	ExpectedGB float64
}

type memoryOwnersHold struct {
	PeakGB     float64
	Window     time.Duration
	BelowSince time.Time
	LapsedAt   time.Time
}

type memoryStopSample struct {
	ExpectedGB   float64 `json:"expected_gb"`
	PeakGB       float64 `json:"peak_gb"`
	OwnersGB     float64 `json:"owners_gb"`
	OwnersPeakGB float64 `json:"owners_peak_gb"`
}

func (k memoryEvidenceKey) v2() memoryEvidenceKey {
	k.Platform = memoryV2Prefix + k.Platform
	return k
}

const xpuCardAttributionQualified = false // Probe 0b-4.

func learnsExpectedRAM(k memoryEvidenceKey) bool {
	return !strings.HasSuffix(k.Platform, "linux/cuda/unified") || unifiedStopAttributionQualified
}

func learnsExpectedCard(m *Machine) bool {
	return m.GPU != "xpu" || xpuCardAttributionQualified
}

func (q *Queue) currentMemoryEvidence() *memoryEvidence {
	if q.memoryEvidencePending != nil {
		return q.memoryEvidencePending
	}
	if q.memoryEvidence != nil {
		return q.memoryEvidence
	}
	return &memoryEvidence{}
}

func (e *memoryEvidence) observeExpected(key memoryEvidenceKey, peak float64) {
	key = key.v2()
	exact := e.exact[key]
	familyKey := key.familyKey()
	family := e.family[familyKey]
	if len(exact.Peaks) == 0 {
		platform := key.platformKey()
		e.first[platform] = appendMemoryValue(e.first[platform], peak, memoryRatioWindow)
		e.write(memoryEvidenceRowKey{kind: "first", key: platform}, e.first[platform])
	}
	exact.Peaks = appendMemoryValue(exact.Peaks, peak, memoryPeakWindow)
	exact.Raise = nil
	family.Peaks = appendMemoryValue(family.Peaks, peak, memoryPeakWindow)
	e.exact[key], e.family[familyKey] = exact, family
	e.write(memoryEvidenceRowKey{kind: "exact", key: key}, exact)
	e.write(memoryEvidenceRowKey{kind: "family", key: familyKey}, family)
}

func (e *memoryEvidence) raiseExpected(key memoryEvidenceKey, peak, expected float64) {
	key = key.v2()
	exact := e.exact[key]
	raise := memoryRaise{PeakGB: peak, ExpectedGB: expected}
	if exact.Raise != nil {
		raise.PeakGB = max(raise.PeakGB, exact.Raise.PeakGB)
		raise.ExpectedGB = max(raise.ExpectedGB, exact.Raise.ExpectedGB)
	}
	exact.Raise = &raise
	e.exact[key] = exact
	e.write(memoryEvidenceRowKey{kind: "exact", key: key}, exact)
}

func (e *memoryEvidence) expected(key memoryEvidenceKey, hint, reach float64, known bool) memoryPoolEstimate {
	key = key.v2()
	exact, family, first := e.exact[key], e.family[key.familyKey()], e.first[key.platformKey()]
	p := memoryPoolEstimate{Learned: true}
	switch {
	case len(exact.Peaks) > 0:
		p.ChargeGB, p.Evidence = slices.Max(exact.Peaks), memoryEstimateEvidence{Level: "exact", Runs: len(exact.Peaks)}
	case len(family.Peaks) > 0:
		p.ChargeGB, p.Evidence = slices.Max(family.Peaks), memoryEstimateEvidence{Level: "family", Runs: len(family.Peaks)}
	case key.Pool == "card":
		p.ChargeGB, p.Evidence.Level = reach, "unknown"
		p.Alone = true // Preserve the existing missing-device-evidence custody.
	case validRate(hint):
		p.ChargeGB, p.Evidence.Level = hint, "hint"
	case len(first) > 0:
		p.ChargeGB, p.Evidence = memoryMedian(first), memoryEstimateEvidence{Level: "platform", Runs: len(first)}
	default:
		p.ChargeGB, p.Evidence.Level = reach, "unknown"
	}
	if exact.Raise != nil {
		prior := max(exact.Raise.PeakGB, exact.Raise.ExpectedGB)
		raised := 2 * prior
		if known {
			raised = min(raised, reach)
		}
		p.ChargeGB = max(p.ChargeGB, raised)
		p.RetryFloorGB = prior
		p.Evidence.Level = "raised"
	}
	p.TypicalGB = p.ChargeGB
	return p
}

func ownersKey(m *Machine, pool string) memoryEvidenceKey {
	return memoryEvidenceKey{Family: identityHash(hostID(m)), Pool: pool}
}

func (e *memoryEvidence) writeOwners(key memoryEvidenceKey, hold memoryOwnersHold) {
	e.owners[key] = hold
	e.write(memoryEvidenceRowKey{kind: "projection", key: key}, hold)
}

func (h memoryOwnersHold) stopped(now time.Time, s *memoryStopSample) memoryOwnersHold {
	if h.PeakGB == 0 {
		if h.Window > 0 && !h.LapsedAt.IsZero() && now.Sub(h.LapsedAt) < h.Window {
			h.Window *= 2
		} else {
			h.Window = firstOwnersWindow
		}
	}
	if s.OwnersPeakGB > h.PeakGB || s.OwnersGB >= h.PeakGB {
		h.BelowSince = time.Time{}
	}
	h.PeakGB = max(h.PeakGB, s.OwnersPeakGB)
	if s.OwnersGB < h.PeakGB && h.BelowSince.IsZero() {
		h.BelowSince = now
	}
	return h
}

func (h memoryOwnersHold) observe(now time.Time, owners float64, continuous bool) memoryOwnersHold {
	if h.PeakGB == 0 {
		return h
	}
	switch {
	case owners >= h.PeakGB:
		h.BelowSince = time.Time{}
	case !continuous || h.BelowSince.IsZero():
		h.BelowSince = now
	case now.Sub(h.BelowSince) >= h.Window:
		h.PeakGB, h.BelowSince, h.LapsedAt = 0, time.Time{}, now
	}
	return h
}

// Hold expiry is driven by complete, ordered worker samples. No gap in
// observations is counted as time spent below the owners' peak.
func (q *Queue) observeMemoryOwners(m, old *Machine, received time.Time, period time.Duration) {
	report := m.MemoryShadow
	if report == nil || !report.stopForecast || report.sampleAge < 0 || report.sampleAge >= 2*checkinPeriod(period) {
		return
	}
	now := received.Add(-report.sampleAge)
	m.memoryShadowAt = now
	for _, p := range report.Pools {
		key := ownersKey(m, p.Name)
		h := q.currentMemoryEvidence().owners[key]
		if h.PeakGB == 0 || p.ReachGB == nil || !validMemoryValue(p.OwnersGB) {
			continue
		}
		continuous := false
		if old != nil && old.Session == m.Session && old.MemoryShadow != nil {
			priorAt := old.memoryShadowAt
			previous := memoryPoolReading(old, p.Name)
			continuous = now.After(priorAt) && now.Sub(priorAt) < 2*checkinPeriod(period) &&
				previous != nil && previous.ReachGB != nil && previous.OwnersGB < h.PeakGB
		}
		if next := h.observe(now, p.OwnersGB, continuous); next != h {
			q.stagedMemoryEvidence().writeOwners(key, next)
		}
	}
}

func memoryPoolReading(m *Machine, pool string) *MemoryShadowPool {
	if m.MemoryShadow != nil {
		for i := range m.MemoryShadow.Pools {
			p := &m.MemoryShadow.Pools[i]
			if p.Name == pool {
				return p
			}
		}
	}
	return nil
}

func (q *Queue) ownersMemory(m *Machine, pool string) (hold, peak float64) {
	holds := q.currentMemoryEvidence().owners
	if len(holds) == 0 {
		return 0, 0
	}
	h := holds[ownersKey(m, pool)]
	owners := 0.0
	if p := memoryPoolReading(m, pool); p != nil && p.ReachGB != nil && validMemoryValue(p.OwnersGB) {
		owners = p.OwnersGB
	}
	return max(0, h.PeakGB-owners), h.PeakGB
}

func (q *Queue) memoryReach(m *Machine, pool string) (reach, ownersPeak float64, known bool) {
	reach, known = q.poolAllowance(m), q.poolAllowance(m) > 0 || m.HostMemoryBudgetMeasured
	if pool != "ram" {
		reach = m.TotalMemoryGB
		known = reach > 0
	}
	if p := memoryPoolReading(m, pool); p != nil && p.ReachGB != nil {
		if !known {
			reach = *p.ReachGB
		} else {
			reach = min(reach, *p.ReachGB)
		}
		known = true
	}
	hold, ownersPeak := q.ownersMemory(m, pool)
	return max(0, reach-hold), ownersPeak, known
}

func (p *memoryPoolEstimate) capacityAt(reach float64, known bool) {
	p.CapacityWait = p.Learned && known && (p.ChargeGB > reach || p.RetryFloorGB > 0 && p.RetryFloorGB >= reach)
}

func memoryCapacityReason(name string, estimate memoryPoolEstimate, reach, owners float64) string {
	return fmt.Sprintf("%s needs %.3f GiB; measured peak or previous E %.3f GiB, owners' peak %.3f GiB, reach %.3f GiB",
		name, estimate.ChargeGB, max(estimate.PeakGB, estimate.RetryFloorGB), owners, reach)
}

func (q *Queue) cardMemoryReach(m *Machine) (float64, bool) {
	if len(m.groupMembers) == 0 {
		reach, _, known := q.memoryReach(m, "card/"+m.Device)
		return reach, known
	}
	reach, known := m.TotalMemoryGB/float64(len(m.groupMembers)), true
	for _, card := range m.groupMembers {
		r, _, ok := q.memoryReach(card, "card/"+card.Device)
		reach, known = min(reach, r), known && ok
	}
	return reach, known
}

// A raise uses the largest reachable pool of the same platform family. Check
// only raised candidates, on placement, so finishing an attempt cannot scan
// the waiting routes or fail a job on a temporary capacity comparison.
func (q *Queue) raiseReach(j *Job, current *Machine, key memoryEvidenceKey, reach float64, known bool) (float64, bool) {
	if q.currentMemoryEvidence().exact[key.v2()].Raise == nil {
		return reach, known
	}
	for _, m := range q.s.Machines {
		if key.Pool == "ram" && m.ID == current.ID || m.Gone || m.Leaving || m.Replacement != nil || !q.jobAllowsMachine(j, m.ID) ||
			workerMemoryHold(m) != nil || m.MemoryShadow == nil || !m.MemoryShadow.stopForecast {
			continue
		}
		consider := func(target *Machine) {
			if !memorySizingTargetFits(j, target) || !toolsFit(j, target) || !nativeNetworkFits(j, target) ||
				!environmentCanRun(j, target) || !extensionCanRun(j, target) {
				return
			}
			profile := machineTarget(target)
			if j.Spec.Needs.cpuOnly() {
				profile.Platform = "cpu"
			}
			if memoryPlatform(profile, target.UnifiedMemory) != key.Platform {
				return
			}
			name := "ram"
			if key.Pool == "card" {
				name = "card/" + target.Device
			}
			if candidate, _, ok := q.memoryReach(target, name); ok {
				reach, known = max(reach, candidate), true
			}
		}
		switch {
		case m.Devices == nil:
			consider(m)
		case j.Spec.Needs.cpuOnly():
			consider(cpuMachineView(m))
		default:
			for _, device := range m.Devices {
				target := device.machine(*m)
				consider(&target)
			}
		}
	}
	return reach, known
}

// Use a terminal pair of the same footprint sampler R4 reads. Peak remains
// available for a stop even when a gap prevents it from training a clean run.
type memoryRunPool struct {
	PeakGB   float64 `json:"peak_gb"`
	Complete bool    `json:"complete"`
}

func (p *memoryRunPool) acceptedGB() *float64 {
	if p.Complete && validMemoryValue(p.PeakGB) {
		return &p.PeakGB
	}
	return nil
}

type memoryRunPeaks struct {
	RAM  memoryRunPool `json:"ram"`
	Card memoryRunPool `json:"card"`
}

func (q *Queue) recordPoolEvidence(j *Job, r Result) bool {
	if q.memoryEvidence == nil || j.StartedAt == nil || cancelled(j) || wallTimeStopped(j) ||
		attemptDiskLimitFailure(r) || r.ExecutionCause != nil && r.ExecutionCause.PrelaunchRefused {
		return false
	}
	if !pooledMemory(j) {
		q.recordLegacyPoolEvidence(j, r)
		return false
	}
	m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if m == nil {
		return false
	}
	pools := memoryPools(j)
	key := pooledEvidenceKey(j, m)
	stop := priorityStop(j)
	pressure := j.Reason == memoryPressureCause || r.ExecutionCause != nil && r.ExecutionCause.Class == memoryPressureCause
	if !r.Success || j.Spec.warmPieces() && j.FanOut.Next < len(j.Spec.FanOut.Pieces) {
		if pressure || r.OutOfMemory {
			return q.learnMemoryStop(j, r, m, key, pools, stop)
		}
	}
	p := r.MemoryPeak
	if !r.ReleaseOnly && r.Success && !r.Interrupted && !r.Forced && p != nil && !p.constrained() &&
		!p.PressureUnavailable && p.Learned != nil {
		if peak := p.Learned.RAM.acceptedGB(); pools.RAM.Learned && peak != nil {
			q.stagedMemoryEvidence().observeExpected(key, *peak)
			retainPoolPeaks(j, *peak, 0)
		}
		if peak := p.Learned.Card.acceptedGB(); pools.Card != nil && pools.Card.Learned && peak != nil {
			key.Pool = "card"
			q.stagedMemoryEvidence().observeExpected(key, *peak)
			retainPoolPeaks(j, 0, *peak)
		}
	}
	if !pools.RAM.Learned {
		q.recordLegacyPoolEvidence(j, r)
	}
	return false
}

func (q *Queue) learnMemoryStop(j *Job, r Result, m *Machine, key memoryEvidenceKey, pools MemoryPools, stop *PriorityPreemption) bool {
	pool, estimate := "ram", &pools.RAM
	p := r.MemoryPeak
	kernelKill := r.OutOfMemory || p != nil && p.Events != nil && (p.Events.OOMKill > 0 || p.Events.OOMGroupKill > 0)
	var sample *memoryStopSample
	if stop != nil && stop.Cause == memoryPressureCause {
		sample = stop.MemorySample
		if !kernelKill && stop.DevicePool != nil {
			pool = "card"
		}
	}
	if !kernelKill && r.ExecutionCause != nil && r.ExecutionCause.RSSLimit.pool() != "" {
		limit := r.ExecutionCause.RSSLimit
		pool = limit.pool()
		sample = &memoryStopSample{ExpectedGB: float64(limit.LimitBytes) / (1 << 30), PeakGB: float64(limit.ObservedBytes) / (1 << 30)}
	}
	poolName := pool
	if pool == "card" {
		estimate = pools.Card
		poolName += "/" + m.Device
	}
	if estimate == nil || !estimate.Learned || sample == nil && !kernelKill {
		q.recordLegacyPoolEvidence(j, r)
		return false
	}
	// The caller still fails a kill proven to be at the job's own explicit limit.
	if kernelKill && r.OutOfMemory && pools.LimitGB > 0 {
		return true
	}
	if sample == nil || kernelKill {
		sample = &memoryStopSample{ExpectedGB: estimate.ChargeGB}
	}
	peak := sample.PeakGB
	if p != nil && p.Learned != nil {
		reading := p.Learned.RAM
		if pool == "card" {
			reading = p.Learned.Card
		}
		if validMemoryValue(reading.PeakGB) {
			peak = max(peak, reading.PeakGB)
		}
	}
	if !validMemoryValue(peak) || !validMemoryValue(sample.ExpectedGB) {
		return false
	}
	key.Pool = pool
	if kernelKill || peak > sample.ExpectedGB {
		q.stagedMemoryEvidence().raiseExpected(key, peak, sample.ExpectedGB)
	} else if validRate(sample.OwnersPeakGB) && validMemoryValue(sample.OwnersGB) {
		e := q.stagedMemoryEvidence()
		key := ownersKey(m, poolName)
		e.writeOwners(key, e.owners[key].stopped(q.clock.Now(), sample))
	}
	if pool == "card" {
		retainPoolPeaks(j, 0, peak)
	} else {
		retainPoolPeaks(j, peak, 0)
	}
	return true
}

func (q *Queue) retryMemoryStop(j *Job, learned bool) {
	if learned {
		if wallTimeStopped(j) || yieldMatches(j) {
			q.retry(j, j.Reason)
		} else {
			q.requeue(j, memoryPressureCause, false)
		}
	} else {
		q.requeueLegacyMemoryPressure(j)
	}
}
