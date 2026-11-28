package myce

import (
	"cmp"
	"maps"
	"runtime"
	"slices"
	"sync/atomic"
	"time"
)

type shadowFootprintSampler struct {
	platform   string
	state      atomic.Pointer[shadowAttemptReading]
	ramMissing bool
	deviceAt   time.Time // owned by the RAM sampler
	pairedRAM  shadowMemoryReading
}

// This sampler borrows raw readings from the existing attempt observers and
// publishes immutable snapshots. Neither the admission ledger nor RSS guard
// consumes its output.
func (s *shadowFootprintSampler) observe(at time.Time, host *float64, device *hostMemorySample, hostPeriod time.Duration) {
	current := shadowAttemptReading{}
	if previous := s.state.Load(); previous != nil {
		current = *previous
	}
	if host == nil || !validMemoryValue(*host) || !current.RAM.At.IsZero() && at.Sub(current.RAM.At) >= 2*hostPeriod {
		s.ramMissing = true
	}
	counters := shadowMemoryCounters{host: host}
	period := hostPeriod
	if s.platform == shadowLinuxUnified || s.platform == shadowLinuxDiscrete {
		period = deviceMemorySamplePeriod
		if device != nil && !at.Before(device.at) && at.Sub(device.at) < deviceMemorySampleFreshFor {
			counters.device = &device.gb
			if device.pinnedKnown {
				counters.driverSystem = &device.pinnedGB
			}
			ram, card := shadowFootprint(s.platform, counters)
			if device.at.After(s.deviceAt) {
				s.ramMissing = s.ramMissing || ram == nil
				current.Card = current.Card.observe(device.at, card, period)
				s.pairedRAM = s.pairedRAM.observe(at, ram, period)
				s.deviceAt = device.at
			}
			// Use the fast RAM reading with the last valid device counter for
			// footprint. Only distinct device reads establish a combined rate.
			current.RAM = current.RAM.observe(at, ram, hostPeriod)
			current.RAM.Rate, current.RAM.Fastest = s.pairedRAM.Rate, s.pairedRAM.Fastest
			if ram == nil {
				s.pairedRAM = s.pairedRAM.observe(at, nil, period)
			}
			s.state.Store(&current)
			return
		}
		s.pairedRAM = s.pairedRAM.observe(at, nil, period)
	}
	ram, card := shadowFootprint(s.platform, counters)
	current.RAM = current.RAM.observe(at, ram, period)
	current.Card = current.Card.observe(at, card, period)
	s.state.Store(&current)
}

func shadowPlatform(w Worker, j Job, cgroup bool) string {
	if w.devEnvironmentSizing && !cgroup && runtime.GOOS != "darwin" {
		return shadowSynthetic
	}
	if !cgroup {
		if runtime.GOOS == "darwin" {
			return shadowDarwin
		}
		return ""
	}
	if j.Spec.Needs.cpuOnly() {
		return shadowLinuxCPU
	}
	if unifiedCUDA(&w.Machine) {
		return shadowLinuxUnified
	}
	if !w.Machine.UnifiedMemory {
		return shadowLinuxDiscrete
	}
	return ""
}

func (p *process) shadowMemoryReading() *shadowAttemptReading {
	if p.custodian != nil {
		return p.custodian.snapshot().MemoryShadow
	}
	return p.shadowMemory.state.Load()
}

type workerMemoryShadow struct {
	at            time.Time
	previous      *MemoryShadowReport
	fastest       map[string]float64
	keepGB        float64 // Most recent effective keep delivered by the queue.
	reactionGrace time.Duration
}

func (s *workerMemoryShadow) sample(w Worker, m *Machine, raw float64, jobs []Job, reports []AttemptReport, at time.Time) *MemoryShadowReport {
	reaction := (checkinPeriod(w.Interval) + s.reactionGrace).Seconds()
	report := &MemoryShadowReport{ReactionSeconds: reaction, sampledAt: at}
	ram := MemoryShadowPool{Name: "ram", KeepGB: max(w.HostMemoryReserveGB, s.keepGB), CapacityGB: m.HostMemoryBudgetGB}
	if m.SharedHost != nil {
		ram.KeepGB = max(ram.KeepGB, m.SharedHost.InteractiveReserveGB)
	}
	if w.supportsMemoryStops() {
		ram.KeepGB += ramStopKeep(m, runtime.GOOS)
	}
	if ram.CapacityGB == 0 && !m.HostMemoryBudgetMeasured {
		total := 0.0
		if m.MemoryObservation != nil {
			total = m.MemoryObservation.TotalGB
		} else if m.CapabilityProfile != nil {
			total = float64(m.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
		}
		ram.CapacityGB = max(0, total-ram.KeepGB)
	}
	if m.HostMemoryMeasured && m.HostMemoryError == "" && validMemoryValue(raw) {
		free := raw - ram.KeepGB
		ram.FreeGB = &free
	}
	if runtime.GOOS == "darwin" && m.HostHealth != nil && len(m.HostHealth.History) > 0 {
		last := m.HostHealth.History[len(m.HostHealth.History)-1]
		ram.PressureWarning = last.Error == "" && last.Pressure > 1
	}
	report.Pools = append(report.Pools, ram)
	cards := map[string]int{}
	for _, card := range m.Devices {
		if card.UnifiedMemory {
			continue
		}
		pool := MemoryShadowPool{Name: "card/" + card.ID, CapacityGB: card.TotalMemoryGB}
		if card.Reason == "" && card.TotalMemoryGB > 0 {
			pool.FreeGB = card.rawFreeGB
			if deviceTelemetryFresh(card.Telemetry, at) && card.Telemetry.rawFreeGB != nil {
				pool.FreeGB = card.Telemetry.rawFreeGB
			}
		}
		if card.Platform == "fake" && card.Reason == "" {
			free := card.FreeMemoryGB
			pool.FreeGB = &free
		}
		pool.KeepGB = card.ReserveMemoryGB
		if w.supportsMemoryStops() {
			pool.KeepGB += cardStopKeep(card.Platform)
		}
		if pool.FreeGB != nil {
			free := *pool.FreeGB - pool.KeepGB
			pool.FreeGB = &free
		}
		cards[card.ID] = len(report.Pools)
		report.Pools = append(report.Pools, pool)
	}
	byAttempt := make(map[attemptKey]AttemptReport, len(reports))
	for _, r := range reports {
		byAttempt[attemptKey{r.ID, r.Attempt, r.Session}] = r
	}
	for _, j := range jobs {
		r := byAttempt[jobKey(j)]
		var readings shadowAttemptReading
		if r.memoryShadow != nil {
			readings = *r.memoryShadow
		}
		local := localJobMachine(w, j)
		platform := shadowLinuxCPU
		switch {
		case w.devEnvironmentSizing && runtime.GOOS != "darwin":
			platform = shadowSynthetic
		case runtime.GOOS == "darwin":
			platform = shadowDarwin
		case !j.Spec.Needs.cpuOnly() && unifiedCUDA(&local):
			platform = shadowLinuxUnified
		case !j.Spec.Needs.cpuOnly():
			platform = shadowLinuxDiscrete
		}
		period := attemptMemorySamplePeriod
		switch platform {
		case shadowDarwin, shadowSynthetic:
			period = processMemorySamplePeriod
		case shadowLinuxDiscrete, shadowLinuxUnified:
			period = deviceMemorySamplePeriod
		}
		if s.fastest == nil {
			s.fastest = map[string]float64{}
		}
		label := platform
		if platform == shadowLinuxDiscrete {
			label += "/" + local.GPU
		}
		if !readings.RAM.At.IsZero() || readings.RAM.Fastest > 0 {
			s.fastest[label+"/ram"] = max(s.fastest[label+"/ram"], readings.RAM.Fastest)
		}
		expected := stopExpectedRAM(j, &local)
		a := shadowAttempt(j, expected, readings.RAM, at, period)
		// Only CPU work on a kernel-contained Linux slice qualifies until
		// the discrete driver probe. During shadow this changes only a number.
		a.Lent = platform == shadowLinuxCPU && m.MemoryContainment != nil && m.MemoryContainment.State == "available" &&
			m.HostMemoryBudgetGB > 0 && a.FootprintGB != nil && at.Sub(readings.RAM.LastGrowth).Seconds() >= reaction
		report.Pools[0].Attempts = append(report.Pools[0].Attempts, a)
		if j.Spec.Needs.cpuOnly() {
			continue
		}
		for _, id := range attemptCards(j, local) {
			if index, ok := cards[id]; ok {
				// A per-attempt total is not evidence for any one of several cards.
				card := readings.Card
				if len(j.AllocatedDevices) > 1 {
					card = shadowMemoryReading{}
				}
				p := &report.Pools[index]
				p.Attempts = append(p.Attempts, shadowAttempt(j, stopExpectedCard(j, p.CapacityGB), card, at, deviceMemorySamplePeriod))
				if !card.At.IsZero() || card.Fastest > 0 {
					s.fastest[label+"/card"] = max(s.fastest[label+"/card"], card.Fastest)
				}
			}
		}
	}
	report.FastestGrowth = maps.Clone(s.fastest)
	for i := range report.Pools {
		p := &report.Pools[i]
		slices.SortFunc(p.Attempts, func(a, b MemoryShadowAttempt) int {
			if n := cmp.Compare(a.ID, b.ID); n != 0 {
				return n
			}
			return cmp.Compare(a.Attempt, b.Attempt)
		})
		p.sum()
		if p.SlackGB == nil {
			continue
		}
		// Keep cancels out of the inferred owners' slope. Use raw physical
		// totals where available; none of these readings fill a footprint gap.
		total := p.CapacityGB
		if p.Name == "ram" && m.MemoryObservation != nil {
			total = m.MemoryObservation.TotalGB
		}
		p.OwnersGB = max(0, total-(*p.FreeGB+p.KeepGB)-p.FootprintGB)
		if s.previous != nil && at.Sub(s.at) < 2*checkinPeriod(w.Interval) {
			for _, prior := range s.previous.Pools {
				if p.Name == prior.Name && p.KeepGB == prior.KeepGB {
					p.forecast(&prior, at.Sub(s.at).Seconds(), reaction)
					break
				}
			}
		}
	}
	s.previous, s.at = report, at
	return report
}

func stopExpectedRAM(j Job, local *Machine) float64 {
	if pooledMemory(&j) && memoryPools(&j).RAM.TypicalGB > 0 {
		return memoryPools(&j).RAM.TypicalGB
	}
	return hostNeed(local, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
}

func shadowAttempt(j Job, expected float64, reading shadowMemoryReading, at time.Time, period time.Duration) MemoryShadowAttempt {
	a := MemoryShadowAttempt{ID: j.ID, Attempt: j.Attempt, ExpectedGB: expected, HighWaterGB: reading.PeakGB}
	if reading.fresh(at, period) {
		a.FootprintGB, a.Growth = &reading.GB, reading.Rate
	}
	return a
}

func stopExpectedCard(j Job, capacity float64) float64 {
	if pooledMemory(&j) {
		if card := memoryPools(&j).Card; card != nil && card.Learned {
			return card.ChargeGB
		}
	}
	return capacity
}

func (s *shadowFootprintSampler) finishPeak(peak *AttemptMemoryPeak, end time.Time, period time.Duration) *AttemptMemoryPeak {
	r := s.state.Load()
	if peak == nil || r == nil {
		return peak
	}
	out := *peak
	device := peak.DeviceSampling
	completeDevice := device != nil && device.Complete
	ram := memoryRunPool{PeakGB: r.RAM.PeakGB, Complete: r.RAM.fresh(end, period) && !s.ramMissing}
	card := memoryRunPool{PeakGB: r.Card.PeakGB}
	if s.platform == shadowLinuxUnified || s.platform == shadowLinuxDiscrete {
		ram.Complete = ram.Complete && completeDevice && !s.pairedRAM.At.IsZero()
	}
	if s.platform == shadowLinuxDiscrete && completeDevice {
		// The card footprint is the device counter. Its terminal coverage
		// already checks the end gap using the original sampler timestamps.
		card = memoryRunPool{PeakGB: device.MaxGB, Complete: true}
	}
	out.Learned = &memoryRunPeaks{RAM: ram, Card: card}
	return &out
}
