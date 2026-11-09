package myce

import (
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

func hostID(m *Machine) string {
	if m.HostID != "" {
		return m.HostID
	}
	return m.ID
}

// Reserve at least 64 KiB for declared host memory: a sub-page cgroup limit
// needs one page. Use the largest supported worker page (4/64 KiB), rather
// than the queue host's page size, so enforced <= reserved across hosts.
const minimumHostMemoryGB = 65536.0 / (1 << 30)

// CPU allocations are additional to GPU allocations on unified machines.
// Unenforced laptop workers support explicit admission-only operation.
func hostNeed(m *Machine, gpu, host float64) float64 {
	need := host
	if m.UnifiedMemory && m.HostMemoryMeasured {
		need += gpu
	}
	if host > 0 {
		return max(need+m.MemoryCeilingSlackGB, minimumHostMemoryGB)
	}
	return need
}
func hostFits(m *Machine, gpu, host float64) bool {
	return hostFitFailure(m, gpu, host) == ""
}

func hostFitFailure(m *Machine, gpu, host float64) string {
	if m.HostMemoryPolicyMismatch {
		return "host memory allowance awaiting worker acknowledgement"
	}
	if host == 0 {
		if strings.HasPrefix(m.MemoryEnforcement, "memory ceilings enforced") || m.HostMemoryBudgetGB != 0 || m.hostBudgetGB != 0 || m.HostMemoryDerivation != nil || m.HostMemoryBudgetMeasured {
			return "host memory declaration is 0 GiB; allowance or enforced ceilings require a positive declaration"
		}
		if !unifiedCUDA(m) || gpu == 0 {
			return ""
		}
	}
	if !m.HostMemoryMeasured || m.HostMemoryError != "" {
		return "host memory capacity unavailable"
	}
	if !hostMemoryAvailableFits(m, gpu, host, m.FreeHostMemoryGB) {
		return "host memory"
	}
	return ""
}
func (q *Queue) refreshHostMemory() {
	// Only committed scheduling passes grow durable reservations; detached
	// observation views compute the same charge without mutating source jobs.
	if q.records != nil && q.records.updating {
		for machine := range q.s.Machines {
			q.retainHostMemoryCharges(machine, q.clock.Now())
		}
	}
	q.refreshHostCPU()
	q.memoryLedgerGeneration++
	q.hostMemoryLedgers = make(map[string]*HostMemoryLedger, len(q.s.Machines))
	charges := map[string]*HostMemoryLedger{}
	for _, m := range q.s.Machines {
		available := m.HostAvailableMemoryGB
		observedAt, availableKnown := m.LastSeen, m.HostMemoryMeasured && m.HostMemoryError == ""
		for _, other := range q.s.Machines {
			if hostID(other) == hostID(m) && !other.Gone {
				observedAt = minTime(observedAt, other.LastSeen)
				availableKnown = availableKnown && other.HostMemoryMeasured && other.HostMemoryError == ""
				if !other.HostMemoryMeasured || other.HostMemoryError != "" {
					available = 0
				} else {
					available = min(available, other.HostAvailableMemoryGB)
				}
			}
		}

		m.hostBudgetGB = 0
		m.HostMemoryDerivation = nil
		m.HostMemoryPolicyMismatch = false
		for _, other := range q.s.Machines {
			if hostID(other) != hostID(m) {
				continue
			}
			if policy := q.s.HostMaintenance[hostID(m)].MemoryBudget; policy != nil && policy.Measured && (other.Gone || other.Replacement != nil) {
				continue
			}
			if other.HostMemoryBudgetGB != m.HostMemoryBudgetGB {
				m.HostMemoryPolicyMismatch = true
			}
			if other.HostMemoryBudgetGB > 0 && (m.hostBudgetGB == 0 || other.HostMemoryBudgetGB < m.hostBudgetGB) {
				m.hostBudgetGB = other.HostMemoryBudgetGB
			}
		}
		if policy, ok := q.s.HostMaintenance[hostID(m)]; ok && policy.MemoryBudget != nil {
			budget, derivation := q.measuredHostMemoryBudget(m, *policy.MemoryBudget)
			m.hostBudgetGB, m.HostMemoryDerivation = budget.GiB, derivation
			for _, other := range q.s.Machines {
				if policy.MemoryBudget.Measured && (other.Gone || other.Replacement != nil) {
					continue
				}
				if hostID(other) == hostID(m) && (other.HostMemoryBudgetGB != m.hostBudgetGB || other.HostMemoryBudgetMeasured != policy.MemoryBudget.Measured) {
					m.HostMemoryPolicyMismatch = true
				}
			}
		}
		allowance, _ := hostMemoryAllowance(m.hostBudgetGB)
		m.HostMemoryAllowanceBytes = allowance.Bytes
		m.HostMemoryQualification = ""
		if allowance.Bytes > 0 {
			m.HostMemoryQualification = "UNQUALIFIED"
		}
		m.EffectiveHostMemoryBudgetGB = m.hostBudgetGB
		owners := charges[hostID(m)]
		if owners == nil {
			owners = q.hostMemoryLedger(m)
			charges[hostID(m)] = owners
		}
		// Sibling targets share immutable owner facts; each target retains its
		// own physical/unified-memory admission calculation below.
		copy := *owners
		ledger := &copy
		reserved, growth := ledger.ReservedGB, ledger.GrowthGB
		m.HostMemoryInUseGB = reserved
		// Availability already has the operator reserve removed. Full envelopes
		// still consume the static budget; only unused growth consumes live RAM.
		room := available - growth
		room = q.unifiedCUDAHostRoom(m, room, growth)
		if m.hostBudgetGB > 0 || m.HostMemoryDerivation != nil {
			room = min(room, m.hostBudgetGB-reserved)
		}
		if d := m.HostMemoryDerivation; d != nil && d.Source == "quiescent_v1" {
			// Existing after-reserve telemetry and learned reserve are upper
			// bounds on the same physical pool, never independent allocations.
			room = min(room, m.MemoryObservation.TotalGB-m.MemoryObservation.UsedGB-d.ReserveGB-growth)
		}
		room = q.sharedHostRoom(m, room, reserved, growth)
		m.hostReservationDebtGB = max(0, -room)
		m.FreeHostMemoryGB = max(0, room)
		ledger.AvailableGB, ledger.AllowanceGB = available, m.hostBudgetGB
		ledger.ObservedAt, ledger.AvailableKnown = observedAt, availableKnown
		ledger.SignedRoomGB, ledger.DebtGB, ledger.FreeGB = room, m.hostReservationDebtGB, m.FreeHostMemoryGB
		q.hostMemoryLedgers[m.ID] = ledger
	}
}

// Plan releases first. Never credit stopping/draining work as free until its
// acknowledgement arrives. GPU releases are local; RAM releases may be on any
// card on the same physical host. Pins and failed unloads remain protected.
func (q *Queue) hostReleases(m *Machine, gpu, host float64, priority string, identity ...Spec) ([]*Job, []*ModelSlot, bool) {
	return q.hostReleasesExcept(m, gpu, host, priorityDemand(priority, identity), nil, nil)
}
func (q *Queue) hostReleasesExcept(m *Machine, gpu, host float64, demand Spec, releasedJobs map[*Job]bool, releasedSlots map[*ModelSlot]bool, releasePreparations ...bool) ([]*Job, []*ModelSlot, bool) {
	releasePreparation := len(releasePreparations) > 0 && releasePreparations[0]
	return q.hostReleasesWithVictims(m, gpu, host, demand, releasedJobs, releasedSlots, releasePreparation, func(j *Job) bool { return q.batchPreemptible(j, demand) })
}
func (q *Queue) hostReleasesWithVictims(m *Machine, gpu, host float64, demand Spec, releasedJobs map[*Job]bool, releasedSlots map[*ModelSlot]bool, releasePreparation bool, canStop func(*Job) bool) ([]*Job, []*ModelSlot, bool) {
	if m.HostMemoryBudgetStatus() != "" {
		return nil, nil, false
	}
	if host == 0 && (strings.HasPrefix(m.MemoryEnforcement, "memory ceilings enforced") || m.HostMemoryBudgetGB > 0 || m.hostBudgetGB > 0) {
		return nil, nil, false
	}
	cores := demand.Needs.cores()
	// Only legacy preparations inherit a core when a worker loses its last
	// execution. Independently charged builds retain their core throughout.
	executions, builds := map[string]int{}, map[string]int{}
	for _, p := range q.s.PreparationPlans {
		if owner := q.s.Machines[p.Machine]; p.Started && p.CPUCores == 0 && !(releasePreparation && p.Machine == m.ID) && owner != nil && hostID(owner) == hostID(m) {
			builds[p.Machine]++
		}
	}
	selected := map[string]int{}
	freedCPU := func(jobs []*Job, slots []*ModelSlot) int {
		clear(selected)
		freed := 0
		for _, j := range jobs {
			freed += j.Spec.Needs.cores()
			if builds[j.Machine] > 0 {
				selected[j.Machine]++
			}
		}
		for _, s := range slots {
			freed++
			if builds[s.Machine] > 0 {
				selected[s.Machine]++
			}
		}
		for worker, count := range selected {
			if count == executions[worker] {
				freed--
			}
		}
		return freed
	}
	cpuOK := func(jobs []*Job, slots []*ModelSlot) bool {
		copy := *m
		copy.CPUInUseCores = max(0, m.CPUInUseCores-freedCPU(jobs, slots))
		copy.FreeCPUCores = max(0, copy.EffectiveCPUCores-copy.CPUInUseCores)
		return cores == 0 || cpuAllocationFits(&copy, cores)
	}
	if (host == 0 || hostFits(m, gpu, host)) && cpuOK(nil, nil) {
		return nil, nil, true
	}
	if host > 0 && (!m.HostMemoryMeasured || m.HostMemoryError != "") {
		return nil, nil, false
	}
	// Free RAM is clamped in the live ledger. Repay any hidden reservation
	// debt before crediting candidate releases (#104).
	room := m.FreeHostMemoryGB - m.hostReservationDebtGB
	need := hostNeed(m, gpu, host)
	if host == 0 {
		need = 0
	}
	var jobs []*Job
	var slots []*ModelSlot
	for _, j := range q.liveJobs() {
		owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
		if !releasedJobs[j] && !j.ReleaseConfirmed && owner != nil && hostID(owner) == hostID(m) && (j.State == "running" || j.State == "stopping") {
			executions[j.Machine]++
		}
		if releasedJobs[j] || j.ReleaseConfirmed || owner == nil || owner.Gone || hostID(owner) != hostID(m) || !canStop(j) {
			continue
		}
		if j.State == "running" || j.State == "stopping" {
			jobs = append(jobs, j)
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if q.comparePriority(jobs[i].Spec, jobs[j].Spec) != 0 {
			return q.comparePriority(jobs[i].Spec, jobs[j].Spec) > 0
		}
		return jobs[i].ID < jobs[j].ID
	})
	for _, s := range q.s.ModelSlots {
		owner := slotMachine(q.s.Machines[s.Machine], s)
		if !releasedSlots[s] && slotReservesMemory(s) && owner != nil && hostID(owner) == hostID(m) {
			executions[s.Machine]++
		}
		if !releasedSlots[s] && slotReservesMemory(s) && owner != nil && !owner.Gone && hostID(owner) == hostID(m) && q.slotReclaimableFor(s, demand) {
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].InFlight != slots[j].InFlight {
			return slots[i].InFlight < slots[j].InFlight
		}
		return slotKey(slots[i].Machine, slots[i].Model, slots[i].Device) < slotKey(slots[j].Machine, slots[j].Model, slots[j].Device)
	})
	ramDeficit := max(0, need-room)
	// Fresh unified over-use is freed too, up to the shortfall (#596).
	freedRAM := func(j *Job) float64 {
		owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
		charge, _ := jobHostMemoryCharge(j, owner, hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), q.hostMemorySamples[j.Machine][jobKey(*j)], q.clock.Now())
		return charge + min(q.unifiedOveruseGB(j), ramDeficit)
	}
	// Exclude victims that free neither RAM nor cores, including build hand-back.
	jobs = slices.DeleteFunc(jobs, func(j *Job) bool {
		return freedRAM(j) == 0 && freedCPU([]*Job{j}, nil) == 0
	})
	slots = slices.DeleteFunc(slots, func(s *ModelSlot) bool {
		model, _ := q.model(s.Model)
		return hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB) == 0 && freedCPU(nil, []*ModelSlot{s}) == 0
	})
	// Reject impossible demands without constructing a partial release plan.
	allRAM := room
	for _, j := range jobs {
		allRAM += freedRAM(j)
	}
	for _, s := range slots {
		model, _ := q.model(s.Model)
		allRAM += hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB)
	}
	if allRAM < need || !cpuOK(jobs, slots) {
		return nil, nil, false
	}
	// This single-victim check plus greedy/reverse prune is a heuristic, not
	// always optimal; #100 accepts this tradeoff to keep check-ins inexpensive.
	ramCapacity := m.HostAvailableMemoryGB
	if m.hostBudgetGB > 0 {
		ramCapacity = min(ramCapacity, m.hostBudgetGB)
	}
	coreDeficit := max(0, m.CPUInUseCores+cores-m.EffectiveCPUCores)
	var bestJobs []*Job
	var bestSlots []*ModelSlot
	var bestTiers []int
	bestStops, bestExcess, bestID := 0, 0.0, ""
	consider := func(js []*Job, ss []*ModelSlot, spec Spec, stops int, ram float64, id string) {
		if room+ram < need || !cpuOK(js, ss) {
			return
		}
		tiers := make([]int, len(q.effectivePriorityPolicy().Tiers))
		tiers[q.priorityMatch(spec).Index] = stops
		excess := max(0, ram-ramDeficit)/max(1, ramCapacity) + float64(max(0, freedCPU(js, ss)-coreDeficit))/float64(max(1, m.EffectiveCPUCores))
		order := slices.Compare(tiers, bestTiers)
		if bestTiers == nil || order < 0 || order == 0 && (stops < bestStops || stops == bestStops && (excess < bestExcess || excess == bestExcess && id < bestID)) {
			bestJobs, bestSlots, bestTiers = js, ss, tiers
			bestStops, bestExcess, bestID = stops, excess, id
		}
	}
	for _, j := range jobs {
		stops := 0
		if j.State == "running" {
			stops = 1
		}
		consider([]*Job{j}, nil, j.Spec, stops, freedRAM(j), "job/"+j.ID)
	}
	for _, s := range slots {
		stops := 0
		if s.State != "draining" && s.State != "unloading" {
			stops = 1
		}
		model, _ := q.model(s.Model)
		consider(nil, []*ModelSlot{s}, q.slotSpec(s), stops, hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB), "slot/"+slotKey(s.Machine, s.Model, s.Device))
	}
	if bestTiers != nil {
		return bestJobs, bestSlots, true
	}
	// Prefer releasing model residency before interrupting commands.
	chosenSlots := []*ModelSlot{}
	for _, s := range slots {
		if room >= need && cpuOK(nil, chosenSlots) {
			break
		}
		model, _ := q.model(s.Model)
		n := hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB)
		if room < need && n > 0 || !cpuOK(nil, chosenSlots) && slotReservesMemory(s) {
			room += n
			chosenSlots = append(chosenSlots, s)
		}
	}
	chosenJobs := []*Job{}
	for _, j := range jobs {
		if room >= need && cpuOK(chosenJobs, chosenSlots) {
			break
		}
		n := freedRAM(j)
		if room < need && n > 0 || !cpuOK(chosenJobs, chosenSlots) && !j.ReleaseConfirmed {
			room += n
			chosenJobs = append(chosenJobs, j)
		}
	}
	// A later RAM release can also cover cores freed by an earlier pick.
	// Prune in reverse preference order, retaining the cheaper victims first.
	for i := len(chosenJobs) - 1; i >= 0; i-- {
		j := chosenJobs[i]
		n := freedRAM(j)
		remaining := slices.Delete(slices.Clone(chosenJobs), i, i+1)
		if room-n >= need && cpuOK(remaining, chosenSlots) {
			room -= n
			chosenJobs = remaining
		}
	}
	for i := len(chosenSlots) - 1; i >= 0; i-- {
		s := chosenSlots[i]
		model, _ := q.model(s.Model)
		n := hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB)
		remaining := slices.Delete(slices.Clone(chosenSlots), i, i+1)
		if room-n >= need && cpuOK(chosenJobs, remaining) {
			room -= n
			chosenSlots = remaining
		}
	}
	return chosenJobs, chosenSlots, room >= need && cpuOK(chosenJobs, chosenSlots)
}
func (q *Queue) reclaimableGPUMemory(m *Machine, priority string, identity ...Spec) float64 {
	demand := priorityDemand(priority, identity)
	room := m.FreeMemoryGB
	for _, s := range q.s.ModelSlots {
		if slotOnTarget(s, m) && q.slotReclaimableFor(s, demand) {
			model, _ := q.model(s.Model)
			room += model.MemoryGB
		}
	}
	for _, j := range q.liveJobs() {
		if batchOnTarget(j, m) && q.batchPreemptible(j, demand) {
			room += deviceShareGB(j)
		}
	}
	return room
}

// Dispatch and the worker launch recheck compare measured headroom with the
// same declared need/slack. The queue separately accounts for reservations.
func hostMemoryAvailableFits(m *Machine, gpu, host, available float64) bool {
	return validMemoryValue(available) && available >= hostNeed(m, gpu, host)
}

// CurrentHostMemory credits only nonreclaimable, uniquely charged cgroup RAM.
// Peaks and RSS (shared pages on macOS) cannot safely reduce growth room.
type CurrentHostMemory struct {
	// Stable worker-local observation identity, not a queue-clock timestamp.
	// Negotiated independently; cached/delayed deliveries cannot renew credit.
	SampleID int64 `json:"sample_id,omitempty"`
	// True only after two timed samples. An older worker's zero growth is
	// unknown and cannot release a cold probe's physical-headroom reservation.
	GrowthObserved bool `json:"growth_observed,omitempty"`
	// Maximum measured positive GiB/second. Extend the sampling allowance
	// through the observation's age instead of assuming growth stopped.
	GrowthRateGB float64 `json:"growth_rate_gb,omitempty"`
	// Complete cgroup anon/kernel/shmem/dirty/unevictable footprint. Separate
	// from anonymous-only credit; nil cannot establish a probe's live charge.
	FootprintGB *float64 `json:"footprint_gb,omitempty"`
	// Projection over this sensor's observed sampling horizon. Older workers
	// still protect their full unconsumed reservation, without this forecast.
	GrowthGB           float64 `json:"growth_gb,omitempty"`
	DeviceGrowthRateGB float64 `json:"device_growth_rate_gb,omitempty"`
	DeviceGrowthGB     float64 `json:"device_growth_gb,omitempty"`
	NonreclaimableGB   float64 `json:"nonreclaimable_gb"`
	AgeSeconds         float64 `json:"age_seconds"`
	sampledAt          time.Time
	// The latest device counter or labelled conservative unified-RAM estimate.
	// Exclusive marks uncertain ownership and forbids co-tenancy. Neither RSS
	// nor this estimate grants nonreclaimable host-memory credit.
	DeviceSource     string   `json:"device_source,omitempty"`
	DeviceExclusive  bool     `json:"device_exclusive,omitempty"`
	DeviceGB         *float64 `json:"device_gb,omitempty"`
	DeviceAgeSeconds float64  `json:"device_age_seconds,omitempty"`
	deviceSampledAt  time.Time
	deviceSample     *atomic.Pointer[hostMemorySample]
	deviceResidentGB float64 // Local direct CUDA single-process lower bound; never sent on the wire.
}

// Refresh device telemetry independently of the actor snapshot. Log/result I/O
// can delay the actor while its device sampler continues to make progress.
// This is an atomic read only: no process inventory or driver I/O on check-in.
func (s *CurrentHostMemory) aged(now time.Time) *CurrentHostMemory {
	if s == nil {
		return nil
	}
	sample := *s
	if !sample.sampledAt.IsZero() {
		sample.SampleID = sample.sampledAt.UnixNano()
	}
	if s.deviceSample != nil {
		if device := s.deviceSample.Load(); device != nil {
			gb := device.gb
			sample.DeviceGB, sample.deviceSampledAt, sample.DeviceGrowthGB = &gb, device.at, device.growthGB
			sample.DeviceSource, sample.DeviceExclusive, sample.DeviceGrowthRateGB = device.source, device.exclusive, device.rateGB
			sample.deviceResidentGB = device.residentGB
		}
	}
	sample.AgeSeconds = now.Sub(sample.sampledAt).Seconds()
	if sample.DeviceGB != nil {
		sample.DeviceAgeSeconds = now.Sub(sample.deviceSampledAt).Seconds()
	}
	return &sample
}

// Legacy bootstrap credit window until this exact attempt has a measured
// delivery interval. Losing resident credit never reduces its reservation.
const hostMemorySampleFreshFor = 5 * time.Second

// Device samples arrive every period; one missed sample makes them stale.
const deviceMemorySamplePeriod = 5 * time.Second
const deviceMemorySampleFreshFor = 2 * deviceMemorySamplePeriod

type hostMemorySample struct {
	id                       int64
	receivedAt               time.Time
	period                   time.Duration
	retainedGB               float64
	peakGB                   float64
	growthObserved           bool
	footprintGB              float64
	source                   string
	exclusive                bool
	gb, deviceGB             float64
	residentGB               float64
	growthGB, deviceGrowthGB float64
	rateGB, deviceRateGB     float64
	at, deviceAt             time.Time
}

func (s hostMemorySample) hostGrowth(now time.Time) (float64, bool) {
	if s.at.IsZero() || now.Before(s.at) || now.Sub(s.at) >= s.freshFor() || !validMemoryValue(s.gb) {
		return 0, false
	}
	growth := s.growthGB + s.rateGB*now.Sub(s.at).Seconds()
	return growth, validMemoryValue(growth)
}

func remainingHostGrowth(reservation float64, sample hostMemorySample, now time.Time) float64 {
	forecast, fresh := sample.hostGrowth(now)
	if !fresh {
		return reservation
	}
	// The envelope and the live forecast protect the same future bytes. Raw
	// availability already excludes resident anon: spend that credit once,
	// then protect whichever remaining bound is larger, including sample age.
	return max(0, reservation-sample.gb, forecast)
}

func currentHostMemorySamples(reports []AttemptReport, now time.Time) map[attemptKey]hostMemorySample {
	return updateHostMemorySamples(reports, now, nil)
}

func updateHostMemorySamples(reports []AttemptReport, now time.Time, previous map[attemptKey]hostMemorySample) map[attemptKey]hostMemorySample {
	samples := map[attemptKey]hostMemorySample{}
	for _, r := range reports {
		key := attemptKey{r.ID, r.Attempt, r.Session}
		old := previous[key]
		sample, s := r.CurrentHostMemory, hostMemorySample{}
		// Native helpers acquire physical custody before main Starts. Their
		// exact-attempt memory is occupancy evidence, never main-start evidence.
		if sample == nil || !validMemoryValue(sample.GrowthGB) || !validMemoryValue(sample.GrowthRateGB) || !validMemoryValue(sample.DeviceGrowthGB) || !validMemoryValue(sample.DeviceGrowthRateGB) {
			samples[key] = hostMemorySample{id: old.id, at: old.at, receivedAt: old.receivedAt, period: old.period, peakGB: old.peakGB, retainedGB: old.retainedGB}
			continue
		}
		if validMemoryValue(sample.NonreclaimableGB) && validMemoryValue(sample.AgeSeconds) {
			s.gb, s.at = sample.NonreclaimableGB, now.Add(-time.Duration(sample.AgeSeconds*float64(time.Second)))
			s.id = sample.SampleID
			if s.id == 0 && !sample.sampledAt.IsZero() {
				s.id = sample.sampledAt.UnixNano()
			}
			s.growthGB = sample.GrowthGB
			if sample.FootprintGB != nil && validMemoryValue(*sample.FootprintGB) {
				// Helper occupancy can reduce pending physical growth, but cannot
				// release a CPU probe's whole future main-execution reservation.
				s.growthObserved = r.Started && sample.GrowthObserved && (sample.GrowthGB == 0 || sample.GrowthRateGB > 0)
				s.footprintGB, s.rateGB = *sample.FootprintGB, sample.GrowthRateGB
			}
		}
		if d := sample.DeviceGB; d != nil && validMemoryValue(*d) && validMemoryValue(sample.DeviceAgeSeconds) && sample.DeviceAgeSeconds < deviceMemorySampleFreshFor.Seconds() {
			s.deviceGB, s.deviceAt = *d, now.Add(-time.Duration(sample.DeviceAgeSeconds*float64(time.Second)))
			s.deviceGrowthGB = sample.DeviceGrowthGB
			s.source, s.exclusive, s.deviceRateGB = sample.DeviceSource, sample.DeviceExclusive, sample.DeviceGrowthRateGB
			s.residentGB = sample.deviceResidentGB
		}
		s.peakGB = max(old.peakGB, s.gb, s.footprintGB)
		s.period, s.receivedAt = old.period, now
		if !old.at.IsZero() {
			if s.id > 0 && old.id == 0 {
				// Anchor the first negotiated identity. It might name the same
				// cached observation as the unidentified bootstrap, so retain its
				// expiry until a distinct identity arrives. A local sampler's
				// timestamp can independently prove a newer observation.
				if sample.sampledAt.IsZero() || !sample.sampledAt.After(old.at) {
					s.at, s.gb = old.at, min(s.gb, old.gb)
				}
			} else if s.id > 0 && old.id > 0 && s.id > old.id {
				// Learn delivery cadence only from proven distinct observations.
				s.period = max(s.period, now.Sub(old.receivedAt))
			} else {
				// Unknown identity, cached or out-of-order: retain the original
				// expiry and credit, while still retaining larger usage below.
				s.at, s.receivedAt, s.id = old.at, old.receivedAt, old.id
				s.gb = min(s.gb, old.gb)
				// A cached/unknown observation cannot replace the slope while
				// borrowing an earlier observation's identity and validity.
				s.growthGB, s.rateGB = old.growthGB, old.rateGB
				s.growthObserved = old.growthObserved
			}
		}
		// Only fresh identified observations can revise a prediction. Keep an
		// accepted prediction through missing/stale data, but let a distinct
		// observation replace it; measured peaks are retained independently.
		forecast, fresh := s.identifiedHostForecast(now)
		s.retainedGB = max(old.retainedGB, forecast)
		if fresh && (old.id == 0 || s.id > old.id) {
			s.retainedGB = forecast
		}
		if !s.at.IsZero() || !s.deviceAt.IsZero() || s.retainedGB > 0 {
			samples[key] = s
		}
	}
	return samples
}

// Bounded learned/exploratory estimates are admission reservations; their
// wider execution ceiling contains unseen growth but is not measured demand.
// Cold probes still need complete fresh evidence before releasing their offered
// envelope. Their adaptive execution ceiling is containment, not extra demand.
func jobHostMemoryCharge(j *Job, m *Machine, ceiling float64, sample hostMemorySample, now time.Time) (charge, growth float64) {
	if m != nil && m.MemoryEnforcement == "memory ceilings enforced (cgroup v2)" &&
		!j.Spec.Run.Docker && j.Spec.Needs.cpuOnly() && j.Spec.Needs.Memory != "auto" && j.Spec.Needs.HostMemoryGB > 0 {
		// The native attempt's parent cgroup contains main and all helpers at
		// hostNeed (declaration plus slack). Its complete reservation already
		// covers every possible growth forecast. Preserve contradictory actual
		// observations, but do not invent use beyond kernel containment. This
		// does not apply to RSS or accelerator memory outside the cgroup.
		charge = max(ceiling, retainedHostCharge(j), sample.peakGB, sample.gb, sample.footprintGB)
		if _, fresh := sample.hostGrowth(now); fresh {
			return charge, max(0, charge-sample.gb)
		}
		return charge, charge // Missing/stale data grants no resident credit.
	}
	adaptiveCeiling := adaptiveMemoryCeilingGB(j, m)
	hardCeiling := max(ceiling, adaptiveCeiling)
	hardContained := j.Spec.Needs.cpuOnly() && m.MemoryEnforcement == "memory ceilings enforced (cgroup v2)"
	e := j.MemoryEnvelope
	bounded := adaptiveCeiling > 0 && e.ProbeFloorGB <= 0
	charge = max(ceiling, retainedHostCharge(j))
	// A cold probe already reserves the full opportunity admitted beside its
	// neighbors. Promoting it to wider execution containment here can exceed the
	// host allowance and block startup before the first observation can arrive.
	if bounded {
		charge = boundedHostMemoryCharge(j, m, ceiling, adaptiveCeiling, sample, now)
	}
	coldObserved := 0.0
	if adaptiveCeiling > 0 && !bounded {
		// An unknown command can outgrow its original opportunity. Retain
		// measured demand independently of its replaceable live forecast.
		coldObserved = max(retainedHostCharge(j), sample.peakGB, sample.gb, sample.footprintGB)
		charge = max(charge, coldObserved+exploratoryDriftMarginGB(m))
	}
	if !bounded && !sample.at.IsZero() && !now.Before(sample.at) && now.Sub(sample.at) < sample.freshFor() {
		charge = max(charge, sample.hostForecast(now))
	}
	growth = remainingAllocationGrowth(m, j.Spec.Needs.MemoryGB, charge, sample, now)
	if hardContained {
		// Bound burst forecasts by the complete physical execution ceiling,
		// without assuming sampled residency remains present at admission.
		growth = min(growth, hardCeiling)
	}
	if e == nil || (e.ProbeFloorGB <= 0 && adaptiveCeiling <= 0) || j.Spec.Needs.Memory != "auto" || !j.Spec.Needs.cpuOnly() || j.State != "running" || !sample.growthObserved || sample.at.IsZero() || now.Before(sample.at) || now.Sub(sample.at) >= sample.freshFor() || !validMemoryValue(sample.gb) || !validMemoryValue(sample.growthGB) {
		return
	}
	floor := e.ProbeFloorGB
	if bounded {
		floor = charge // Estimates can grow, never earn probe credit.
	} else if adaptiveCeiling > 0 {
		floor = max(floor, coldObserved+exploratoryDriftMarginGB(m))
	}
	charge = floor
	if !bounded {
		charge = max(floor, sample.hostForecast(now)+exploratoryDriftMarginGB(m))
	}
	if m.MemoryEnforcement == "memory ceilings enforced (cgroup v2)" {
		// Adaptive guards use their physical execution ceiling, not the
		// admission estimate. Retain measured footprint and known floors.
		charge = max(floor, sample.gb, sample.footprintGB, min(charge, hardCeiling))
	}
	charge = boundedProcessMemoryCharge(m, floor, sample.footprintGB, charge)
	growth = max(0, charge-sample.gb)
	return
}

// Two observed delivery intervals allow one missed delivery. Until an interval
// is measured, preserve the legacy credit window. Receipt of the same cached
// sample cannot extend validity. Device attribution keeps its separate fence.
func (s hostMemorySample) freshFor() time.Duration {
	if s.period > 0 {
		return 2 * s.period
	}
	return hostMemorySampleFreshFor
}

func (s hostMemorySample) hostForecast(now time.Time) float64 {
	if s.at.IsZero() || now.Before(s.at) {
		return 0
	}
	return max(s.gb, s.footprintGB) + s.growthGB + s.rateGB*now.Sub(s.at).Seconds()
}

func (s hostMemorySample) identifiedHostForecast(now time.Time) (float64, bool) {
	if s.id <= 0 || !s.growthObserved {
		return 0, false
	}
	if _, fresh := s.hostGrowth(now); !fresh {
		return 0, false
	}
	return s.hostForecast(now), true
}

func boundedHostMemoryCharge(j *Job, m *Machine, ceiling, containment float64, sample hostMemorySample, now time.Time) float64 {
	retained := retainedHostCharge(j)
	forecast, fresh := sample.identifiedHostForecast(now)
	observed := max(sample.peakGB, sample.gb, sample.footprintGB)
	if c := j.HostMemoryCharge; retained > 0 && c != nil && c.ObservedGB != nil {
		observed = max(observed, *c.ObservedGB)
		if fresh && sample.id > c.SampleID {
			// A genuinely newer observation replaces forecast uncertainty. The
			// durable observed peak cannot be released, even across restart.
			retained = *c.ObservedGB
		}
	}
	drift := exploratoryDriftMarginGB(m)
	return max(ceiling, retained, observed+drift, min(containment, max(sample.retainedGB, forecast)+drift))
}

// Durable exact-attempt demand and replaceable forecast survive restart without
// preserving resident credit. Nil ObservedGB means a pre-provenance opaque floor:
// it cannot safely be attributed to forecast, so it remains conservative.
type AttemptHostMemoryCharge struct {
	Execution
	Session    string   `json:"session"`
	GiB        float64  `json:"gib"`
	ObservedGB *float64 `json:"observed_gb,omitempty"`
	SampleID   int64    `json:"sample_id,omitempty"`
}

func retainedHostCharge(j *Job) float64 {
	if c := j.HostMemoryCharge; c != nil && c.ID == j.ID && c.Attempt == j.Attempt && c.Session == j.Session {
		return c.GiB
	}
	return 0
}

func (q *Queue) retainHostMemoryCharges(machine string, now time.Time) {
	for _, j := range q.liveJobs() {
		m := q.s.Machines[j.Machine]
		if j.Machine != machine || j.ReleaseConfirmed || (j.State != "running" && j.State != "stopping") || adaptiveMemoryCeilingGB(j, m) <= 0 {
			continue
		}
		sample := q.hostMemorySamples[machine][jobKey(*j)]
		charge, _ := jobHostMemoryCharge(j, m, hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), sample, now)
		observed, id := 0.0, int64(0)
		if old := j.HostMemoryCharge; retainedHostCharge(j) > 0 && old != nil {
			observed, id = old.GiB, old.SampleID
			if old.ObservedGB != nil {
				observed = *old.ObservedGB
			}
		}
		observed = max(observed, sample.peakGB, sample.gb, sample.footprintGB)
		if j.MemoryEnvelope.ProbeFloorGB > 0 {
			// Cold probes may release the unused offered envelope after complete
			// Started evidence. Persist only actual high water, never the offer
			// or forecast, so a transient prediction cannot become a floor.
			charge = max(retainedHostCharge(j), observed)
		}
		if _, fresh := sample.identifiedHostForecast(now); fresh {
			id = max(id, sample.id)
		}
		old := j.HostMemoryCharge
		if charge != retainedHostCharge(j) || old == nil || old.ObservedGB == nil || observed != *old.ObservedGB || id != old.SampleID {
			j = q.touchJob(j.ID)
			j.HostMemoryCharge = &AttemptHostMemoryCharge{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, GiB: charge, ObservedGB: &observed, SampleID: id}
		}
	}
}

func (q *Queue) hostJobCharge(j *Job) float64 {
	m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	charge, _ := jobHostMemoryCharge(j, m, hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), q.hostMemorySamples[j.Machine][jobKey(*j)], q.clock.Now())
	return charge
}
