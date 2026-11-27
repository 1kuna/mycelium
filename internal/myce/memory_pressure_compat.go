package myce

import "time"

// Workers without the stage 1 report and unified RAM pending 0b-1 keep
// their previous stop behavior. Qualified reports take the R4 path.

type poolAvailability struct {
	session         string
	at              time.Time
	free            float64
	growth          float64
	sustainedGrowth float64
	period          time.Duration
	raw             bool
}

func (q *Queue) observePoolAvailability(m *Machine, period time.Duration, receivedAt time.Time) {
	previous := q.poolAvailability[m.ID]
	if previous.session == m.Session && receivedAt.Before(previous.at) {
		return
	}
	if !workerUsesMemoryPools(m) || !m.HostMemoryMeasured || m.HostMemoryError != "" {
		delete(q.poolAvailability, m.ID)
		return
	}
	reading := poolAvailability{session: m.Session, at: receivedAt, free: m.HostAvailableMemoryGB, period: period}
	if o := m.MemoryObservation; validHostMemoryObservation(o) {
		reading.free, reading.raw = o.TotalGB-o.UsedGB, true
	}
	if dt := receivedAt.Sub(previous.at); previous.session == m.Session && previous.raw == reading.raw && dt > 0 {
		reading.growth = max(0, previous.free-reading.free) / dt.Seconds()
		// A single allocation changes the level without proving a lasting
		// rate. Project across the stop grace only at a rate both intervals
		// support. A stall or recovery resets that rate.
		reading.sustainedGrowth = min(previous.growth, reading.growth)
	}
	if q.poolAvailability == nil {
		q.poolAvailability = map[string]poolAvailability{}
	}
	q.poolAvailability[m.ID] = reading
}

func (q *Queue) memoryPoolShort(m *Machine, r poolAvailability) *MemoryPoolStop {
	if !workerUsesMemoryPools(m) || m.Gone || q.unreported[m.ID] || !m.HostMemoryMeasured || m.HostMemoryError != "" || r.session != m.Session {
		return nil
	}
	if !q.workerFresh(m, q.clock.Now()) {
		return nil
	}
	grace := q.preemptionGrace()
	d := MemoryPoolStop{ObservedAt: r.at, AvailableGB: r.free, ReserveGB: q.pooledKeep(m),
		GrowthRate: r.growth, SustainedGrowthRate: r.sustainedGrowth,
		SampleSeconds: r.period.Seconds(), GraceSeconds: grace.Seconds(),
		NextLossGB: r.growth * r.period.Seconds(), GraceLossGB: r.sustainedGrowth * (r.period + grace).Seconds()}
	if m.CapabilityProfile != nil && m.CapabilityProfile.OS == "darwin" && m.HostHealth != nil && len(m.HostHealth.History) > 0 {
		if latest := m.HostHealth.History[len(m.HostHealth.History)-1]; latest.Error == "" && latest.Pressure > 1 {
			d.Trigger = "host_pressure"
			d.HostPressure = latest.Pressure
			return &d
		}
	}
	// Act immediately below the reserve or if even the next sample could
	// arrive too late. Use the sustained rate for the longer stop horizon.
	room := d.AvailableGB - d.ReserveGB
	switch {
	case room < d.NextLossGB:
		d.Trigger = "next_sample"
	case room < d.GraceLossGB:
		d.Trigger = "stop_grace"
	default:
		return nil
	}
	if room < 0 {
		d.Trigger = "below_reserve"
	}
	return &d
}

func (q *Queue) yieldPooledMemory() {
	short := map[string]*MemoryPoolStop{}
	for id, r := range q.poolAvailability {
		m := q.s.Machines[id]
		if m == nil || usesAttemptStopForecast(m, "ram") || !r.at.After(q.poolLastStop[hostID(m)]) {
			continue
		}
		host := hostID(m)
		if d := q.memoryPoolShort(m, r); d != nil && (short[host] == nil || d.ObservedAt.After(short[host].ObservedAt)) {
			short[host] = d
		}
	}
	for host, observation := range short {
		var victim *Job
		production, pending := 0, false
		for _, j := range q.liveJobs() {
			m := q.s.Machines[j.Machine]
			if m == nil || hostID(m) != host || !batchReservesCapacity(j) {
				continue
			}
			if j.State == "stopping" {
				pending = true
				continue
			}
			if jobClass(j.Spec) == "production" {
				production++
				if !j.Spec.Resumable {
					continue
				}
			}
			if cancelled(j) || plannedStop(j) || j.StartedAt == nil {
				continue
			}
			if victim == nil || q.poolPressureBefore(j, victim) {
				victim = j
			}
		}
		if pending || victim == nil || jobClass(victim.Spec) == "production" && production < 2 {
			continue
		}
		q.stopForPoolPressure(victim, PriorityPreemption{Demand: "host/" + host, MemoryPool: observation})
		if q.poolLastStop == nil {
			q.poolLastStop = map[string]time.Time{}
		}
		q.poolLastStop[host] = q.clock.Now()
	}
}

func (q *Queue) poolPressureBefore(a, b *Job) bool {
	if tier := q.comparePriority(a.Spec, b.Spec); tier != 0 {
		return tier > 0
	}
	overage := func(j *Job) float64 {
		sample := q.hostMemorySamples[j.Machine][jobKey(*j)]
		m := q.planningAllocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
		if m == nil || !sample.freshAt(q.clock.Now()) {
			return 0
		}
		return max(0, pooledFootprint(sample, unifiedCUDA(m))-hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB))
	}
	if av, bv := overage(a), overage(b); av != bv {
		return av > bv
	}
	if a.Spec.Resumable != b.Spec.Resumable {
		return a.Spec.Resumable
	}
	if a.StartedAt != nil && b.StartedAt != nil && !a.StartedAt.Equal(*b.StartedAt) {
		return a.StartedAt.After(*b.StartedAt)
	}
	if (a.StartedAt == nil) != (b.StartedAt == nil) {
		return a.StartedAt == nil
	}
	return a.ID < b.ID
}

// The discrete pool uses current driver availability and per-attempt
// footprints. Unified allocations are handled by the host pool.
func (q *Queue) guardLegacyCardPressure(m *Machine) {
	if m.Gone || m.Leaving || q.unreported[m.ID] || m.Replacement != nil {
		return
	}
	for _, d := range m.Devices {
		if d.UnifiedMemory || d.BudgetGB <= 0 || usesAttemptStopForecast(m, "card/"+d.ID) {
			continue
		}
		card := d.machine(*m)
		var victim *Job
		var overage, used float64
		pending := false
		for _, j := range q.liveJobs() {
			if !batchOnTarget(j, &card) || !batchReservesCapacity(j) {
				continue
			}
			pending = pending || j.State == "stopping"
			if j.StartedAt == nil || j.Session != m.Session || cancelled(j) {
				continue
			}
			s := q.hostMemorySamples[m.ID][jobKey(*j)]
			if !s.deviceAt.IsZero() && q.clock.Now().Sub(s.deviceAt) < deviceMemorySampleFreshFor {
				used += s.deviceGB
				over := s.deviceGB - deviceShareGB(j)
				if victim == nil || q.comparePriority(j.Spec, victim.Spec) > 0 ||
					q.comparePriority(j.Spec, victim.Spec) == 0 && (over > overage || over == overage && j.ID < victim.ID) {
					victim, overage = j, over
				}
			}
		}
		budgetExceeded := used > d.BudgetGB
		short := budgetExceeded || d.MemoryMeasured && d.FreeMemoryGB <= 0 && overage > 0
		if victim == nil || pending || !short {
			continue
		}
		trigger := "device_full"
		if budgetExceeded {
			trigger = "over_budget"
		}
		q.stopForPoolPressure(victim, PriorityPreemption{Demand: "device/" + d.ID, DevicePool: &DevicePoolStop{
			ObservedAt: q.clock.Now(), BudgetGB: d.BudgetGB, UsedGB: used, FreeGB: d.FreeMemoryGB,
			MemoryMeasured: d.MemoryMeasured, OverageGB: overage, Trigger: trigger,
		}})
	}
}
