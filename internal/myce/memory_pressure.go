package myce

import "time"

const memoryPressureCause = "memory_pressure"

func queuedPoolStop(j *Job) bool {
	stop := priorityStop(j)
	return stop != nil && stop.Cause == memoryPressureCause
}

type poolAvailability struct {
	session          string
	at               time.Time
	free             float64
	unreserved       float64
	reserved         float64
	previousOther    float64
	previousReserved float64
	reservedRoom     float64
	period           time.Duration
	raw              bool
	attempts         map[attemptKey]poolPressureSample
}

type poolPressureSample struct {
	id       int64
	resident float64
	charge   float64
	distinct bool
}

func (q *Queue) observePoolAvailability(m *Machine, period time.Duration, receivedAt time.Time, reports []AttemptReport) {
	previous := q.poolAvailability[m.ID]
	if previous.session == m.Session && receivedAt.Before(previous.at) ||
		m.HostRAMGate != nil && !m.hostRAMGateAt.Equal(receivedAt) {
		return
	}
	if !workerUsesMemoryPools(m) || !m.HostMemoryMeasured || m.HostMemoryError != "" {
		delete(q.poolAvailability, m.ID)
		return
	}
	reading := poolAvailability{session: m.Session, at: receivedAt, free: m.HostAvailableMemoryGB, period: period}
	if m.HostRAMGate != nil {
		reading.free, reading.raw = m.HostRAMGate.RawGB, true
	} else if o := m.MemoryObservation; validHostMemoryObservation(o) {
		reading.free, reading.raw = o.TotalGB-o.UsedGB, true
	}
	reading.attempts = make(map[attemptKey]poolPressureSample, len(reports))
	for _, r := range reports {
		j := q.s.Jobs[r.ID]
		s := r.CurrentHostMemory
		if j == nil || j.Memory == nil || j.Machine != m.ID || j.Session != m.Session ||
			j.Session != r.Session || j.Attempt != r.Attempt || !batchReservesCapacity(j) ||
			s == nil || s.SampleID <= 0 || !validMemoryValue(s.NonreclaimableGB) ||
			!validMemoryValue(s.AgeSeconds) || s.AgeSeconds >= max(period, hostMemorySampleFreshFor).Seconds() {
			continue
		}
		// Only uniquely charged RAM can explain a drop in physical headroom.
		// Estimated or overlapping device use stays in the physical trend.
		key := jobKey(*j)
		if old := previous.attempts[key]; previous.session == m.Session && s.SampleID <= old.id {
			reading.attempts[key] = poolPressureSample{id: old.id}
			continue
		}
		reading.attempts[key] = poolPressureSample{id: s.SampleID, resident: s.NonreclaimableGB, charge: j.Memory.RAM.ChargeGB, distinct: true}
	}
	if dt := receivedAt.Sub(previous.at); previous.session == m.Session && previous.raw == reading.raw && dt > 0 {
		var reservedGrowth float64
		for key, now := range reading.attempts {
			old, ok := previous.attempts[key]
			if !ok || !old.distinct || !now.distinct || now.charge != old.charge {
				continue
			}
			change := min(now.charge, now.resident) - min(now.charge, old.resident)
			reservedGrowth += change
			if change > 0 {
				reading.reservedRoom += max(0, now.charge-now.resident)
			}
		}
		drop := max(0, previous.free-reading.free)
		reading.reserved = min(drop, max(0, reservedGrowth)) / dt.Seconds()
		reading.unreserved = drop/dt.Seconds() - reading.reserved
		// A single allocation changes the level without proving a lasting
		// rate. Project across the stop grace only at a rate both intervals
		// support. A stall or recovery resets that rate.
		reading.previousOther, reading.previousReserved = previous.unreserved, previous.reserved
	}
	if q.poolAvailability == nil {
		q.poolAvailability = map[string]poolAvailability{}
	}
	q.poolAvailability[m.ID] = reading
}

// MemoryPoolStop is the queue's decision-time comparison. The completion
// observation can show recovery after the victim released RAM.
type MemoryPoolStop struct {
	ObservedAt          time.Time `json:"observed_at"`
	AvailableGB         float64   `json:"available_gb"`
	ReserveGB           float64   `json:"reserve_gb"`
	ReservedGrowthRate  float64   `json:"reserved_growth_gb_per_second"`
	OtherGrowthRate     float64   `json:"other_growth_gb_per_second"`
	RemainingReservedGB float64   `json:"remaining_reserved_gb"`
	NextLossGB          float64   `json:"next_loss_gb"`
	GraceLossGB         float64   `json:"grace_loss_gb"`
	SampleSeconds       float64   `json:"sample_seconds"`
	GraceSeconds        float64   `json:"grace_seconds"`
	Trigger             string    `json:"trigger"`
}

func (q *Queue) memoryPoolShort(m *Machine, r poolAvailability) *MemoryPoolStop {
	if !workerUsesMemoryPools(m) || m.Gone || q.unreported[m.ID] || !m.HostMemoryMeasured || m.HostMemoryError != "" || r.session != m.Session {
		return nil
	}
	if !q.workerFresh(m, q.clock.Now()) {
		return nil
	}
	// Act immediately below the reserve or if even the next sample could
	// arrive too late. Continue measured in-charge growth only into the
	// remaining reservation. Extrapolate unattributed and over-charge loss.
	next, grace := r.period.Seconds(), q.preemptionGrace().Seconds()
	loss := func(other, reserved, horizon float64) float64 {
		return other*horizon + min(reserved*horizon, r.reservedRoom)
	}
	d := MemoryPoolStop{ObservedAt: r.at, AvailableGB: r.free, ReserveGB: q.pooledKeep(m),
		ReservedGrowthRate: r.reserved, OtherGrowthRate: r.unreserved, RemainingReservedGB: r.reservedRoom,
		SampleSeconds: next, GraceSeconds: grace,
		NextLossGB:  loss(r.unreserved, r.reserved, next),
		GraceLossGB: min(loss(r.previousOther, r.previousReserved, next+grace), loss(r.unreserved, r.reserved, next+grace))}
	room := d.AvailableGB - d.ReserveGB
	switch {
	case room < 0:
		d.Trigger = "below_reserve"
	case room < d.NextLossGB:
		d.Trigger = "next_sample"
	case room < d.GraceLossGB:
		d.Trigger = "stop_grace"
	case m.CapabilityProfile != nil && m.CapabilityProfile.OS == "darwin" && m.HostHealth != nil && len(m.HostHealth.History) > 0:
		if latest := m.HostHealth.History[len(m.HostHealth.History)-1]; latest.Error == "" && latest.Pressure > 1 {
			d.Trigger = "host_pressure"
		}
	}
	if d.Trigger == "" {
		return nil
	}
	return &d
}

func (q *Queue) yieldPooledMemory() {
	short := map[string]*MemoryPoolStop{}
	for id, r := range q.poolAvailability {
		m := q.s.Machines[id]
		if m == nil || !r.at.After(q.poolLastStop[hostID(m)]) {
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
		q.stopForPoolPressure(victim, "host/"+host, observation)
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

func (q *Queue) stopForPoolPressure(victim *Job, demand string, observation *MemoryPoolStop) {
	q.touchJob(victim.ID)
	for i := range victim.Copies {
		c := &victim.Copies[i]
		if c.Attempt == victim.Attempt && c.Machine == victim.Machine && c.Session == victim.Session {
			c.StopCause = memoryPressureCause
			c.PriorityStop = &PriorityPreemption{Cause: memoryPressureCause, Demand: demand, Victim: victim.ID, Policy: q.priorityMatch(victim.Spec), MemoryPool: observation}
			c.Recoverable = false
		}
	}
	q.stopFor(victim, "memory pool", "memory")
	victim.Reason = memoryPressureCause
}
