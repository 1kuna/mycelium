package myce

import "time"

const memoryPressureCause = "memory_pressure"

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

func (q *Queue) memoryPoolShort(m *Machine, r poolAvailability) bool {
	if !workerUsesMemoryPools(m) || m.Gone || q.unreported[m.ID] || !m.HostMemoryMeasured || m.HostMemoryError != "" || r.session != m.Session {
		return false
	}
	if !q.workerFresh(m, q.clock.Now()) {
		return false
	}
	if m.CapabilityProfile != nil && m.CapabilityProfile.OS == "darwin" && m.HostHealth != nil && len(m.HostHealth.History) > 0 {
		if latest := m.HostHealth.History[len(m.HostHealth.History)-1]; latest.Error == "" && latest.Pressure > 1 {
			return true
		}
	}
	room := r.free - q.pooledKeep(m)
	// Act immediately below the reserve or if even the next sample could
	// arrive too late. Use the sustained rate for the longer stop horizon.
	return room < r.growth*r.period.Seconds() ||
		room < r.sustainedGrowth*(r.period+q.preemptionGrace()).Seconds()
}

func (q *Queue) yieldPooledMemory() {
	short := map[string]*Machine{}
	for id, r := range q.poolAvailability {
		if m := q.s.Machines[id]; m != nil && r.at.After(q.poolLastStop[hostID(m)]) && q.memoryPoolShort(m, r) {
			short[hostID(m)] = m
		}
	}
	for host, machine := range short {
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
			if victim == nil || q.poolPressureBefore(j, victim, machine) {
				victim = j
			}
		}
		if pending || victim == nil || jobClass(victim.Spec) == "production" && production < 2 {
			continue
		}
		q.stopForPoolPressure(victim, "host/"+host)
		if q.poolLastStop == nil {
			q.poolLastStop = map[string]time.Time{}
		}
		q.poolLastStop[host] = q.clock.Now()
	}
}

func (q *Queue) poolPressureBefore(a, b *Job, m *Machine) bool {
	if tier := q.comparePriority(a.Spec, b.Spec); tier != 0 {
		return tier > 0
	}
	overage := func(j *Job) float64 {
		sample := q.hostMemorySamples[j.Machine][jobKey(*j)]
		return max(0, pooledFootprint(sample, unifiedCUDA(m))-hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB))
	}
	if av, bv := overage(a), overage(b); av != bv {
		return av > bv
	}
	if a.Spec.Resumable != b.Spec.Resumable {
		return a.Spec.Resumable
	}
	if a.StartedAt.Equal(*b.StartedAt) {
		return a.ID < b.ID
	}
	return a.StartedAt.After(*b.StartedAt)
}

func (q *Queue) stopForPoolPressure(victim *Job, demand string) {
	q.touchJob(victim.ID)
	for i := range victim.Copies {
		c := &victim.Copies[i]
		if c.Attempt == victim.Attempt && c.Machine == victim.Machine && c.Session == victim.Session {
			c.StopCause = memoryPressureCause
			c.PriorityStop = &PriorityPreemption{Cause: memoryPressureCause, Demand: demand, Victim: victim.ID, Policy: q.priorityMatch(victim.Spec)}
			c.Recoverable = false
		}
	}
	q.stopFor(victim, "memory pool", "memory")
	victim.Reason = memoryPressureCause
}
