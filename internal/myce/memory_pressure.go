package myce

import "time"

const memoryPressureCause = "memory_pressure"

type poolAvailability struct {
	session string
	at      time.Time
	free    float64
	growth  float64
	period  time.Duration
}

func (q *Queue) observePoolAvailability(m *Machine, period time.Duration) {
	if !workerUsesMemoryPools(m) || !m.HostMemoryMeasured || m.HostMemoryError != "" {
		return
	}
	now := q.clock.Now()
	available := m.HostAvailableMemoryGB
	if observation := m.MemoryObservation; validHostMemoryObservation(observation) {
		available = observation.TotalGB - observation.UsedGB
	}
	previous := q.poolAvailability[m.ID]
	reading := poolAvailability{session: m.Session, at: now, free: available, period: period}
	if dt := now.Sub(previous.at); previous.session == m.Session && dt > 0 {
		reading.growth = max(0, previous.free-available) / dt.Seconds()
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
	return r.free-q.pooledKeep(m) < r.growth*(r.period+q.preemptionGrace()).Seconds()
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
			if cancelled(j) || plannedStop(j) || j.StartedAt == nil || !pooledMemory(j) {
				continue
			}
			if victim == nil || q.poolPressureBefore(j, victim, machine) {
				victim = j
			}
		}
		if pending || victim == nil || jobClass(victim.Spec) == "production" && production < 2 {
			continue
		}
		q.touchJob(victim.ID)
		for i := range victim.Copies {
			c := &victim.Copies[i]
			if c.Attempt == victim.Attempt && c.Machine == victim.Machine && c.Session == victim.Session {
				c.StopCause = memoryPressureCause
				c.PriorityStop = &PriorityPreemption{Cause: memoryPressureCause, Demand: "host/" + host, Victim: victim.ID, Policy: q.priorityMatch(victim.Spec)}
				c.Recoverable = false
			}
		}
		q.stopFor(victim, "host", "memory")
		victim.Reason = memoryPressureCause
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
		return max(0, pooledFootprint(sample, unifiedCUDA(m))-memoryPools(j).RAM.ChargeGB)
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
