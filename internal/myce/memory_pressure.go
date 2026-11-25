package myce

import "time"

const memoryPressureCause = "memory_pressure"
const memoryPressureRetryHold = "memory_pressure_retry"

// One retry tests whether the first interruption was transient. A second stop
// on the same pool without saved progress is evidence against repeating it.
const poolPressureStopsWithoutProgress = 2

// PoolPressureDecision captures the sample used to order the stop. The
// completion-time host snapshot can already reflect the memory it released.
type PoolPressureDecision struct {
	Check         string    `json:"check"`
	ObservedAt    time.Time `json:"observed_at"`
	FreeGB        float64   `json:"free_gb"`
	KeepGB        float64   `json:"keep_gb"`
	NextSampleGB  float64   `json:"next_sample_gb"`
	StopHorizonGB float64   `json:"stop_horizon_gb"`
	Checkpoint    int64     `json:"checkpoint_generation"`
	PauseRevision uint64    `json:"pause_revision"`
}

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

func (q *Queue) memoryPoolShort(m *Machine, r poolAvailability) *PoolPressureDecision {
	if !workerUsesMemoryPools(m) || m.Gone || q.unreported[m.ID] || !m.HostMemoryMeasured || m.HostMemoryError != "" || r.session != m.Session {
		return nil
	}
	if !q.workerFresh(m, q.clock.Now()) {
		return nil
	}
	d := PoolPressureDecision{ObservedAt: r.at, FreeGB: r.free, KeepGB: q.pooledKeep(m),
		NextSampleGB: r.growth * r.period.Seconds(), StopHorizonGB: r.sustainedGrowth * (r.period + q.preemptionGrace()).Seconds()}
	room := d.FreeGB - d.KeepGB
	switch {
	case room < 0:
		d.Check = "reserve"
	case room < d.NextSampleGB:
		d.Check = "next_sample"
	case room < d.StopHorizonGB:
		d.Check = "stop_horizon"
	}
	if m.CapabilityProfile != nil && m.CapabilityProfile.OS == "darwin" && m.HostHealth != nil && len(m.HostHealth.History) > 0 {
		if latest := m.HostHealth.History[len(m.HostHealth.History)-1]; latest.Error == "" && latest.Pressure > 1 {
			d.Check = "os_pressure"
		}
	}
	if d.Check == "" {
		return nil
	}
	return &d
}

func (q *Queue) yieldPooledMemory() {
	short := map[string]*PoolPressureDecision{}
	for id, r := range q.poolAvailability {
		if m := q.s.Machines[id]; m != nil && r.at.After(q.poolLastStop[hostID(m)]) {
			if d := q.memoryPoolShort(m, r); d != nil {
				host := hostID(m)
				if old := short[host]; old == nil || d.ObservedAt.After(old.ObservedAt) ||
					d.ObservedAt.Equal(old.ObservedAt) && d.FreeGB-d.KeepGB < old.FreeGB-old.KeepGB {
					short[host] = d
				}
			}
		}
	}
	for host, decision := range short {
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
		q.stopForPoolPressure(victim, "host/"+host, decision)
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

func (q *Queue) stopForPoolPressure(victim *Job, demand string, decision *PoolPressureDecision) {
	q.touchJob(victim.ID)
	if decision != nil {
		decision.Checkpoint, decision.PauseRevision = poolPressureCheckpoint(victim), victim.PauseRevision
	}
	for i := range victim.Copies {
		c := &victim.Copies[i]
		if c.Attempt == victim.Attempt && c.Machine == victim.Machine && c.Session == victim.Session {
			if decision != nil && !victim.FileTransport && victim.Spec.Resumable {
				if c.ResumeCheckpointGeneration != nil {
					decision.Checkpoint = max(decision.Checkpoint, *c.ResumeCheckpointGeneration)
				} else if origin := victim.CheckpointTarget; origin != nil && origin.Attempt < victim.Attempt {
					// Older workers do not report what they copied. Keep
					// the previously accepted generation as their floor.
					decision.Checkpoint = max(decision.Checkpoint, origin.Generation)
				}
			}
			c.StopCause = memoryPressureCause
			c.PriorityStop = &PriorityPreemption{Cause: memoryPressureCause, Demand: demand, Victim: victim.ID, Policy: q.priorityMatch(victim.Spec), PoolPressure: decision}
			c.Recoverable = false
		}
	}
	q.stopFor(victim, "memory pool", "memory")
	victim.Reason = memoryPressureCause
}

func poolPressureCheckpoint(j *Job) int64 {
	if !j.Spec.Resumable {
		return 0
	}
	generation := max(0, j.Progress, j.LocalCheckpointGeneration)
	if save := bestFileCheckpoint(j); save != nil {
		generation = max(generation, save.Generation)
	}
	return generation
}

func poolPressureReadmissionHold(j *Job, m *Machine) *Hold {
	if j.State != "queued" {
		return nil
	}
	host := "host/" + hostID(m)
	progress := poolPressureCheckpoint(j)
	stops := 0
	for i := len(j.Copies) - 1; i >= 0; i-- {
		copy := &j.Copies[i]
		stop := copy.PriorityStop
		if stop == nil || stop.Cause != memoryPressureCause || stop.Demand != host || stop.PoolPressure == nil {
			continue
		}
		d := stop.PoolPressure
		if d.PauseRevision != j.PauseRevision {
			break
		}
		baseline := d.Checkpoint
		if copy.ResumeCheckpointGeneration != nil {
			baseline = *copy.ResumeCheckpointGeneration
		}
		if stops == 0 && progress > baseline {
			break // This attempt produced a newer checkpoint than it started with.
		}
		if stops == 0 {
			progress = baseline
		}
		if baseline != progress {
			break
		}
		stops++
		if stops == poolPressureStopsWithoutProgress {
			return newHold(memoryPressureRetryHold, "two host memory-pressure stops at the same checkpoint generation")
		}
	}
	return nil
}
