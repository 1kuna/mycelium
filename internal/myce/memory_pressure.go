package myce

import (
	"fmt"
	"time"
)

const memoryPressureCause = "memory_pressure"

const memoryPressureRetryHold = "memory_pressure_retry"

// Losses six minutes apart repeated nine times in the incident. Count two
// stops within ten minutes, then skip at least two of those retry cycles.
// Recovery, even with new progress, may wait up to fifteen minutes on this host.
const memoryPressureStopWindow = 10 * time.Minute
const memoryPressureCoolOff = 15 * time.Minute

// PoolPressureDecision retains the numbers at the stop, before cleanup can
// change room. The comparison is diagnostic and never grants capacity.
type PoolPressureDecision struct {
	Check         string    `json:"check"`
	ObservedAt    time.Time `json:"observed_at"`
	FreeGB        float64   `json:"free_gb"`
	KeepGB        float64   `json:"keep_gb"`
	NextSampleGB  float64   `json:"next_sample_gb"`
	StopHorizonGB float64   `json:"stop_horizon_gb"`
	OSPressure    float64   `json:"os_pressure,omitempty"`
}

type MemoryPressureRetry struct {
	LastStopAt time.Time             `json:"last_stop_at"`
	RetryAt    time.Time             `json:"retry_at,omitempty,omitzero"`
	Decision   *PoolPressureDecision `json:"decision,omitempty"`
}

func memoryRetryActive(j *Job, host string, now time.Time) bool {
	return now.Before(j.LaunchCapacityWait[host].RetryAt) || now.Before(j.MemoryPressureRetries[host].RetryAt)
}

func (q *Queue) recordMemoryPressureStop(j *Job, host string, decision *PoolPressureDecision) {
	if j.MemoryPressureRetries == nil {
		j.MemoryPressureRetries = map[string]MemoryPressureRetry{}
	}
	now := q.clock.Now()
	prior := j.MemoryPressureRetries[host]
	retry := MemoryPressureRetry{LastStopAt: now, Decision: decision}
	if elapsed := now.Sub(prior.LastStopAt); !prior.LastStopAt.IsZero() && elapsed >= 0 && elapsed <= memoryPressureStopWindow {
		retry.RetryAt = now.Add(memoryPressureCoolOff)
		j.PauseRevision++ // Fence a resume request from before this hold.
	}
	j.MemoryPressureRetries[host] = retry
}

func (r MemoryPressureRetry) hold(host string, now time.Time) *Hold {
	if !now.Before(r.RetryAt) {
		return nil
	}
	detail := "two host memory-pressure stops within 10 minutes"
	if d := r.Decision; d != nil {
		detail += fmt.Sprintf("; last trigger %s: %.3f GiB room (%.3f GiB free, %.3f GiB reserve), %.3f GiB projected next sample, %.3f GiB projected through stop grace",
			d.Check, d.FreeGB-d.KeepGB, d.FreeGB, d.KeepGB, d.NextSampleGB, d.StopHorizonGB)
		if d.Check == "os_pressure" {
			detail += fmt.Sprintf(", OS pressure %g > 1", d.OSPressure)
		}
	}
	h := newHold(memoryPressureRetryHold, detail)
	h.Scope, h.Expires = host, &r.RetryAt
	return h
}

func (q *Queue) memoryPressureWait(j *Job) *Hold {
	now := q.clock.Now()
	var hold *Hold
	for host, r := range j.MemoryPressureRetries {
		if now.Before(r.RetryAt) && (hold == nil || r.RetryAt.Before(*hold.Expires) || r.RetryAt.Equal(*hold.Expires) && host < hold.Scope) {
			hold = r.hold(host, now)
		}
	}
	return hold
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
	if m.CapabilityProfile != nil && m.CapabilityProfile.OS == "darwin" && m.HostHealth != nil && len(m.HostHealth.History) > 0 {
		if latest := m.HostHealth.History[len(m.HostHealth.History)-1]; latest.Error == "" && latest.Pressure > 1 {
			d.Check, d.OSPressure = "os_pressure", latest.Pressure
			return &d
		}
	}
	room := d.FreeGB - d.KeepGB
	// Act immediately below the reserve or if even the next sample could
	// arrive too late. Use the sustained rate for the longer stop horizon.
	switch {
	case room < d.NextSampleGB:
		d.Check = "next_sample"
	case room < d.StopHorizonGB:
		d.Check = "stop_horizon"
	default:
		return nil
	}
	return &d
}

func (q *Queue) yieldPooledMemory() {
	short := map[string]*PoolPressureDecision{}
	for id, r := range q.poolAvailability {
		m := q.s.Machines[id]
		if m == nil || !r.at.After(q.poolLastStop[hostID(m)]) {
			continue
		}
		if decision := q.memoryPoolShort(m, r); decision != nil {
			host := hostID(m)
			if prior := short[host]; prior == nil || decision.ObservedAt.After(prior.ObservedAt) {
				short[host] = decision
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
		q.recordMemoryPressureStop(victim, host, decision)
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
	for i := range victim.Copies {
		c := &victim.Copies[i]
		if c.Attempt == victim.Attempt && c.Machine == victim.Machine && c.Session == victim.Session {
			c.StopCause = memoryPressureCause
			c.PriorityStop = &PriorityPreemption{Cause: memoryPressureCause, Demand: demand, Victim: victim.ID, Policy: q.priorityMatch(victim.Spec), PoolPressure: decision}
			c.Recoverable = false
		}
	}
	q.stopFor(victim, "memory pool", "memory")
	victim.Reason = memoryPressureCause
}
