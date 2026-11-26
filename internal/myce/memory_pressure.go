package myce

import (
	"fmt"
	"time"
)

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

// MemoryPoolStop is the queue's decision-time comparison. The completion
// observation can show recovery after the victim released RAM.
type MemoryPoolStop struct {
	ObservedAt          time.Time `json:"observed_at"`
	AvailableGB         float64   `json:"available_gb"`
	ReserveGB           float64   `json:"reserve_gb"`
	GrowthRate          float64   `json:"growth_gb_per_second"`
	SustainedGrowthRate float64   `json:"sustained_growth_gb_per_second"`
	NextLossGB          float64   `json:"next_loss_gb"`
	GraceLossGB         float64   `json:"grace_loss_gb"`
	SampleSeconds       float64   `json:"sample_seconds"`
	GraceSeconds        float64   `json:"grace_seconds"`
	HostPressure        float64   `json:"host_pressure,omitempty"`
	Trigger             string    `json:"trigger"`
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

// A memory_pressure stop shows the job's charge did not protect the pool
// beside its peers. Later attempts run alone on the pool rather than restart
// at the same charge beside the same contention (#2091). A clean completion
// ends the job; its exact peak sizes later runs of the workload as usual.
func pressureStoppedBefore(j *Job, attempt int) bool {
	for _, h := range j.AttemptHistory {
		if h.Attempt < attempt && (h.PriorityStop != nil && h.PriorityStop.Cause == memoryPressureCause ||
			h.ExecutionCause != nil && h.ExecutionCause.Class == memoryPressureCause) {
			return true
		}
	}
	return false
}

// An attempt that already ran alone after a pressure stop met pressure with no
// Mycelium peer to blame. Running it alone again would meet the same pressure,
// so hold it for its owner with the measured peak and the stop comparison.
func (q *Queue) holdAlonePressure(j *Job) bool {
	if cancelled(j) || wallTimeStopped(j) || yieldMatches(j) || !pooledMemory(j) || !memoryPools(j).RAM.Alone || !pressureStoppedBefore(j, j.Attempt) {
		return false
	}
	reason := q.alonePressureReason(j)
	q.requeue(j, reason, false)
	if j.State == "queued" {
		q.pauseJob(j, JobPause{Reason: reason})
	}
	return true
}

func (q *Queue) alonePressureReason(j *Job) string {
	gb := func(v *float64) string {
		if v == nil {
			return "unmeasured"
		}
		return fmt.Sprintf("%.3f GiB", *v)
	}
	var peak AttemptMemoryPeak
	for _, f := range j.MemoryAttempts {
		if f.Attempt == j.Attempt && f.Machine == j.Machine && f.Session == j.Session && f.MemoryPeak != nil {
			peak = *f.MemoryPeak
		}
	}
	reason := fmt.Sprintf("memory_capacity: attempt %d was stopped for memory_pressure while running alone; measured peak host %s, cgroup %s, device %s; charge %.3f GiB",
		j.Attempt, gb(peak.HostGB), gb(peak.CgroupGB), gb(peak.DeviceGB), memoryPools(j).RAM.ChargeGB)
	if m := q.s.Machines[j.Machine]; m != nil {
		reason += fmt.Sprintf(" of a %.3f GiB pool allowance", q.poolAllowance(m))
	}
	if stop := priorityStop(j); stop != nil && stop.MemoryPool != nil {
		d := stop.MemoryPool
		reason += fmt.Sprintf("; queue trigger %s with %.3f GiB available, %.3f GiB reserve, %.3f GiB forecast loss", d.Trigger, d.AvailableGB, d.ReserveGB, max(d.NextLossGB, d.GraceLossGB))
	} else {
		reason += "; kernel memory kill outside the attempt's own limit"
	}
	return reason + "; resume runs it alone again"
}

func (q *Queue) stopForPoolPressure(victim *Job, stop PriorityPreemption) {
	q.touchJob(victim.ID)
	stop.Cause = memoryPressureCause
	stop.Victim = victim.ID
	stop.Policy = q.priorityMatch(victim.Spec)
	for i := range victim.Copies {
		c := &victim.Copies[i]
		if c.Attempt == victim.Attempt && c.Machine == victim.Machine && c.Session == victim.Session {
			c.StopCause = memoryPressureCause
			c.PriorityStop = &stop
			c.Recoverable = false
		}
	}
	q.stopFor(victim, "memory pool", "memory")
	victim.Reason = memoryPressureCause
}
