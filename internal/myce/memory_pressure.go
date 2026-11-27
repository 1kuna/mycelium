package myce

import (
	"fmt"
	"time"
)

const memoryPressureCause = "memory_pressure"

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

// DevicePoolStop records the discrete pool comparison that triggered a stop.
type DevicePoolStop struct {
	ObservedAt     time.Time `json:"observed_at"`
	BudgetGB       float64   `json:"budget_gb"`
	UsedGB         float64   `json:"used_gb"`
	FreeGB         float64   `json:"free_gb"`
	MemoryMeasured bool      `json:"memory_measured"`
	OverageGB      float64   `json:"overage_gb"`
	Trigger        string    `json:"trigger"`
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

// R5 needs an attributed stop comparison. Keep exclusive-retry custody on
// pools whose stop or counting rule still awaits qualification.
func legacyPoolPressureRetry(j *Job, m *Machine) bool {
	for _, h := range j.AttemptHistory {
		stop := h.PriorityStop
		if stop != nil && stop.Cause == memoryPressureCause &&
			(stop.DevicePool != nil && !learnsExpectedCard(m) ||
				stop.MemoryPool != nil && !usesAttemptStopForecast(m, "ram")) {
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
