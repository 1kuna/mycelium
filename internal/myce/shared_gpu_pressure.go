package myce

import "time"

const sharedGPUPressureCause = "shared_gpu_pressure"

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

// The discrete pool uses current driver availability and per-attempt
// footprints. Unified allocations are handled by the host pool.
func (q *Queue) guardSharedGPU(m *Machine) {
	if m.Gone || m.Leaving || q.unreported[m.ID] || m.Replacement != nil {
		return
	}
	for _, d := range m.Devices {
		if d.UnifiedMemory || d.BudgetGB <= 0 {
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
