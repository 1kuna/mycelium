package myce

const sharedGPUPressureCause = "shared_gpu_pressure"
const sharedGPUUnknown = "shared GPU growth guard: per-process device sample unavailable or stale"

// Check-in already collected the samples outside q.mu. Use only exact-attempt,
// fresh per-process measurements here; host-wide deltas cannot identify a tenant.
func (q *Queue) guardSharedGPU(m *Machine) {
	if m.Gone || m.Leaving || q.unreported[m.ID] || m.Replacement != nil {
		return
	}
	for _, d := range m.Devices {
		card := d.machine(*m)
		var tenants []*Job
		settled := true
		for _, j := range q.liveJobs() {
			if batchOnTarget(j, &card) && batchReservesCapacity(j) {
				tenants = append(tenants, j)
				settled = settled && j.State == "running" && j.StartedAt != nil && j.Session == m.Session && len(j.AllocatedDevices) == 1 && !cancelled(j)
			}
		}
		if len(tenants) < 2 || !settled || d.BudgetGB <= 0 {
			continue // Own limits handle single tenants; pending stops retain custody.
		}
		used, over := 0.0, map[string]float64{}
		growth := false
		measured := true
		var victim *Job
		for _, j := range tenants {
			s, now := q.hostMemorySamples[m.ID][jobKey(*j)], q.clock.Now()
			fresh := !s.deviceAt.IsZero() && !now.Before(s.deviceAt) && now.Sub(s.deviceAt) < deviceMemorySampleFreshFor
			measured = measured && fresh
			peak := 0.0 // Stale or missing device samples contribute no growth.
			if fresh {
				peak = s.deviceGB
				if j.Reason == sharedGPUUnknown {
					j.Reason = ""
				}
			} else {
				j.Reason = sharedGPUUnknown
			}
			reservation := deviceShareGB(j)
			used += max(reservation, peak)
			over[j.ID] = peak - reservation
			growth = growth || over[j.ID] > 0
			if victim == nil || q.comparePriority(j.Spec, victim.Spec) > 0 || q.comparePriority(j.Spec, victim.Spec) == 0 && (over[j.ID] > over[victim.ID] || over[j.ID] == over[victim.ID] && j.ID < victim.ID) {
				victim = j
			}
		}
		for _, slot := range q.s.ModelSlots {
			if slotOnTarget(slot, &card) && slotReservesMemory(slot) {
				used += q.slotMemory(slot)
			}
		}
		if !measured || !growth || used <= d.BudgetGB {
			continue
		}
		// Retain each grower's lower bound even if a lower tier yields for it.
		// Measured growth covers the sensor horizon. Never resize active custody.
		for _, j := range tenants {
			if over[j.ID] > 0 || j.ID == victim.ID {
				sample := q.hostMemorySamples[m.ID][jobKey(*j)]
				j.DeviceMemoryFloorGB = max(j.DeviceMemoryFloorGB, sample.deviceGB+sample.deviceGrowthGB)
			}
		}
		for i := range victim.Copies {
			c := &victim.Copies[i]
			if c.Attempt == victim.Attempt && c.Machine == victim.Machine && c.Session == victim.Session {
				c.StopCause = sharedGPUPressureCause
				c.PriorityStop = &PriorityPreemption{Cause: sharedGPUPressureCause, Demand: "device/" + d.ID, Victim: victim.ID, Policy: q.priorityMatch(victim.Spec)}
				c.Recoverable = false
			}
		}
		q.stopFor(victim, "GPU", "memory pressure")
		victim.Reason = "shared GPU memory growth exceeds device budget"
	}
}
