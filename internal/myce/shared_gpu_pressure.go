package myce

const sharedGPUPressureCause = "shared_gpu_pressure"
const sharedGPUUnknown = "shared GPU growth guard: per-process device sample unavailable or stale"

// Check-in already collected the samples outside q.mu. Use only exact-attempt,
// fresh counters or reconciled attempt estimates here; an ambiguous host
// delta forces exclusive execution rather than claiming a tenant grew.
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
		if unifiedCUDA(&card) {
			for _, j := range tenants {
				if j.State != "running" {
					continue
				}
				q.touchJob(j.ID)
				s := q.hostMemorySamples[m.ID][jobKey(*j)]
				if unifiedDeviceSampleStarting(j, s, q.clock.Now()) {
					j.Reason = deviceUnifiedStarting
				} else if !sharedDeviceSampleReady(s, q.clock.Now()) {
					j.Reason = deviceUnifiedUncertain
				} else if j.Reason == deviceUnifiedUncertain || j.Reason == deviceUnifiedStarting {
					j.Reason = ""
				}
			}
		}
		if len(tenants) < 2 || !settled || d.BudgetGB <= 0 {
			continue // Own limits handle single tenants; pending stops retain custody.
		}
		fanOut := false
		for _, j := range tenants {
			fanOut = fanOut || fanOutDeviceSharing(j)
		}
		used, over := 0.0, map[string]float64{}
		unobserved := map[string]bool{}
		growth := false
		measured := true
		uncertain, forecast := false, 0.0
		var victim *Job
		for _, j := range tenants {
			q.touchJob(j.ID)
			s, now := q.hostMemorySamples[m.ID][jobKey(*j)], q.clock.Now()
			fresh := !s.deviceAt.IsZero() && !now.Before(s.deviceAt) && now.Sub(s.deviceAt) < deviceMemorySampleFreshFor
			measured = measured && fresh
			unobserved[j.ID] = unifiedCUDA(&card) && (!fresh && !unifiedDeviceSampleStarting(j, s, now) || s.exclusive)
			uncertain = uncertain || unobserved[j.ID]
			peak := 0.0 // Stale or missing device samples contribute no growth.
			if fresh {
				peak = s.deviceGB
				if unifiedCUDA(&card) || fanOut {
					// Cover both the next observed sensor horizon and the existing
					// checkpoint/termination grace, using the measured growth rate.
					margin := s.deviceGrowthGB + s.deviceRateGB*q.gpuStopGrace().Seconds()
					forecast += margin
					peak += margin
				}
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
			prefer := victim == nil
			if victim != nil {
				tier := q.comparePriority(j.Spec, victim.Spec)
				switch {
				case tier != 0:
					prefer = tier > 0
				case unobserved[j.ID] != unobserved[victim.ID]:
					// Unknown evidence requires exclusivity, not evidence that a
					// measurable co-tenant caused pressure. Shed the unknown
					// tenant first within the lowest tier.
					prefer = unobserved[j.ID]
				default:
					prefer = over[j.ID] > over[victim.ID] || over[j.ID] == over[victim.ID] && j.ID < victim.ID
				}
			}
			if prefer {
				victim = j
			}
		}
		for _, slot := range q.s.ModelSlots {
			if slotOnTarget(slot, &card) && slotReservesMemory(slot) {
				used += q.slotMemory(slot)
			}
		}
		physicalPressure := false
		if unifiedCUDA(&card) && forecast > 0 && validHostMemoryObservation(m.MemoryObservation) {
			physicalPressure = m.MemoryObservation.TotalGB-m.MemoryObservation.UsedGB-q.hostMemoryMargin(m, m) <= forecast
		}
		if fanOut && backendFor(card.GPU).deviceHostMemory && !card.UnifiedMemory && forecast > 0 && deviceTelemetryReady(d.Telemetry, q.clock.Now()) {
			physicalPressure = *d.Telemetry.FreeGB <= forecast
		}
		if !uncertain && (!measured || !growth && !physicalPressure || used <= d.BudgetGB && !physicalPressure) {
			continue
		}
		// Retain each grower's lower bound even if a lower tier yields for it.
		// Measured growth covers the sensor horizon. Never resize active custody.
		for _, j := range tenants {
			if !uncertain && (over[j.ID] > 0 || j.ID == victim.ID) {
				sample := q.hostMemorySamples[m.ID][jobKey(*j)]
				q.touchJob(j.ID)
				j.DeviceMemoryFloorGB = max(j.DeviceMemoryFloorGB, sample.deviceGB+sample.deviceGrowthGB)
			}
		}
		q.touchJob(victim.ID)
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
		if physicalPressure {
			victim.Reason = "shared GPU growth consumes physical memory headroom"
		}
		if uncertain {
			victim.Reason = deviceUnifiedUncertain
		}
	}
}
