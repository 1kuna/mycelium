package myce

const sharedGPUPressureCause = "shared_gpu_pressure"

// The discrete pool uses current driver availability and per-attempt
// footprints. Unified allocations are handled by the host pool.
func (q *Queue) guardSharedGPU(m *Machine) {
	if m.Gone || m.Leaving || q.unreported[m.ID] || m.Replacement != nil {
		return
	}
	for _, d := range m.Devices {
		if d.UnifiedMemory || d.BudgetGB <= 0 || !d.MemoryMeasured {
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
		if victim == nil || pending || used <= d.BudgetGB && (d.FreeMemoryGB > 0 || overage <= 0) {
			continue
		}
		q.stopForPoolPressure(victim, "device/"+d.ID)
	}
}
