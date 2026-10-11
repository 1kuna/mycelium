package myce

import "sort"

func hostID(m *Machine) string {
	if m.HostID != "" {
		return m.HostID
	}
	return m.ID
}

// CPU allocations are additional to GPU allocations on unified machines.
// Legacy workers/requests keep the GPU-only contract.
func hostNeed(m *Machine, gpu, host float64) float64 {
	if m.UnifiedMemory && m.HostMemoryMeasured {
		return gpu + host
	}
	return host
}
func hostFits(m *Machine, gpu, host float64) bool {
	if host == 0 {
		return true
	}
	return m.HostMemoryMeasured && m.HostMemoryError == "" && m.FreeHostMemoryGB >= hostNeed(m, gpu, host)
}
func (q *Queue) refreshHostMemory() {
	for _, m := range q.s.Machines {
		available := m.HostAvailableMemoryGB
		for _, other := range q.s.Machines {
			if hostID(other) == hostID(m) && !other.Gone {
				if !other.HostMemoryMeasured || other.HostMemoryError != "" {
					available = 0
				} else {
					available = min(available, other.HostAvailableMemoryGB)
				}
			}
		}
		reserved := 0.0
		for _, j := range q.s.Jobs {
			owner := q.s.Machines[j.Machine]
			if owner != nil && hostID(owner) == hostID(m) && (j.State == "running" || j.State == "stopping") {
				reserved += hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
			}
		}
		for _, s := range q.s.ModelSlots {
			owner := q.s.Machines[s.Machine]
			if owner != nil && hostID(owner) == hostID(m) {
				model, _ := q.model(s.Model)
				reserved += hostNeed(owner, model.MemoryGB, model.HostMemoryGB)
			}
		}
		m.FreeHostMemoryGB = max(0, available-reserved)
	}
}

// Plan releases first. Never credit stopping/draining work as free until its
// acknowledgement arrives. GPU releases are local; RAM releases may be on any
// card on the same physical host. Pins and failed unloads remain protected.
func (q *Queue) hostReleases(m *Machine, gpu, host float64, priority string) ([]*Job, []*ModelSlot, bool) {
	if host == 0 || hostFits(m, gpu, host) {
		return nil, nil, true
	}
	if !m.HostMemoryMeasured || m.HostMemoryError != "" {
		return nil, nil, false
	}
	room := m.FreeHostMemoryGB
	need := hostNeed(m, gpu, host)
	var jobs []*Job
	var slots []*ModelSlot
	for _, j := range q.s.Jobs {
		owner := q.s.Machines[j.Machine]
		if owner == nil || owner.Gone || hostID(owner) != hostID(m) || rank(j.Spec.Priority) <= rank(priority) || (!j.Spec.Resumable && priority != "production") || owner.CheckpointUnavailable || !q.checkpointsAvailable() {
			continue
		}
		if j.State == "running" || j.State == "stopping" {
			jobs = append(jobs, j)
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if rank(jobs[i].Spec.Priority) != rank(jobs[j].Spec.Priority) {
			return rank(jobs[i].Spec.Priority) > rank(jobs[j].Spec.Priority)
		}
		return jobs[i].ID < jobs[j].ID
	})
	for _, s := range q.s.ModelSlots {
		owner := q.s.Machines[s.Machine]
		if owner != nil && !owner.Gone && hostID(owner) == hostID(m) && !q.slotPinned(s) && s.State != "unload_failed" && s.State != "failed" && rank(s.Priority) > rank(priority) {
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].InFlight != slots[j].InFlight {
			return slots[i].InFlight < slots[j].InFlight
		}
		return slotKey(slots[i].Machine, slots[i].Model) < slotKey(slots[j].Machine, slots[j].Model)
	})
	// Prefer releasing model residency before interrupting commands.
	chosenSlots := []*ModelSlot{}
	for _, s := range slots {
		if room >= need {
			break
		}
		model, _ := q.model(s.Model)
		n := hostNeed(q.s.Machines[s.Machine], model.MemoryGB, model.HostMemoryGB)
		if n > 0 {
			room += n
			chosenSlots = append(chosenSlots, s)
		}
	}
	chosenJobs := []*Job{}
	for _, j := range jobs {
		if room >= need {
			break
		}
		n := hostNeed(q.s.Machines[j.Machine], j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
		if n > 0 {
			room += n
			chosenJobs = append(chosenJobs, j)
		}
	}
	return chosenJobs, chosenSlots, room >= need
}
func (q *Queue) reclaimHost(m *Machine, gpu, host float64, priority, kind string) bool {
	jobs, slots, ok := q.hostReleases(m, gpu, host, priority)
	if !ok {
		return false
	}
	for _, s := range slots {
		s.State = "draining"
	}
	for _, j := range jobs {
		q.stopFor(j, priority, kind)
	}
	return true
}

func (q *Queue) reclaimableGPUMemory(m *Machine, priority string) float64 {
	room := m.FreeMemoryGB
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && !q.slotPinned(s) && s.State != "unload_failed" && s.State != "failed" && rank(s.Priority) > rank(priority) {
			model, _ := q.model(s.Model)
			room += model.MemoryGB
		}
	}
	for _, j := range q.s.Jobs {
		if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") && rank(j.Spec.Priority) > rank(priority) && (j.Spec.Resumable || priority == "production") && !m.CheckpointUnavailable && q.checkpointsAvailable() {
			room += j.Spec.Needs.MemoryGB
		}
	}
	return room
}
