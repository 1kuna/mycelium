package myce

import "sort"

// A placement is a read-only release projection. Only the cheapest compatible
// placement is applied, after all workers have been considered. Pending releases
// are useful capacity for planning, never authority to admit a replacement.
type placementPlan struct {
	machine          *Machine
	jobs             []*Job
	slots            []*ModelSlot
	calls            []HostObligation
	stops            int
	cost             int
	preparations     bool
	preparationCores int
}

func (p *placementPlan) cheaper(other *placementPlan) bool {
	return other == nil || p.stops < other.stops || p.stops == other.stops && p.cost < other.cost
}
func (p *placementPlan) addJob(j *Job) {
	for _, old := range p.jobs {
		if old == j {
			return
		}
	}
	p.jobs = append(p.jobs, j)
	if j.State == "running" {
		p.stops++
		if !j.Spec.Resumable {
			p.cost++
		}
	}
}
func (p *placementPlan) addSlot(s *ModelSlot) {
	for _, old := range p.slots {
		if old == s {
			return
		}
	}
	p.slots = append(p.slots, s)
	if s.State != "draining" && s.State != "unloading" {
		p.stops++
		if s.InFlight > 0 {
			p.cost++
		}
	}
}
func (q *Queue) applyPlacement(p *placementPlan, demand *Job) {
	for _, j := range p.jobs {
		q.stopBatchFor(j, demand)
	}
	for _, s := range p.slots {
		q.preemptSlot(s, demand)
	}
	for _, c := range p.calls {
		q.preemptObligation(c, demand)
	}
	if p.preparations {
		q.preemptPreparations(p.machine, demand)
	}
}

// Calls reclaim only the required request slots, preserving the ready engine
// and pin intent. The slot remains occupied until finishCall acknowledges it.
func (q *Queue) callSlotPlan(m *Machine, c *ModelCall, s *ModelSlot, limit int) *placementPlan {
	p := &placementPlan{machine: m}
	if limit == 0 || s.InFlight < limit {
		return p
	}
	var victims []HostObligation
	for _, o := range q.hostObligations(hostID(m)) {
		if o.Call == nil {
			continue
		}
		v := q.obligationCall(o.Call)
		if v != nil && v.Machine == m.ID && v.Session == s.Session && v.Attempt == s.Attempt && v.Model == s.Model && q.comparePriority(callJob(v).Spec, callJob(c).Spec) > 0 {
			victims = append(victims, o)
		}
	}
	sort.Slice(victims, func(i, j int) bool {
		a, b := q.obligationCall(victims[i].Call), q.obligationCall(victims[j].Call)
		if (a.PriorityStop != nil) != (b.PriorityStop != nil) {
			return a.PriorityStop != nil
		}
		if n := q.comparePriority(callJob(a).Spec, callJob(b).Spec); n != 0 {
			return n > 0
		}
		return a.ID < b.ID
	})
	need := s.InFlight - limit + 1
	if len(victims) < need {
		return nil
	}
	for _, o := range victims[:need] {
		p.calls = append(p.calls, o)
		if q.obligationCall(o.Call).PriorityStop == nil {
			p.stops++
			p.cost++
		}
	}
	return p
}

// roomPlan is shared by batches (including native groups and environment
// demands), model loads and pins. A batch needs the worker's command slot;
// engines can coexist with a batch when the resource vector has room.
func (q *Queue) roomPlan(j *Job, m *Machine, batch bool) *placementPlan {
	if m.Recovering {
		return nil
	}
	best := q.roomPlanWithBatch(j, m, batch, batch)
	if !batch {
		if p := q.roomPlanWithBatch(j, m, false, true); p != nil && p.cheaper(best) {
			best = p
		}
	}
	return best
}
func (q *Queue) roomPlanWithBatch(j *Job, m *Machine, batch, releaseBatch bool) *placementPlan {
	p := &placementPlan{machine: m, preparations: true}
	need, host := j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB
	if !batch {
		model, ok := q.model(j.Spec.Needs.Model)
		if !ok || q.modelLoadDiskHeadroomReason(model, m) != "" {
			return nil
		}
		need, host = model.MemoryGB, model.HostMemoryGB
	}
	// Only preparation launches participate here. Enumerating every retained
	// execution copies the entire backlog for each candidate's resource check.
	for _, launch := range q.s.PreparationPlans {
		l := &launch
		if q.executionHost(l.Machine) != hostID(m) || l.Kind != "environment" || l.Machine != m.ID || l.Session != m.Session || !l.Started {
			continue
		}
		if q.preparationShared(m, j, l) {
			// The winning execution covers this machine's preparation core.
			// Hand it back in the projection without stopping the shared build.
			p.preparationCores = 1
			continue
		}
		p.preparationCores = 1
		if l.PriorityStop == nil {
			if l.Spec == nil || q.comparePriority(*l.Spec, j.Spec) <= 0 {
				return nil
			}
			if owner := q.s.Jobs[l.Demand]; owner != nil && q.preemptionProtected(owner.PreemptionCount, l.StartedAt) {
				return nil
			}
			p.stops++
			p.cost++
		}
	}
	room := m.FreeMemoryGB - m.modelReservationDebtGB
	if r, ok := q.s.BatchReleases[batchReleaseKey(m)]; ok && r.Session == m.Session && q.clock.Now().Before(r.Until) {
		room += r.MemoryGB
	}
	for _, r := range q.liveJobs() {
		if !batchOnTarget(r, m) || r.ReleaseConfirmed || (r.State != "running" && r.State != "stopping") {
			continue
		}
		if !q.batchPreemptible(r, j.Spec) {
			if batch {
				return nil
			}
			continue
		}
		if releaseBatch {
			p.addJob(r)
			room += r.Spec.Needs.MemoryGB
		}
	}
	var slots []*ModelSlot
	for _, s := range q.s.ModelSlots {
		if !slotOnTarget(s, m) || !q.slotReclaimableFor(s, j.Spec) {
			continue
		}
		if s.State == "draining" {
			room += q.slotMemory(s)
		} else {
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(a, b int) bool {
		if q.slotMemory(slots[a]) != q.slotMemory(slots[b]) {
			return q.slotMemory(slots[a]) > q.slotMemory(slots[b])
		}
		return slots[a].Model < slots[b].Model
	})
	possible, count := room, 0
	for _, s := range slots {
		if possible >= need {
			break
		}
		possible += q.slotMemory(s)
		count++
	}

	if possible < need {
		return nil
	}
	for count > 0 && room < need {
		chosen := 0
		for i, s := range slots {
			if s.InFlight != 0 {
				continue
			}
			capacity, n := room+q.slotMemory(s), 0
			for k, other := range slots {
				if k != i && n < count-1 {
					capacity += q.slotMemory(other)
					n++
				}
			}
			if capacity >= need {
				chosen = i
				break
			}
		}
		s := slots[chosen]
		room += q.slotMemory(s)
		p.addSlot(s)
		slots = append(slots[:chosen], slots[chosen+1:]...)
		count--
	}
	// Mandatory/local releases already provide RAM as well as GPU memory.
	// Project both once before asking for additional host-wide releases.
	projected := q.cpuAfterReleases(m, p.jobs, p.slots, p.preparationCores)
	projected.FreeHostMemoryGB -= m.hostReservationDebtGB
	projected.hostReservationDebtGB = 0 // Carry signed room through local releases once.
	releasedJobs, releasedSlots := map[*Job]bool{}, map[*ModelSlot]bool{}
	for _, r := range p.jobs {
		releasedJobs[r] = true
		projected.FreeHostMemoryGB += hostNeed(allocationMachine(q.s.Machines[r.Machine], r.AllocatedDevices), r.Spec.Needs.MemoryGB, r.Spec.Needs.HostMemoryGB)
	}
	for _, s := range p.slots {
		releasedSlots[s] = true
		model, _ := q.model(s.Model)
		projected.FreeHostMemoryGB += hostNeed(slotMachine(q.s.Machines[s.Machine], s), model.MemoryGB, model.HostMemoryGB)
	}
	hostJobs, hostSlots, ok := q.hostReleasesExcept(&projected, need, host, j.Spec, releasedJobs, releasedSlots, p.preparationCores > 0)
	if !ok {
		return nil
	}
	for _, r := range hostJobs {
		p.addJob(r)
	}
	for _, s := range hostSlots {
		p.addSlot(s)
	}
	return p
}
func (q *Queue) slotMemory(s *ModelSlot) float64 { model, _ := q.model(s.Model); return model.MemoryGB }
