package myce

import "time"

const priorityPreemptionCause = "priority_preemption"

// PriorityPreemption is planned intent, not proof of delivery or release. D5's
// member facts still decide whether the command completed or was interrupted.
type PriorityPreemption struct {
	Cause  string        `json:"cause"`
	Demand string        `json:"demand"`
	Victim string        `json:"victim"`
	Policy PriorityMatch `json:"policy"`
}

func (q *Queue) preemptionGrace() time.Duration {
	if q.productionGrace > 0 {
		return q.productionGrace
	}
	return 10 * time.Second
}
func priorityStop(j *Job) *PriorityPreemption {
	for _, c := range j.Copies {
		if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session {
			return c.PriorityStop
		}
	}
	return nil
}
func plannedPriorityStop(j *Job) bool { return priorityStop(j) != nil }
func plannedStop(j *Job) bool         { return plannedMaintenanceStop(j) || plannedPriorityStop(j) }

// After two displacements a resumed payload gets one full checkpoint grace to
// make progress. Strict tiers still win after that bounded interval. Unstarted
// offers are always revocable; the protection begins only at Started.
func (q *Queue) batchPreemptible(j *Job, demand Spec) bool {
	if j.State != "running" && j.State != "stopping" || q.comparePriority(j.Spec, demand) <= 0 {
		return false
	}
	if j.State == "stopping" && expired(j.StopAt, q.clock.Now()) {
		return false
	}
	return j.State == "stopping" || !q.preemptionProtected(j.PreemptionCount, j.StartedAt)
}
func (q *Queue) preemptionProtected(count int, since *time.Time) bool {
	return count >= 2 && since != nil && q.clock.Now().Before(since.Add(q.preemptionGrace()))
}
func (q *Queue) preemption(demand *Job, victim string) *PriorityPreemption {
	return &PriorityPreemption{Cause: priorityPreemptionCause, Demand: demand.ID, Victim: victim, Policy: q.priorityMatch(demand.Spec)}
}
func (q *Queue) stopBatchFor(j *Job, demand *Job) {
	if !q.batchPreemptible(j, demand.Spec) {
		return
	}
	if cancelled(j) {
		q.stopFor(j, demand.Spec.Priority, "job")
		return
	}
	if j.State == "running" {
		for i := range j.Copies {
			c := &j.Copies[i]
			if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session {
				c.StopCause = priorityPreemptionCause
				c.PriorityStop = q.preemption(demand, j.ID)
				c.Recoverable = false
			}
		}
		j.PreemptionCount++
	}
	q.stopFor(j, demand.Spec.Priority, "job")
}

// The same enumeration as host maintenance supplies every stop adapter. Native
// sidecars belong to the batch obligation and stop under its exact-attempt gate.
func (q *Queue) preemptObligation(p HostObligation, demand *Job) {
	switch {
	case p.Call != nil:
		c := q.obligationCall(p.Call)
		if c == nil || q.comparePriority(callJob(c).Spec, demand.Spec) <= 0 {
			return
		}
		c.PriorityStop = q.preemption(demand, c.ID)
		now := q.clock.Now()
		boundStop(&c.StopAt, now)
		c.Error = priorityPreemptionCause
	case p.Engine != nil:
		e := p.Engine
		s := q.s.ModelSlots[slotKey(e.Machine, e.Model)]
		if s == nil || s.Session != e.Session || s.Attempt != e.Attempt || !q.slotReclaimableFor(s, demand.Spec) {
			return
		}
		if s.State == "unloading" {
			return
		} // Never rewrite retained cleanup.
		// Idle same-tier cache eviction is not priority preemption.
		if q.comparePriority(q.slotSpec(s), demand.Spec) > 0 {
			q.recordEnginePreemption(s, demand)
			now := q.clock.Now()
			boundStop(&s.StopAt, now)
			for _, other := range q.hostObligations(q.executionHost(e.Machine)) {
				if other.Call != nil && other.Call.Machine == e.Machine && other.Call.Model == e.Model && other.Call.Attempt == e.Attempt {
					q.preemptObligation(other, demand)
				}
			}
		}
		loading := s.State == "loading"
		s.State = "draining"
		if loading && expired(s.StopAt, q.clock.Now()) {
			s.State = "unloading"
		}
	case p.Launch != nil:
		l, ok := q.s.PreparationPlans[p.Launch.Attempt]
		if !ok || l.Spec == nil || q.comparePriority(*l.Spec, demand.Spec) <= 0 {
			return
		}
		// An unstarted offer has no process custody. Revoke its exact launch
		// rather than inventing a cleanup wait that can block a free worker.
		if !l.Started {
			delete(q.s.PreparationPlans, l.Attempt)
			return
		}
		if j := q.s.Jobs[l.Demand]; j != nil && l.PriorityStop == nil {
			if q.preemptionProtected(j.PreemptionCount, l.StartedAt) {
				return
			}
			j.PreemptionCount++
		}
		l.PriorityStop = q.preemption(demand, "preparation/"+l.Attempt)
		now := q.clock.Now()
		boundStop(&l.StopAt, now)
		q.s.PreparationPlans[l.Attempt] = l
	default:
		r := p.Result
		if j := q.s.Jobs[r.ID]; j != nil && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
			q.stopBatchFor(j, demand)
		}
	}
}
func (q *Queue) recordEnginePreemption(s *ModelSlot, demand *Job) {
	if s.PriorityStop == nil {
		for i := range q.s.Pins {
			p := &q.s.Pins[i]
			if p.Model == s.Model && p.Assigned == s.Machine {
				p.PreemptionCount++
			}
		}
		for _, c := range q.calls {
			if c.Model == s.Model && c.Machine == "" && c.Error == "" {
				c.PreemptionCount++
			}
		}
	}
	s.PriorityStop = q.preemption(demand, obligationKey(HostObligation{Engine: &HostEngineExecution{Machine: s.Machine, Session: s.Session, Model: s.Model, Attempt: s.Attempt}}))
}
func (q *Queue) preemptSlot(s *ModelSlot, demand *Job) {
	q.preemptObligation(HostObligation{Engine: &HostEngineExecution{Machine: s.Machine, Session: s.Session, Model: s.Model, Attempt: s.Attempt}}, demand)
}
func (q *Queue) preemptPreparations(m *Machine, demand *Job) {
	for _, p := range q.hostObligations(hostID(m)) {
		if p.Launch != nil && p.Launch.Machine == m.ID && p.Launch.Session == m.Session {
			l := *p.Launch
			// Preparation shared with the winner inherits its tier. Stopping a
			// build that the replacement also needs cannot release useful capacity.
			if q.preparationShared(m, demand, &l) && l.PriorityStop == nil {
				if l.Spec == nil || q.comparePriority(demand.Spec, *l.Spec) < 0 {
					spec := Spec{Project: demand.Spec.Project, Class: demand.Spec.Class, Priority: demand.Spec.Priority}
					l.Spec, l.Demand = &spec, demand.ID
					q.s.PreparationPlans[l.Attempt] = l
				}
				continue
			}
			q.preemptObligation(p, demand)
		}
	}
}
func (q *Queue) preparationStopping(m *Machine) bool {
	for _, l := range q.s.PreparationPlans {
		if l.Machine == m.ID && l.Session == m.Session && l.PriorityStop != nil {
			return true
		}
	}
	return false
}
func (q *Queue) preparationActive(m *Machine) bool {
	for _, l := range q.s.PreparationPlans {
		if l.Kind == "environment" && l.Machine == m.ID && l.Session == m.Session && l.Started {
			return true
		}
	}
	return false
}

// History belongs to desired pins or a still-waiting load request, so cleanup
// cannot erase the runtime floor when a fresh engine attempt is made.
func (q *Queue) initEnginePreemption(s *ModelSlot) {
	now := q.clock.Now()
	s.RunSince = &now
	for _, p := range q.s.Pins {
		if p.Model == s.Model && p.Project == s.Project {
			s.PreemptionCount = max(s.PreemptionCount, p.PreemptionCount)
		}
	}
	for _, c := range q.calls {
		if c.Model == s.Model && c.Project == s.Project && c.Machine == "" {
			s.PreemptionCount = max(s.PreemptionCount, c.PreemptionCount)
		}
	}
}

func (q *Queue) hostPreemptionDemand(host string) *Job {
	spec := q.hostPrioritySpec(host)
	for _, j := range q.s.Jobs {
		if j.ProductionHost == host && !terminal(j.State) && !cancelled(j) && q.comparePriority(j.Spec, spec) == 0 {
			return j
		}
	}
	for _, c := range q.calls {
		if c.ProductionHost == host && c.Error == "" && q.comparePriority(callJob(c).Spec, spec) == 0 {
			return callJob(c)
		}
	}
	return &Job{ID: "host/" + host, Spec: spec}
}

func (q *Queue) preparationShared(m *Machine, demand *Job, l *HostLaunch) bool {
	key := environmentKey(demand, m)
	if model, ok := q.model(demand.Spec.Needs.Model); ok && model.Environment != "" {
		key = q.environments.Environments[model.Environment][m.GPU].Key
	}
	if e := qualifiedExtension(demand, m); e != nil {
		key = extensionPreparationKey(*e, m)
	}
	return l.Kind == "environment" && l.Key == key
}
