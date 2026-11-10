package myce

// A transaction-local worklist propagates decisions through batch relations.
// The version contains only relation inputs, never a serialized job comparison.
// Ordinary progress/diagnostic touches do not repeat relation work.
type maintenanceVersion struct {
	state                                              string
	attempt                                            int
	cancelled, released, filesExpired, dependencyReady bool
	pauseRevision                                      uint64
	next, children, results                            int
}

func relationVersion(j *Job) maintenanceVersion {
	v := maintenanceVersion{state: j.State, attempt: j.Attempt, cancelled: cancelled(j), released: j.ReleaseConfirmed, filesExpired: j.FilesExpired, dependencyReady: dependencyReady(j), pauseRevision: j.PauseRevision}
	if j.FanOut != nil {
		v.next, v.children, v.results = j.FanOut.Next, len(j.FanOut.Children), len(j.FanOut.Results)
	}
	return v
}

type queueMaintenance struct {
	pending    []string
	queued     map[string]bool
	forced     map[string]bool
	seen       map[string]maintenanceVersion
	dependents map[string]map[string]bool
	draining   bool
	ran        bool
}

func (q *Queue) beginMaintenance() {
	q.maintenance = &queueMaintenance{ran: true, queued: map[string]bool{}, forced: map[string]bool{}, seen: map[string]maintenanceVersion{}, dependents: map[string]map[string]bool{}}
	for _, j := range q.liveJobs() {
		q.maintenanceJob(j)
	}
}
func (q *Queue) maintenanceJob(j *Job) {
	if q.maintenance == nil || !q.maintenance.ran || j == nil {
		return
	}
	if a := j.Spec.After; a != nil {
		if q.maintenance.dependents[a.Job] == nil {
			q.maintenance.dependents[a.Job] = map[string]bool{}
		}
		q.maintenance.dependents[a.Job][j.ID] = true
	}
	q.enqueueMaintenance(j.ID, false)
}
func (q *Queue) enqueueMaintenance(id string, force bool) {
	m := q.maintenance
	if m == nil || !m.ran || id == "" {
		return
	}
	if force {
		m.forced[id] = true
	}
	if !m.queued[id] {
		m.queued[id] = true
		m.pending = append(m.pending, id)
	}
}
func (q *Queue) settleMaintenance() {
	m := q.maintenance
	if m == nil || !m.ran || m.draining {
		return
	}
	m.draining = true
	defer func() { m.draining = false }()
	for len(m.pending) > 0 {
		id := m.pending[0]
		m.pending = m.pending[1:]
		delete(m.queued, id)
		force := m.forced[id]
		delete(m.forced, id)
		j := q.s.Jobs[id]
		if j == nil {
			continue
		}
		before := relationVersion(j)
		last, seen := m.seen[id]
		if seen && before == last && !force {
			continue
		}
		q.maintainJobPause(j)
		q.maintainFanOutJob(j)
		q.maintainPieceDependency(j)
		after := relationVersion(j)
		m.seen[id] = after
		if before != after {
			// A newly terminal parent must run its terminal branch too, so
			// sibling cancellation belongs to this same atomic decision.
			q.enqueueMaintenance(id, true)
		}
		if before == after && (!seen || before == last) {
			continue
		}
		if j.Batch != nil {
			q.enqueueMaintenance(j.Batch.Parent, true)
		}
		if j.FanOut != nil {
			for _, child := range j.FanOut.Children {
				q.enqueueMaintenance(child, true)
			}
		}
		for dependent := range m.dependents[id] {
			q.enqueueMaintenance(dependent, true)
		}
	}
}

func (q *Queue) maintenanceRemoved(j *Job) {
	if q.maintenance == nil || !q.maintenance.ran || j == nil {
		return
	}
	if j.Batch != nil {
		q.enqueueMaintenance(j.Batch.Parent, true)
	}
	for dependent := range q.maintenance.dependents[j.ID] {
		q.enqueueMaintenance(dependent, true)
	}
}
