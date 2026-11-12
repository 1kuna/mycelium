package myce

import (
	"slices"
	"time"
)

// Work that Mycelium itself stopped (planned host or device maintenance, a
// rollout drain or a critical update) was already running ahead of everything
// still queued. It keeps that place within its tier: it ranks ahead of the
// tier's other queued work, ahead of the fair turn too, and resumes among
// themselves age from first submission. Higher tiers still outrank it.
func maintenanceResume(j *Job) bool {
	return j.MaintenanceResume != nil && j.State == "queued"
}

// maintenanceResumeOrder reports whether a goes before b, and whether that
// rule decides the pair at all.
func maintenanceResumeOrder(a, b *Job) (before, decided bool) {
	ra, rb := maintenanceResume(a), maintenanceResume(b)
	if ra != rb {
		return ra, true
	}
	if ra && !a.Queued.Equal(b.Queued) {
		return a.Queued.Before(b.Queued), true
	}
	return false, false
}

// A maintenance resume's target drains toward it. Its fence projects the
// release of every finite batch attempt on the host, accelerator work
// included, not only CPU work: running attempts finish under their existing
// contract and nothing is stopped. drainStart is when enough of them are
// expected to finish (wall_time bound, else the last accepted duration of
// the same work), or nil when one it needs has neither.
func (q *Queue) drainProjection(m *Machine, release []*Job) Machine {
	projected := q.afterReleases(m, release, nil)
	projected.FreeHostMemoryGB = min(q.qualificationCapacity(m).FreeHostMemoryGB, projected.FreeHostMemoryGB)
	for _, j := range release {
		if !j.Spec.Needs.cpuOnly() && batchOnTarget(j, m) {
			projected.FreeMemoryGB += j.Spec.Needs.MemoryGB
		}
	}
	return projected
}

func (q *Queue) drainingAttempts(m *Machine) []*Job {
	jobs := []*Job{}
	for _, running := range q.liveJobs() {
		owner := q.s.Machines[running.Machine]
		if owner != nil && !owner.Gone && hostID(owner) == hostID(m) && backfillFiniteExecution(running) && batchReservesCapacity(running) {
			jobs = append(jobs, running)
		}
	}
	return jobs
}

func (q *Queue) expectedEnd(j *Job, m *Machine) *time.Time {
	start := q.clock.Now()
	if j.StartedAt != nil {
		start = *j.StartedAt
	}
	if d := j.Spec.wallTimeDuration(); d > 0 {
		at := start.Add(d)
		return &at
	}
	if e, ok := q.estimateCompletion(j, m); ok {
		at := start.Add(time.Duration(e.seconds * float64(time.Second)))
		return &at
	}
	return nil
}

// drainStart releases the host's attempts in expected-end order until the
// demand fits, and returns that time. Unknown ends sort last.
func (q *Queue) drainStart(demand *Job, m *Machine, running []*Job) *time.Time {
	now := q.clock.Now()
	if fits(demand, m, m.FreeMemoryGB) && q.batchSlotFree(m, demand) {
		return &now
	}
	type end struct {
		j  *Job
		at *time.Time
	}
	ends := make([]end, 0, len(running))
	for _, j := range running {
		ends = append(ends, end{j, q.expectedEnd(j, q.s.Machines[j.Machine])})
	}
	slices.SortStableFunc(ends, func(a, b end) int {
		switch {
		case a.at == nil && b.at == nil:
			return 0
		case a.at == nil:
			return 1
		case b.at == nil:
			return -1
		}
		return a.at.Compare(*b.at)
	})
	released := []*Job{}
	for _, e := range ends {
		if e.at == nil {
			return nil
		}
		released = append(released, e.j)
		if projected := q.drainProjection(m, released); fits(demand, &projected, projected.FreeMemoryGB) {
			at := *e.at
			if at.Before(now) {
				at = now
			}
			return &at
		}
	}
	return nil
}

// Backfill behind a maintenance resume is allowed when the work is expected
// to finish before the resume could start anyway.
func (q *Queue) finishesBeforeDrain(j *Job, m *Machine, start *time.Time) bool {
	if start == nil {
		return false
	}
	end := q.expectedEnd(j, m)
	return end != nil && !end.After(*start)
}
