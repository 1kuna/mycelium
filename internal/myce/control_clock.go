package myce

import "time"

// Reconnect accounting for a legacy worker uses its existing default interval.
// Loss itself waits for an advertised or observed cadence. New workers advertise
// their actual interval in a header that older queues can safely ignore.
const checkinPeriodHeader = "X-Myce-Checkin-Period"

func checkinPeriod(d time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return time.Second
}

func (q *Queue) workerLossAllowance(m *Machine) time.Duration {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	c := q.contacts[m.ID]
	if c == nil || c.session != m.Session {
		return 0 // A legacy sender has not established a cadence yet.
	}
	if c.allowance > 0 { // Frozen detached observation.
		return c.allowance
	}
	// One expected arrival plus one missed-arrival confirmation. Variation is
	// learned with the shared RFC 6298 estimator, in control-opportunity time.
	period := c.period
	if period == 0 {
		period = time.Duration(c.cadence.Mean * float64(time.Second))
	}
	if period == 0 {
		return 0 // Unknown is not evidence of a lost worker.
	}
	allowance := max(2*period, time.Duration(max(c.gap.upper(), c.path.upper())*float64(time.Second)))
	return max(allowance, 2*c.retry.delay)
}

// Wall-time suspicion and observations need the actual delivered cadence, even
// when a current worker advertises a shorter send interval. Queue service is
// absent from opportunity gaps but remains part of these wall arrival gaps.
// Keeping the two clocks separate avoids both starvation under healthy slow
// control and teaching blocked service as confirmed worker silence.
func (q *Queue) workerArrivalAllowance(m *Machine) time.Duration {
	allowance := q.machineLossTimeout(m.ID)
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	if c := q.contacts[m.ID]; c != nil && c.session == m.Session {
		allowance = max(allowance, c.arrivalAllowance, time.Duration(max(2*c.cadence.Mean, c.cadence.upper())*float64(time.Second)))
		// Before a second arrival, a completed response already measures the
		// service the sender must wait for before its next interval can begin.
		allowance = max(allowance, 2*c.period+time.Duration(c.delivery.upper()*float64(time.Second)))
	}
	return allowance
}

// workerSuspect bounds uncertainty even if no control service opportunities
// elapse. An old in-flight request cannot prove the sender is still alive.
// Suspicion withholds new work and alerts, but never interrupts current custody
// or charges loss/retry. A new arrival clears it, including while waiting on mu.
func (q *Queue) workerSuspect(m *Machine, now time.Time) bool {
	return q.workerArrivalSuspect(m, now, true)
}

func (q *Queue) workerArrivalSuspect(m *Machine, now time.Time, includePending bool) bool {
	if m == nil || m.Gone || m.LastSeen.IsZero() {
		return false
	}
	allowance := q.workerArrivalAllowance(m)
	if q.workerLossAllowance(m) == 0 {
		return true // Unknown cold cadence cannot qualify capacity as healthy.
	}
	arrival, suspect := q.workerArrival(m, includePending)
	return suspect || !arrival.IsZero() && now.Sub(arrival) >= allowance
}

// Preserve the wall arrival as well as its measured allowance when freezing
// contact. A later published read ages that evidence at its own capture time;
// an already detached view keeps its original clock and suspicion.
func (q *Queue) workerArrival(m *Machine, includePending bool) (time.Time, bool) {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	arrival := m.LastSeen
	suspect := false
	if c := q.contacts[m.ID]; c != nil && c.session == m.Session {
		suspect = c.suspect
		arrival = c.processedArrival
		if q.simulation {
			arrival = m.LastSeen // The event loop compresses continuous heartbeats.
		}
		for p := range c.pending {
			if includePending {
				arrival = maxTime(arrival, p.arrived)
			}
		}
		if arrival.IsZero() {
			arrival = m.LastSeen
		}
	}
	return arrival, suspect
}

func (q *Queue) workerFresh(m *Machine, now time.Time) bool {
	return m != nil && !m.Gone && !m.LastSeen.IsZero() && !now.Before(m.LastSeen) && !q.workerSuspect(m, now)
}

func (q *Queue) contactFresh(m *Machine, now time.Time, includePending bool) bool {
	return m != nil && !m.Gone && !m.LastSeen.IsZero() && !now.Before(m.LastSeen) &&
		now.Sub(q.contactTime(m, now, includePending)) < q.machineLossTimeout(m.ID)
}

// Restart recovery also needs available queue service; a startup stall cannot
// consume the explicit operator recovery window before workers can reconcile.
func (q *Queue) inRestartGrace(now time.Time) bool {
	if q.mu.control != nil && q.restartControlUntil > 0 {
		return q.controlNow() < q.restartControlUntil
	}
	return now.Before(q.restartUntil)
}

// freezeContactView preserves accepted cadence, suspicion and remaining service
// grace in a detached observation. The caller owns q.mu or an immutable source.
func (q *Queue) freezeContactView(v *Queue) {
	if q.mu.control != nil {
		v.mu.control = &controlClock{clock: v.clock.Now, at: v.clock.Now(), opportunity: q.controlNow()}
	}
	if q.mu.control != nil && q.restartControlUntil > 0 {
		v.restartUntil = v.clock.Now().Add(q.restartControlUntil - q.controlNow())
	}
	// Freeze session contact at capture time just like the queue clock. Without
	// it, a detached fleet view turns an in-flight check-in into worker silence.
	v.contacts = make(map[string]*workerContact, len(q.s.Machines))
	for id, m := range q.s.Machines {
		arrival, _ := q.workerArrival(m, true)
		v.contacts[id] = &workerContact{session: m.Session, processed: q.lastContact(m, v.clock.Now()), processedArrival: arrival, allowance: q.workerLossAllowance(m), arrivalAllowance: q.workerArrivalAllowance(m), suspect: q.workerSuspect(m, v.clock.Now())}
	}
}

// Capture only the inputs to contact/loss diagnostics while q.mu is held.
// A status reader computes the frozen view afterward, without the writer lock.
// In particular, machineLossTimeout must not scan retained jobs repeatedly on
// the live queue merely to copy a worker's contact evidence.
func (q *Queue) captureContactInputs(clock Clock) *Queue {
	v := &Queue{clock: clock, simulation: q.simulation, lossTimeouts: q.lossTimeouts,
		restartUntil: q.restartUntil, restartControlUntil: q.restartControlUntil,
		s:     state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{}, ModelSlots: map[string]*ModelSlot{}},
		calls: map[string]*ModelCall{}, contacts: map[string]*workerContact{}}
	if q.mu.control != nil {
		v.mu.control = &controlClock{clock: clock.Now, at: clock.Now(), opportunity: q.controlNow()}
	}
	jobs := q.s.Jobs
	if q.records != nil && !q.simulation && q.records.jobCount == len(jobs) {
		jobs = q.records.live
	}
	for id, j := range jobs {
		if j.State == "running" || j.State == "stopping" {
			v.s.Jobs[id] = &Job{Machine: j.Machine, State: j.State, Spec: Spec{Priority: j.Spec.Priority}}
		}
	}
	for id, c := range q.calls {
		v.calls[id] = &ModelCall{Machine: c.Machine, Error: c.Error, Priority: c.Priority}
	}
	for id, slot := range q.s.ModelSlots {
		v.s.ModelSlots[id] = &ModelSlot{Machine: slot.Machine, State: slot.State, InFlight: slot.InFlight, Priority: slot.Priority}
	}
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	for id, m := range q.s.Machines {
		v.s.Machines[id] = &Machine{ID: m.ID, Session: m.Session, LastSeen: m.LastSeen, Gone: m.Gone}
		if c := q.contacts[id]; c != nil {
			copy := *c
			copy.pending = make(map[*checkinContact]struct{}, len(c.pending))
			for pending := range c.pending {
				p := *pending
				copy.pending[&p] = struct{}{}
			}
			v.contacts[id] = &copy
		}
	}
	v.workerPolls = q.cloneWorkerPolls()
	return v
}
