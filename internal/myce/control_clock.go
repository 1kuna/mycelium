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
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	arrival := m.LastSeen
	if c := q.contacts[m.ID]; c != nil && c.session == m.Session {
		if c.arrivalAllowance > 0 || c.suspect { // Frozen detached observation.
			return c.suspect
		}
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
	return !arrival.IsZero() && now.Sub(arrival) >= allowance
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
		v.contacts[id] = &workerContact{session: m.Session, processed: q.lastContact(m, v.clock.Now()), allowance: q.workerLossAllowance(m), arrivalAllowance: q.workerArrivalAllowance(m), suspect: q.workerSuspect(m, v.clock.Now())}
	}
}
