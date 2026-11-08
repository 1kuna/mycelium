package myce

import (
	"context"
	"time"
)

// workerContact is the queue's own record of one worker session's check-ins.
// Queue service time is excluded by controlClock, including cancelled requests.
// Accepted check-ins count from processing completion; pending arrivals have a
// measured, session-specific allowance in the same opportunity clock.
type workerContact struct {
	session              string
	pending              map[*checkinContact]struct{}
	processed            time.Time
	processedArrival     time.Time
	suspect              bool // Frozen detached observation.
	processedOpportunity time.Duration
	measured             bool
	period               time.Duration
	cadence              probeEstimate // Wall-time send cadence, independent of service availability.
	gap                  probeEstimate
	path                 probeEstimate
	retry                workerRetry // Existing legacy worker reconnect schedule.
	arrivalOpportunity   time.Duration
	arrivalWall          time.Time
	seenArrival          bool
	allowance            time.Duration // Only on immutable detached observations.
}

type checkinContact struct {
	arrived     time.Time
	opportunity time.Duration
	period      time.Duration
	advertised  bool
	inFlight    bool
}

// Cadence qualification precedes new offers. A cold legacy sender must first
// establish an observed interval; no job can acquire an unbounded unknown loss
// allowance. Current workers qualify on their advertised interval immediately.
func (q *Queue) acceptContactPeriod(m *Machine, period time.Duration, pending *checkinContact) {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	if q.contacts == nil {
		q.contacts = map[string]*workerContact{}
	}
	c := q.contacts[m.ID]
	if c == nil || c.session != m.Session {
		c = &workerContact{session: m.Session}
		q.contacts[m.ID] = c
	}
	c.period = period
	if pending != nil {
		if c.pending == nil {
			c.pending = map[*checkinContact]struct{}{}
		}
		c.pending[pending] = struct{}{}
	}
}

// Reconnection gaps describe an outage, not the live sender's cadence.
// Call with q.mu held, at the loss/outage decision rather than after a report
// has already cleared Gone or completed the recovery epoch.
func (q *Queue) discontinueContactCadence(id string) {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	if c := q.contacts[id]; c != nil {
		c.seenArrival = false
	}
}

// beginCheckin records a check-in before it waits for q.mu. The returned
// settle is idempotent. Only the last accepted current session gets pending
// credit. Rejection removes its credit; cancellation grants only a measured
// reconnect opportunity, never refreshing accepted LastSeen. An abandoned arrival
// cannot hide silence after the queue has had enough service opportunities.
func (q *Queue) beginCheckin(machine, session string, contexts ...context.Context) func(accepted bool) {
	return q.beginWorkerCheckin(machine, session, 0, false, contexts...)
}

func (q *Queue) beginWorkerCheckin(machine, session string, period time.Duration, active bool, contexts ...context.Context) func(accepted bool) {
	_, settle := q.trackWorkerCheckin(machine, session, period, active, contexts...)
	return settle
}

func (q *Queue) trackWorkerCheckin(machine, session string, period time.Duration, active bool, contexts ...context.Context) (*checkinContact, func(bool)) {
	q.contactMu.Lock()
	c := q.contacts[machine]
	pending := &checkinContact{arrived: q.clock.Now(), opportunity: q.controlNow(), period: checkinPeriod(period), advertised: period > 0, inFlight: active}
	if c != nil && c.session == session {
		if c.pending == nil {
			c.pending = map[*checkinContact]struct{}{}
		}
		c.pending[pending] = struct{}{}
	}
	q.contactMu.Unlock()
	settled := false
	return pending, func(accepted bool) {
		// Recheck the authoritative session before publishing a completed
		// contact. Never acquire q.mu while holding contactMu.
		if accepted {
			unlockRequest := requestReadLock(requestContext(contexts), &q.mu)
			defer unlockRequest()
		}
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		if settled {
			return
		}
		settled = true
		pending.inFlight = false
		// Acceptance can attach a first/new-session request after begin. Settle
		// the authoritative contact, not just the session captured at arrival.
		if c != nil {
			delete(c.pending, pending)
		}
		c = q.contacts[machine]
		if c != nil && c.session != session {
			c = nil
		}
		if c != nil && requestContext(contexts).Err() != nil {
			if c.pending == nil {
				c.pending = map[*checkinContact]struct{}{}
			}
			c.pending[pending] = struct{}{}
			// The server has finished a cancelled control call. Give its retry
			// the measured send opportunity; do not backdate queue service as
			// silence or change the accepted LastSeen timestamp.
			pending.opportunity = q.controlNow()
			c.retry.failed(q.clock.Now(), pending.period)
			pending.period = max(pending.period, c.retry.delay)
		}
		if c != nil && requestContext(contexts).Err() == nil {
			delete(c.pending, pending)
		}
		if accepted {
			m := q.s.Machines[machine]
			if m != nil && m.Session == session {
				if q.contacts == nil {
					q.contacts = map[string]*workerContact{}
				}
				current := q.contacts[machine]
				if current == nil || current.session != session {
					current = &workerContact{session: session}
					q.contacts[machine] = current
				}
				if current.seenArrival && !q.simulation && !q.networkQuiet && q.s.NetworkRecoveryAt.IsZero() {
					if gap := pending.arrived.Sub(current.arrivalWall); gap > 0 {
						current.cadence.observe(gap.Seconds())
					}
					if gap := pending.opportunity - current.arrivalOpportunity; gap > 0 {
						current.gap.observe(gap.Seconds())
					}
				}
				current.arrivalOpportunity, current.arrivalWall, current.seenArrival = pending.opportunity, pending.arrived, true
				current.period = 0
				if pending.advertised {
					current.period = pending.period
				}
				if requestContext(contexts).Err() == nil {
					current.retry = workerRetry{}
					for p := range current.pending {
						if !p.inFlight {
							delete(current.pending, p)
						}
					}
				}
				if cost := q.controlNow() - pending.opportunity; cost > 0 {
					current.path.observe(cost.Seconds())
				}
				current.processedArrival = maxTime(current.processedArrival, pending.arrived)
				current.processed = q.clock.Now()
				current.processedOpportunity, current.measured = q.controlNow(), true
			}
		}
	}
}

// lastContact is the later of m.LastSeen and the queue's own contact record for
// m's current session; active processing protects that worker from confirmed
// loss, while wall-clock suspicion remains independent. Callers hold q.mu.
func (q *Queue) lastContact(m *Machine, now time.Time) time.Time {
	allowance := q.machineLossTimeout(m.ID)
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	c := q.contacts[m.ID]
	if c == nil || c.session != m.Session {
		return m.LastSeen
	}
	opportunity := q.controlNow()
	contact := false
	for pending := range c.pending {
		if pending.inFlight || opportunity-pending.opportunity < max(allowance, 2*pending.period) {
			contact = true
		} else {
			delete(c.pending, pending)
		}
	}
	if contact {
		return maxTime(now, m.LastSeen)
	}
	if c.measured {
		return maxTime(m.LastSeen, now.Add(-max(0, opportunity-c.processedOpportunity)))
	}
	if c.processed.After(m.LastSeen) {
		return c.processed
	}
	return m.LastSeen
}
