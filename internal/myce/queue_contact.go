package myce

import (
	"context"
	"maps"
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
	discarded   bool
}

// Contact decisions participate in the SQLite transaction without copying
// every worker on every queue update. Only a changed worker is copied. Pending
// arrivals can still be registered concurrently; rollback retains those for the
// restored session and never resurrects a settled rejected request.
type contactTransaction struct{ before map[string]*workerContact }

// Caller holds q.mu and contactMu.
func (q *Queue) editContact(id string) *workerContact {
	current := q.contacts[id]
	if tx := q.contactTransaction; tx != nil {
		if tx.before == nil {
			tx.before = map[string]*workerContact{}
		}
		if _, saved := tx.before[id]; !saved {
			tx.before[id] = current
			if current != nil {
				copy := *current
				copy.pending = maps.Clone(current.pending)
				current = &copy
				q.contacts[id] = current
			}
		}
	}
	return current
}

func (q *Queue) rollbackContacts(tx *contactTransaction) {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	for id, before := range tx.before {
		current := q.contacts[id]
		if before == nil {
			delete(q.contacts, id)
			continue
		}
		if current != nil && current.session == before.session {
			if before.pending == nil {
				before.pending = map[*checkinContact]struct{}{}
			}
			for p := range current.pending {
				before.pending[p] = struct{}{}
			}
		}
		for p := range before.pending {
			if p.discarded {
				delete(before.pending, p)
			}
		}
		q.contacts[id] = before
	}
}

// Recovery is a fresh service opportunity, not a fictitious accepted heartbeat.
// Actual arrival/LastSeen timestamps stay unchanged and continue to drive
// suspicion. The confirmed-silence baseline alone restarts for missing peers.
func (q *Queue) startNetworkRecovery(now time.Time) {
	q.s.NetworkRecoveryAt = now
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	for id, m := range q.s.Machines {
		if c := q.editContact(id); c != nil && c.session == m.Session {
			c.processedOpportunity, c.measured = q.controlNow(), true
			c.seenArrival = false
		}
	}
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
	c := q.editContact(m.ID)
	if c == nil || c.session != m.Session {
		c = &workerContact{session: m.Session}
		q.contacts[m.ID] = c
	}
	if period > 0 {
		c.period = period
	}
	if pending != nil {
		// Qualify a legacy sender on its second arrival, before scheduling this
		// response. Waiting until response settlement would idle it for an extra
		// send interval. The contact undo discards learning on commit failure.
		if c.seenArrival && !q.simulation && !q.networkQuiet && q.s.NetworkRecoveryAt.IsZero() {
			if gap := pending.arrived.Sub(c.arrivalWall); gap > 0 {
				c.cadence.observe(gap.Seconds())
			}
			if gap := pending.opportunity - c.arrivalOpportunity; gap > 0 {
				c.gap.observe(gap.Seconds())
			}
		}
		c.arrivalOpportunity, c.arrivalWall, c.seenArrival = pending.opportunity, pending.arrived, true
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
	if c := q.editContact(id); c != nil {
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
		pending.discarded = requestContext(contexts).Err() == nil
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
func (q *Queue) lastContact(m *Machine, now time.Time, ignored ...*checkinContact) time.Time {
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
		if len(ignored) > 0 && pending == ignored[0] {
			continue
		}
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
