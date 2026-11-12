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
	delivery             probeEstimate // Completed control response cost, including queue service.
	gap                  probeEstimate
	path                 probeEstimate
	retry                workerRetry // Existing legacy worker reconnect schedule.
	arrivalOpportunity   time.Duration
	arrivalWall          time.Time
	seenArrival          bool
	allowance            time.Duration // Only on immutable detached observations.
	arrivalAllowance     time.Duration // Frozen wall-delivery allowance on detached observations.
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

type contactEvidence struct {
	Session   string        `json:"session"`
	Allowance time.Duration `json:"allowance"`
	Seen      time.Time     `json:"seen,omitempty"` // Accepted LastSeen; machine records omit it.
}

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
	defer func() {
		q.contactMu.Unlock()
		if q.s.ContactEvidence == nil {
			q.s.ContactEvidence = map[string]contactEvidence{}
		}
		q.s.ContactEvidence[m.ID] = contactEvidence{Session: m.Session, Seen: m.LastSeen}
		if q.workerLossAllowance(m) > 0 {
			q.s.ContactEvidence[m.ID] = contactEvidence{Session: m.Session, Allowance: q.machineLossTimeout(m.ID), Seen: m.LastSeen}
		}
	}()
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
				if cost := q.clock.Now().Sub(pending.arrived); cost > 0 {
					current.delivery.observe(cost.Seconds())
				}
				current.processedArrival = maxTime(current.processedArrival, pending.arrived)
				current.processed = q.clock.Now()
				current.processedOpportunity, current.measured = q.controlNow(), true
			}
		}
	}
}

// workerPoll records one worker session's stop-event long polls. The worker
// runs that poll beside its check-in loop, so an open poll shows the process is
// alive and connected while local work (a slow fsync under host load) delays
// its next check-in (#1450). It protects only against confirmed loss; suspicion
// and cadence learning still come from check-ins alone.
type workerPoll struct {
	open int
	last time.Duration // Control opportunity of the latest proof of life.
}

// trackWorkerPoll registers an events request for its lifetime. A poll that
// ends normally is contact at its end; a dropped connection proves nothing
// after its start. A silently dead host therefore keeps its credit for at most
// the server's existing poll window.
func (q *Queue) trackWorkerPoll(ctx context.Context, machine, session string) func() {
	if machine == "" || session == "" {
		return func() {}
	}
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	if q.workerPolls == nil {
		q.workerPolls = map[string]map[string]*workerPoll{}
	}
	sessions := q.workerPolls[machine]
	if sessions == nil {
		sessions = map[string]*workerPoll{}
		q.workerPolls[machine] = sessions
	}
	for s, p := range sessions {
		if s != session && p.open == 0 {
			delete(sessions, s) // A replaced session's settled record.
		}
	}
	p := sessions[session]
	if p == nil {
		p = &workerPoll{}
		sessions[session] = p
	}
	p.open++
	p.last = q.controlNow()
	return func() {
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		p.open--
		if ctx.Err() == nil {
			p.last = q.controlNow()
		}
	}
}

// Detached views keep the same poll evidence as the live loss decision.
// Caller holds contactMu.
func (q *Queue) cloneWorkerPolls() map[string]map[string]*workerPoll {
	out := make(map[string]map[string]*workerPoll, len(q.workerPolls))
	for machine, sessions := range q.workerPolls {
		out[machine] = make(map[string]*workerPoll, len(sessions))
		for session, p := range sessions {
			copy := *p
			out[machine][session] = &copy
		}
	}
	return out
}

// lastContact is the later of m.LastSeen and the queue's own contact record for
// m's current session; active processing protects that worker from confirmed
// loss, while wall-clock suspicion remains independent. Callers hold q.mu.
func (q *Queue) lastContact(m *Machine, now time.Time) time.Time {
	return q.contactTime(m, now, true)
}

// checkinDeliveryUpper predicts, in seconds, how long this session's next
// check-in takes from arrival to completion (mean plus four deviations of
// completed check-ins). Zero until a check-in of the session has completed.
func (q *Queue) checkinDeliveryUpper(m *Machine) float64 {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	if c := q.contacts[m.ID]; c != nil && c.session == m.Session {
		return c.delivery.upper()
	}
	return 0
}

// Pending requests protect their sender, but cannot identify a continuously
// reporting survivor when deciding whether a fleet outage preceded reconnects.
func (q *Queue) contactTime(m *Machine, now time.Time, includePending bool) time.Time {
	allowance := q.machineLossTimeout(m.ID)
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	opportunity := q.controlNow()
	polled := m.LastSeen
	if p := q.workerPolls[m.ID][m.Session]; includePending && p != nil {
		if p.open > 0 {
			return maxTime(now, m.LastSeen)
		}
		polled = maxTime(polled, now.Add(-max(0, opportunity-p.last)))
	}
	c := q.contacts[m.ID]
	if c == nil || c.session != m.Session {
		return polled
	}
	contact := false
	for pending := range c.pending {
		if !includePending {
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
		return maxTime(polled, now.Add(-max(0, opportunity-c.processedOpportunity)))
	}
	return maxTime(polled, c.processed)
}
