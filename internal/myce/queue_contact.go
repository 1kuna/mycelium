package myce

import (
	"context"
	"time"
)

// workerContact is the queue's own record of one worker session's check-ins.
// Time a check-in spends waiting behind q.mu is queue time, not worker silence
// (#223, #343): a matching-session arrival counts for at most 30 s while in
// flight, including when queue delay causes its client to cancel.
// Accepted check-ins count from when the queue finished processing them.
type workerContact struct {
	session   string
	pending   map[*checkinContact]struct{}
	processed time.Time
}

// This bounds how long an abandoned request can hide worker silence. The cost
// is losing protection for queue stalls longer than 30 s; it is not a retry or
// an extension of the ordinary 5 s production silence timeout.
const pendingCheckinContactLimit = 30 * time.Second

type checkinContact struct{ arrived time.Time }

// beginCheckin records a check-in before it waits for q.mu. The returned
// settle is idempotent. Only the last accepted current session gets pending
// credit. A report later rejected under q.mu can delay loss detection by at
// most 30 s. Rejection removes its credit; cancellation retains only the
// original bounded arrival credit, never refreshing accepted contact. Otherwise
// a client deadline during queue contention retroactively becomes silence.
func (q *Queue) beginCheckin(machine, session string, contexts ...context.Context) func(accepted bool) {
	q.contactMu.Lock()
	c := q.contacts[machine]
	var pending *checkinContact
	if c != nil && c.session == session {
		pending = &checkinContact{arrived: q.clock.Now()}
		if c.pending == nil {
			c.pending = map[*checkinContact]struct{}{}
		}
		c.pending[pending] = struct{}{}
	}
	q.contactMu.Unlock()
	settled := false
	return func(accepted bool) {
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
		if pending != nil && (accepted || requestContext(contexts).Err() == nil) {
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
				current.processed = q.clock.Now()
			}
		}
	}
}

// lastContact is the later of m.LastSeen and the queue's own contact record for
// m's current session; an unexpired in-flight arrival is contact at now.
// Expired requests stop contributing even if settle is never called. Callers hold q.mu.
func (q *Queue) lastContact(m *Machine, now time.Time) time.Time {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	c := q.contacts[m.ID]
	if c == nil || c.session != m.Session {
		return m.LastSeen
	}
	for pending := range c.pending {
		if now.Before(pending.arrived.Add(pendingCheckinContactLimit)) {
			return maxTime(now, m.LastSeen)
		}
		delete(c.pending, pending)
	}
	if c.processed.After(m.LastSeen) {
		return c.processed
	}
	return m.LastSeen
}
