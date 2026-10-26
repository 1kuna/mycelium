package myce

import "time"

// workerContact is the queue's own record of one worker session's check-ins.
// Time a check-in spends waiting behind q.mu is queue time, not worker silence
// (#223): while one is in flight the worker is in contact, and an accepted
// check-in counts from when the queue finished processing it.
type workerContact struct {
	pending   int
	processed time.Time
}

// beginCheckin records a check-in before it waits for q.mu. The returned
// settle is idempotent; only an accepted check-in refreshes contact, so a
// rejected or stale report never extends liveness beyond its own wait.
func (q *Queue) beginCheckin(machine, session string) func(accepted bool) {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	if q.contacts == nil {
		q.contacts = map[string]map[string]*workerContact{}
	}
	if q.contacts[machine] == nil {
		q.contacts[machine] = map[string]*workerContact{}
	}
	c := q.contacts[machine][session]
	if c == nil {
		c = &workerContact{}
		q.contacts[machine][session] = c
	}
	c.pending++
	settled := false
	return func(accepted bool) {
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		if settled {
			return
		}
		settled = true
		c.pending--
		if accepted {
			c.processed = q.clock.Now()
		}
		for id, other := range q.contacts[machine] {
			if other.pending == 0 && (other.processed.IsZero() || accepted && id != session) {
				delete(q.contacts[machine], id)
			}
		}
	}
}

// lastContact is the later of m.LastSeen and the queue's own contact record for
// m's current session; a check-in still in flight is contact at now. Callers hold q.mu.
func (q *Queue) lastContact(m *Machine, now time.Time) time.Time {
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	c := q.contacts[m.ID][m.Session]
	switch {
	case c == nil:
		return m.LastSeen
	case c.pending > 0 && now.After(m.LastSeen):
		return now
	case c.processed.After(m.LastSeen):
		return c.processed
	}
	return m.LastSeen
}
