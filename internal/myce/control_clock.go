package myce

import (
	"sync"
	"time"
)

// controlClock measures opportunities to hear from a worker. Time spent serving
// queue transactions is not evidence of worker silence. Overlapping readers
// count once. This is scalar accounting only, never I/O. A worker's own pending
// check-in is tracked separately: it cannot indefinitely hide a dead peer.
// Lock order: q.mu -> contactMu -> controlClock.mu; no reverse acquisition.
type controlClock struct {
	mu          sync.Mutex
	clock       func() time.Time
	at          time.Time
	active      int
	opportunity time.Duration
}

func (c *controlClock) advance() {
	now := c.clock()
	if c.active == 0 && !c.at.IsZero() && now.After(c.at) {
		c.opportunity += now.Sub(c.at)
	}
	c.at = now
}

func (c *controlClock) begin() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	c.active++
}

func (c *controlClock) end() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	c.active--
}

func (c *controlClock) now() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	return c.opportunity
}

func (q *Queue) controlNow() time.Duration {
	if c := q.mu.control; c != nil {
		return c.now()
	}
	return time.Duration(q.clock.Now().UnixNano())
}

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

// workerSuspect bounds uncertainty even if no control service opportunities
// elapse. An old in-flight request cannot prove the sender is still alive.
// Suspicion withholds new work and alerts, but never interrupts current custody
// or charges loss/retry. A new arrival clears it, including while waiting on mu.
func (q *Queue) workerSuspect(m *Machine, now time.Time) bool {
	if m == nil || m.Gone || m.LastSeen.IsZero() {
		return false
	}
	allowance := q.machineLossTimeout(m.ID)
	if q.workerLossAllowance(m) == 0 {
		return true // Unknown cold cadence cannot qualify capacity as healthy.
	}
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	arrival := m.LastSeen
	if c := q.contacts[m.ID]; c != nil && c.session == m.Session {
		if c.allowance > 0 || c.suspect { // Frozen observation.
			return c.suspect
		}
		arrival = c.processedArrival
		if q.simulation {
			arrival = m.LastSeen // The event loop compresses continuous heartbeats.
		}
		for p := range c.pending {
			arrival = maxTime(arrival, p.arrived)
		}
		if arrival.IsZero() {
			arrival = m.LastSeen
		}
	}
	return !arrival.IsZero() && now.Sub(arrival) >= allowance
}

func (q *Queue) workerFresh(m *Machine, now time.Time) bool {
	return q.workerContactFresh(m, now) && !q.workerSuspect(m, now)
}

// Network-outage recovery uses confirmation evidence, not wall-clock suspicion.
func (q *Queue) workerContactFresh(m *Machine, now time.Time) bool {
	return m != nil && !m.Gone && !m.LastSeen.IsZero() && !now.Before(m.LastSeen) &&
		now.Sub(q.lastContact(m, now)) < q.machineLossTimeout(m.ID)
}

// Restart recovery also needs available queue service; a startup stall cannot
// consume the explicit operator recovery window before workers can reconcile.
func (q *Queue) inRestartGrace(now time.Time) bool {
	if q.mu.control != nil && q.restartControlUntil > 0 {
		return q.controlNow() < q.restartControlUntil
	}
	return now.Before(q.restartUntil)
}
