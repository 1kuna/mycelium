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

// Serialize the physical release and its accounting. A contender can acquire
// the underlying mutex, but cannot enter service until this transition ends.
// release only unlocks; it must not acquire locks or reenter queue code.
func (c *controlClock) end(release func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	release()
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
