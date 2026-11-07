package myce

import (
	"sync"
	"sync/atomic"
	"time"
)

// queuePressure measures every mutex acquisition, not just slow-log samples.
// Its mutex protects only local scalar metadata. It is NEVER held while taking
// q.mu, calling a handler, or doing I/O; q.mu -> mu therefore has no reverse edge.
// The injected clock must synchronize its own reads and mutations: wait and
// measurement sample it without q.mu, even while the queue owner advances time.
// The mean uncontended occupied interval is the installation's service baseline.
// Waiting longer than one such interval means work is queued beyond an ordinary
// control turn. There is no rate, fleet-calibrated latency, or recovery timer.
type queuePressure struct {
	observedPressure atomic.Bool // Last measured result; diagnostics must not rescan waiters.
	mu               sync.Mutex
	clock            Clock
	waiting          map[*queuePressureWait]time.Time
	holders          int
	since            time.Time
	quiet            bool
	samples          uint64
	epoch            uint64  // invalidates read-cost samples across contention, including recovered waits
	baseline         float64 // nanoseconds, incremental mean; fixed space
}

type queuePressureWait struct{ contended bool }

func (p *queuePressure) wait() *queuePressureWait {
	p.mu.Lock()
	defer p.mu.Unlock()
	w := &queuePressureWait{contended: p.holders != 0 || len(p.waiting) != 0}
	if w.contended {
		p.epoch++
		p.quiet = false
	}
	p.waiting[w] = p.clock.Now()
	return w
}

func (p *queuePressure) acquired(w *queuePressureWait) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if started, ok := p.waiting[w]; ok && p.samples != 0 && float64(p.clock.Now().Sub(started)) > p.baseline {
		p.epoch++
	}
	delete(p.waiting, w)
	if len(p.waiting) == 0 {
		p.observedPressure.Store(false)
	}
	if p.holders == 0 {
		p.since = p.clock.Now()
		p.quiet = len(p.waiting) == 0 && (w == nil || !w.contended)
	} else {
		p.quiet = false
	}
	p.holders++
}

func (p *queuePressure) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.holders--
	if p.holders == 0 && p.quiet {
		if d := p.clock.Now().Sub(p.since); d > 0 {
			p.samples++
			p.baseline += (float64(d) - p.baseline) / float64(p.samples)
		}
	}
}

func (p *queuePressure) pressure() (bool, time.Duration) {
	pressure, delay, _ := p.measurement()
	return pressure, delay
}
func (p *queuePressure) measurement() (bool, time.Duration, uint64) {
	pressure, wait, epoch, _, _ := p.facts()
	return pressure, wait, epoch
}
func (p *queuePressure) facts() (bool, time.Duration, uint64, float64, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var wait time.Duration
	now := p.clock.Now()
	for _, started := range p.waiting {
		wait = max(wait, now.Sub(started))
	}
	pressured := p.samples != 0 && float64(wait) > p.baseline
	p.observedPressure.Store(pressured)
	return pressured, wait, p.epoch, p.baseline, p.samples
}
