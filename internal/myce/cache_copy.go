package myce

import (
	"context"
	"sync"
	"time"
)

// Each experiment spans at least the previous measured observation window and
// a contribution from every active stream. This measures aggregate throughput,
// rather than rewarding whichever reader happens to report a burst first.
// A single stream is the minimum cold experiment; only waiting demand adds one
// experimental stream. No CPU count or installation-specific rate is assumed.
type copyGovernor struct {
	mu              sync.Mutex
	clock           Clock
	limit           int
	streams         map[*copyStream]bool
	waiters         int
	changed         chan struct{}
	at              time.Time
	bytes           int64
	rate, blockedAt float64
	window          time.Duration
	probe           bool
	controlBaseline time.Duration
	controlRange    time.Duration
	controlPrevious time.Duration
	controlSamples  int
	measureControl  func() (time.Duration, error)
	epoch           uint64
	measuring       bool
	controlSlow     bool
}
type copyStream struct {
	governor *copyGovernor
	once     sync.Once
}

func (g *copyGovernor) initLocked() {
	if g.limit == 0 {
		g.limit = 1
	}
	if g.changed == nil {
		g.changed = make(chan struct{})
	}
	if g.clock == nil {
		g.clock = RealClock{}
	}
	if g.streams == nil {
		g.streams = map[*copyStream]bool{}
	}
}
func (g *copyGovernor) wakeLocked() { close(g.changed); g.changed = make(chan struct{}) }
func (g *copyGovernor) resetLocked() {
	g.epoch++
	g.at, g.bytes = g.clock.Now(), 0
	for s := range g.streams {
		g.streams[s] = false
	}
}
func (s *copyStream) progress(n int64) {
	if n <= 0 {
		return
	}
	g := s.governor
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, active := g.streams[s]; !active {
		return
	}
	g.streams[s] = true
	g.bytes = cacheSum(g.bytes, n)
	elapsed := g.clock.Now().Sub(g.at)
	if elapsed <= 0 || elapsed < g.window {
		return
	}
	for _, seen := range g.streams {
		if !seen {
			return
		}
	}
	if g.measureControl != nil {
		if g.measuring {
			return
		}
		probe, epoch := g.measureControl, g.epoch
		g.measuring = true
		g.mu.Unlock()
		duration, err := probe()
		g.mu.Lock()
		g.measuring = false
		if epoch != g.epoch {
			return
		}
		if err != nil {
			g.controlSlow = true
		} else {
			g.controlLocked(duration)
		}
		elapsed = g.clock.Now().Sub(g.at)
	}
	rate := float64(g.bytes) / elapsed.Seconds()
	if g.controlSlow {
		// Completed epochs can retract established concurrency as the volume's
		// conditions change. Active streams finish; future admission contracts.
		g.limit = max(1, g.limit-1)
		g.blockedAt = 0
		g.probe = false
	} else if g.probe && len(g.streams) >= g.limit {
		if rate <= g.rate {
			g.limit = max(1, g.limit-1)
			g.blockedAt = max(g.rate, rate)
		}
		g.probe = false
	} else if !g.probe && len(g.streams) == g.limit && g.waiters > 0 && rate > g.blockedAt && !g.controlSlow {
		g.limit++
		g.probe = true
	}
	g.rate, g.window = rate, elapsed
	g.resetLocked()
	g.wakeLocked()
}

// Metadata latency is measured independently of transfer reads. An experiment
// cannot buy throughput at the cost of worsening this volume's control access.
func (g *copyGovernor) control(d time.Duration) {
	if d <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	g.controlLocked(d)
}
func (g *copyGovernor) controlLocked(d time.Duration) {
	if d <= 0 {
		return
	}
	ceiling := time.Duration(cacheSum(int64(g.controlBaseline), int64(g.controlRange)))
	g.controlSlow = g.controlBaseline > 0 && d > ceiling
	baseline := !g.probe || len(g.streams) < g.limit
	// Two observations are the minimum pair that can measure natural variation.
	if baseline && (g.controlSamples < 2 || !g.controlSlow) {
		if g.controlPrevious > 0 {
			delta := d - g.controlPrevious
			if delta < 0 {
				delta = -delta
			}
			g.controlRange = max(g.controlRange, delta)
		}
		g.controlBaseline, g.controlPrevious = d, d
		g.controlSamples = min(2, g.controlSamples+1)
		g.controlSlow = false
	}
}

func (g *copyGovernor) acquire(ctx context.Context, explicit int) (*copyStream, error) {
	g.mu.Lock()
	g.initLocked()
	g.waiters++
	defer func() { g.waiters--; g.mu.Unlock() }()
	for {
		limit := g.limit
		if explicit > 0 {
			limit = explicit
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(g.streams) < limit {
			s := &copyStream{governor: g}
			g.streams[s] = false
			g.resetLocked()
			return s, nil
		}
		wait := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-wait:
		}
		g.mu.Lock()
	}
}
func (s *copyStream) release() {
	s.once.Do(func() {
		g := s.governor
		g.mu.Lock()
		defer g.mu.Unlock()
		delete(g.streams, s)
		// A completed short stream is not evidence for a larger experiment.
		if g.probe {
			g.limit = max(1, len(g.streams))
			g.probe = false
		}
		g.resetLocked()
		g.wakeLocked()
	})
}
func (g *copyGovernor) capacity() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.initLocked()
	return g.limit
}
