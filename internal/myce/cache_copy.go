package myce

import (
	"context"
	"sync"
	"time"
)

// copyGovernor probes one additional writer only after a productive observation.
// It keeps that writer only if aggregate throughput increases. Starting with one
// is the minimum experiment that can measure a previously unknown volume/link,
// not a throughput assumption. Demand bounds exploration; cancellation wakes a
// waiter without discarding the active writer's custody.
type copyGovernor struct {
	mu            sync.Mutex
	clock         Clock
	limit, active int
	changed       chan struct{}
	at            time.Time
	bytes         int64
	rate          float64
	probe         bool
	sampleActive  int
	blockedAt     float64
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
}
func (g *copyGovernor) wakeLocked() { close(g.changed); g.changed = make(chan struct{}) }
func (g *copyGovernor) progress(n int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.initLocked()
	now := g.clock.Now()
	if g.sampleActive != g.active {
		g.at = now
		g.bytes = 0
		g.sampleActive = g.active
		return
	}
	g.bytes = cacheSum(g.bytes, n)
	if g.at.IsZero() {
		g.at = now
		return
	}
	elapsed := now.Sub(g.at)
	if elapsed <= 0 {
		return
	}
	rate := float64(g.bytes) / elapsed.Seconds()
	if g.probe && g.active >= g.limit {
		if rate <= g.rate {
			g.limit = max(1, g.limit-1)
			g.blockedAt = rate
		}
		g.probe = false
	} else if !g.probe && g.active == g.limit && rate > g.blockedAt {
		g.limit++
		g.probe = true
	}
	g.rate, g.at, g.bytes = rate, now, 0
	g.wakeLocked()
}
func (g *copyGovernor) acquire(ctx context.Context, explicit int) (func(), error) {
	for {
		g.mu.Lock()
		g.initLocked()
		limit := g.limit
		if explicit > 0 {
			limit = explicit
		}
		if err := ctx.Err(); err != nil {
			g.mu.Unlock()
			return nil, err
		}
		if g.active < limit {
			g.active++
			g.mu.Unlock()
			var once sync.Once
			return func() { once.Do(func() { g.mu.Lock(); g.active--; g.wakeLocked(); g.mu.Unlock() }) }, nil
		}
		wait := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait:
		}
	}
}
func (g *copyGovernor) capacity() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.initLocked()
	return g.limit
}
