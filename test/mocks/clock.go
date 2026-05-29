package mocks

import (
	"sync"
	"time"

	"mycelium/internal/ports"
)

type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) NewTimer(d time.Duration) ports.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeTimer{ch: make(chan time.Time, 1), fireAt: c.now.Add(d)}
	c.timers = append(c.timers, timer)
	return timer
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, timer := range c.timers {
		if !timer.fired && !c.now.Before(timer.fireAt) {
			timer.fired = true
			timer.ch <- c.now
		}
	}
}

type fakeTimer struct {
	ch     chan time.Time
	fireAt time.Time
	fired  bool
}

func (t *fakeTimer) C() <-chan time.Time {
	return t.ch
}

func (t *fakeTimer) Stop() bool {
	if t.fired {
		return false
	}
	t.fired = true
	return true
}

var _ ports.Clock = (*FakeClock)(nil)
