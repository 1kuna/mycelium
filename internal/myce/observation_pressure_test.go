package myce

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Observation must keep sampling while the queue owner advances injected time
// under its writer lock. Neither pressure sampling nor admission can take that
// lock: they must remain available when control work is blocked behind it.
func TestQueueMutexPressureConcurrentClockAdvance(t *testing.T) {
	q, clock := fixture(t)
	l := newObservationLimit(clock, q)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for _, observe := range []func(){
		func() { q.mu.Lock(); q.mu.Unlock() },
		func() { q.mu.RLock(); q.mu.RUnlock() },
		func() { l.pressure.measurement() },
		func() {
			done, _ := observationAdmit(l, "/api/status")
			if done != nil {
				done()
			}
		},
	} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 1000 {
				observe()
			}
		}()
	}
	before := clock.Now()
	close(start)
	for range 1000 {
		q.mu.Lock()
		clock.Advance(time.Nanosecond)
		q.mu.Unlock()
	}
	workers.Wait()
	if got := clock.Now().Sub(before); got != 1000*time.Nanosecond {
		t.Fatal("injected time changed", got)
	}
	p := l.pressure
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.holders != 0 || len(p.waiting) != 0 {
		t.Fatal("completed observations left pressure state", p.holders, len(p.waiting))
	}
}

func TestQueueMutexPressureClockCoverage(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1, 0)}
	var mu queueMutex
	p := mu.enablePressure(clock)
	interval := 10 * time.Microsecond
	// Include every lock entry point: these fast operations are absent from slow
	// logs, but must establish the controller's own measured service baseline.
	for _, lock := range []func() func(){
		func() func() { mu.Lock(); return mu.Unlock },
		func() func() { mu.RLock(); return mu.RUnlock },
		func() func() { return requestLock(context.Background(), &mu) },
		func() func() { return requestReadLock(context.Background(), &mu) },
		func() func() {
			if !mu.TryLock() {
				t.Fatal("try lock")
			}
			return mu.Unlock
		},
		func() func() {
			if !mu.TryRLock() {
				t.Fatal("try read lock")
			}
			return mu.RUnlock
		},
	} {
		unlock := lock()
		clock.Advance(interval)
		unlock()
	}
	if p.samples != 6 || time.Duration(p.baseline) != interval {
		t.Fatal(p.samples, p.baseline)
	}
	mu.RLock()
	mu.RLock()
	clock.Advance(interval)
	mu.RUnlock()
	if p.holders != 1 {
		t.Fatal("overlapping occupancy ended early")
	}
	mu.RUnlock()
	if p.holders != 0 || p.samples != 6 {
		t.Fatal("overlap counted as quiet baseline", p.holders, p.samples)
	}

	for _, reader := range []bool{false, true} {
		mu.Lock()
		release, acquired, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() {
			defer close(done)
			var unlock func()
			if reader {
				unlock = requestReadLock(context.Background(), &mu)
			} else {
				unlock = requestLock(context.Background(), &mu)
			}
			close(acquired)
			<-release
			unlock()
		}()
		// Synchronize with the real blocked acquisition, not a wall-clock sleep.
		deadline := time.After(testWaitTimeout)
		for {
			p.mu.Lock()
			pending := len(p.waiting)
			p.mu.Unlock()
			if pending > 0 {
				break
			}
			select {
			case <-deadline:
				mu.Unlock()
				close(release)
				<-done
				t.Fatal("lock waiter not registered")
			default:
				runtime.Gosched()
			}
		}
		clock.Advance(interval)
		if pressured, _ := p.pressure(); pressured {
			t.Fatal("one normal service turn treated as overload")
		}
		clock.Advance(time.Nanosecond)
		if pressured, _ := p.pressure(); !pressured {
			t.Fatal("blocked work not measured")
		}
		mu.Unlock()
		<-acquired
		if pressured, _ := p.pressure(); pressured {
			t.Fatal("completed wait left stale pressure")
		}
		close(release)
		<-done
	}
}
