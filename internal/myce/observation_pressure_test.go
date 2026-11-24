package myce

import (
	"context"
	"runtime"
	"testing"
	"time"
)

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
