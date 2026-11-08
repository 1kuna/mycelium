package myce

import (
	"testing"
	"time"
)

// Pause release accounting while another goroutine tries the underlying lock.
// Physical availability must not get ahead of its service-clock transition.
func TestControlServicePausedQueueRelease(t *testing.T) {
	for _, reader := range []bool{false, true} {
		t.Run(map[bool]string{false: "writer", true: "reader"}[reader], func(t *testing.T) {
			q, clock := fixture(t)
			if reader {
				q.mu.RLock()
			} else {
				q.mu.Lock()
			}
			q.mu.control.mu.Lock()
			released := make(chan struct{})
			go func() {
				if reader {
					q.mu.RUnlock()
				} else {
					q.mu.Unlock()
				}
				close(released)
			}()
			acquired := make(chan struct{})
			go func() { q.mu.mu.Lock(); q.mu.mu.Unlock(); close(acquired) }()
			leaked := false
			select {
			case <-acquired:
				leaked = true
			case <-time.After(100 * time.Millisecond):
			}
			clock.Advance(4 * time.Second)
			q.mu.control.mu.Unlock()
			<-released
			<-acquired
			if leaked {
				t.Fatal("queue became available before release bookkeeping could run")
			}
			start := q.controlNow()
			clock.Advance(3 * time.Second)
			if got := q.controlNow() - start; got != 3*time.Second {
				t.Fatal("available queue time disappeared", got)
			}
		})
	}
}
