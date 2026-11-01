package myce

import (
	"context"
	"errors"
	"net/http"
	"time"
)

const (
	serverProbeInterval  = 100 * time.Millisecond
	serverProbeTimeout   = 5 * time.Second
	serverHealthFailures = 3
	serverHealthWindow   = 15 * time.Second
)

type serverProbeFailures struct {
	count int
	since time.Time
}

func (f *serverProbeFailures) observe(healthy bool, started, now time.Time) bool {
	if healthy {
		*f = serverProbeFailures{}
		return false
	}
	if f.count == 0 {
		f.since = started
	}
	f.count++
	return f.count >= serverHealthFailures && now.Sub(f.since) >= serverHealthWindow
}

// Probing is bounded I/O, independent of the queue's injected scheduling Clock.
// It never releases custody or starts another process; the heartbeat owns that
// transition. Completion remains observable even with logs directed to /dev/null.
func (l *LlamaEngine) startHealthMonitor(health string) {
	l.startHealthMonitorTimeout(health, serverProbeTimeout)
}

func (l *LlamaEngine) startHealthMonitorTimeout(health string, timeout time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	l.monitorCancel = cancel
	l.monitorDone = make(chan struct{})
	clock := l.Clock
	if clock == nil {
		clock = RealClock{}
	}
	go func() {
		defer close(l.monitorDone)
		tick := time.NewTicker(serverProbeInterval)
		defer tick.Stop()
		client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		failures := serverProbeFailures{}
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.exited:
				return
			case <-tick.C:
			}
			started := clock.Now()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, health, nil)
			res, err := client.Do(req)
			healthy := err == nil && res.StatusCode >= 200 && res.StatusCode < 300
			if res != nil {
				res.Body.Close()
			}
			if ctx.Err() != nil {
				return
			}
			l.healthMu.Lock()
			l.healthDegraded = !healthy
			if !healthy {
				l.healthRevision++
			}
			failed := failures.observe(healthy, started, clock.Now())
			if failed {
				l.healthErr = errors.New("server health failed")
			}
			l.healthMu.Unlock()
			if failed {
				return
			}
		}
	}()
}

func (l *LlamaEngine) runtimeHealth() (error, uint64, bool) {
	l.healthMu.Lock()
	defer l.healthMu.Unlock()
	err := l.healthErr
	if l.exited != nil {
		select {
		case <-l.exited:
			err = errors.New("server process exited")
		default:
		}
	}
	return err, l.healthRevision, l.healthDegraded
}
