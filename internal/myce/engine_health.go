package myce

import (
	"context"
	"errors"
	"net/http"
	"time"
)

type serverProbeFailures struct {
	count   int
	since   time.Time
	latency readinessLatency
}

func (f *serverProbeFailures) observe(healthy bool, started, now time.Time) bool {
	if healthy {
		f.count, f.since = 0, time.Time{}
		f.latency.observe(now.Sub(started))
		return false
	}
	if f.count == 0 {
		f.since = started
	}
	f.count++
	// A second observation confirms continuous failure over the host's learned
	// healthy-response envelope. One slow/failed request alone cannot withdraw
	// readiness. This is confirmation, not a fleet-specific failure threshold.
	return f.count > 1 && now.Sub(f.since) >= f.latency.budget()
}

// Probing is bounded I/O, independent of the queue's injected scheduling Clock.
// It never releases custody or starts another process; the heartbeat owns that
// transition. Completion remains observable even with logs directed to /dev/null.
func (l *LlamaEngine) startHealthMonitor(health string) {
	l.startHealthMonitorTimeout(health, 0)
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
		client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		failures := serverProbeFailures{latency: l.probeLatency}
		for {
			if readinessPause(ctx, failures.latency.cadence()) != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-l.exited:
				return
			default:
			}
			client.Timeout = failures.latency.budget()
			if timeout > 0 {
				client.Timeout = timeout
			}
			started := clock.Now()
			realStarted := RealClock{}.Now()
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
			now := clock.Now()
			elapsed := max(now.Sub(started), RealClock{}.Now().Sub(realStarted))
			failed := failures.observe(healthy, now.Add(-elapsed), now)
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
