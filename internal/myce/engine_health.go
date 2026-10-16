package myce

import (
	"context"
	"errors"
	"net/http"
	"time"
)

const (
	serverProbeInterval  = 100 * time.Millisecond
	serverProbeTimeout   = time.Second
	serverHealthFailures = 3
)

// Probing is bounded I/O, independent of the queue's injected scheduling Clock.
// It never releases custody or starts another process; the heartbeat owns that
// transition. Completion remains observable even with logs directed to /dev/null.
func (l *LlamaEngine) startHealthMonitor(health string) {
	ctx, cancel := context.WithCancel(context.Background())
	l.monitorCancel = cancel
	l.monitorDone = make(chan struct{})
	go func() {
		defer close(l.monitorDone)
		tick := time.NewTicker(serverProbeInterval)
		defer tick.Stop()
		client := &http.Client{Timeout: serverProbeTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		failures := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.exited:
				return
			case <-tick.C:
			}
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
			if healthy {
				failures = 0
			} else {
				failures++
				l.healthRevision++
			}
			if failures >= serverHealthFailures {
				l.healthErr = errors.New("server health failed")
			}
			l.healthMu.Unlock()
			if failures >= serverHealthFailures {
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
