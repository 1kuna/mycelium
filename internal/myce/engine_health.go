package myce

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"
)

const modelProbeInterval = time.Second

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
	pause := l.healthPause
	if pause == nil {
		pause = func(ctx context.Context, duration time.Duration) error {
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
	if timeout <= 0 {
		timeout = modelProbeInterval
	}
	go func() {
		defer close(l.monitorDone)
		client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		var failedSince time.Time
		for {
			if pause(ctx, modelProbeInterval) != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-l.exited:
				return
			default:
			}
			started := clock.Now()
			healthy, _ := probeServerHealth(ctx, client, health)
			if ctx.Err() != nil {
				return
			}
			l.healthMu.Lock()
			l.healthDegraded = !healthy
			if healthy {
				failedSince = time.Time{}
			} else {
				l.healthRevision++
				if failedSince.IsZero() {
					failedSince = started
				} else if clock.Now().Sub(failedSince) >= modelProbeInterval {
					l.healthErr = errors.New("server health failed")
				}
			}
			failed := l.healthErr != nil
			l.healthMu.Unlock()
			if failed {
				return
			}
		}
	}()
}

// probeServerHealth finishes the body before returning so successive probes can
// reuse the connection. Request cancellation still owns the transport deadline.
func probeServerHealth(ctx context.Context, client *http.Client, health string) (bool, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, health, nil)
	res, err := client.Do(req)
	healthy := err == nil && res.StatusCode >= 200 && res.StatusCode < 300
	if res != nil {
		_, _ = io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}
	return healthy, err
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
