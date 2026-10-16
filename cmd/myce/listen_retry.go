package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

// Boot can precede assignment of the configured interface address. Keep this
// transport-only retry separate from queue scheduling, and never retry a conflict
// or permission failure. Both queue and joining listeners use the same bound.
func listenWithRetry(ctx context.Context, address string, listen func(string, string) (net.Listener, error), clock myce.Clock, sleep func(context.Context, time.Duration) error, logf func(string, ...any)) (net.Listener, error) {
	deadline := clock.Now().Add(time.Minute)
	var nextLog time.Time
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		listener, err := listen("tcp", address)
		if err == nil {
			return listener, nil
		}
		if !errors.Is(err, syscall.EADDRNOTAVAIL) && !errors.Is(err, syscall.ENETUNREACH) {
			return nil, err
		}
		now := clock.Now()
		if !now.Before(deadline) {
			return nil, fmt.Errorf("listen retry deadline exceeded: %w", err)
		}
		if !now.Before(nextLog) {
			logf("Listen address unavailable; retrying for up to %.0fs: %v", deadline.Sub(now).Seconds(), err)
			nextLog = now.Add(5 * time.Second)
		}
		if err := sleep(ctx, min(2*time.Second, deadline.Sub(now))); err != nil {
			return nil, err
		}
	}
}

func waitForBind(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
