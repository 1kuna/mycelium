package main

import (
	"context"
	"time"
)

// Parent PID changes when a process is reparented on both supported Unix OSes.
// Check before the first request too: starting with init as parent is already
// orphaned. One second bounds cleanup latency without a per-request watcher.
func waitParentContext(ctx context.Context, parent int, interval time.Duration, getppid func() int) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	if parent <= 1 || getppid() != parent {
		cancel()
		return ctx, cancel
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if getppid() != parent {
					cancel()
					return
				}
			}
		}
	}()
	return ctx, cancel
}
