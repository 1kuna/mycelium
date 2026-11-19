package main

import (
	"time"

	"mycelium/internal/myce"
)

// Startup telemetry contains phase names and durations only, never host data.
func queueStartupTrace(clock myce.Clock, logf func(string, ...any)) func(string) {
	started := clock.Now()
	previous := started
	return func(phase string) {
		now := clock.Now()
		logf("event=queue_startup_phase phase=%s duration_ms=%.3f elapsed_ms=%.3f", phase, float64(now.Sub(previous))/float64(time.Millisecond), float64(now.Sub(started))/float64(time.Millisecond))
		previous = now
	}
}
