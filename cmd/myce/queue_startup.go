package main

import (
	"errors"
	"net"
	"strings"
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

// systemd waits for READY=1 for the queue's Type=notify unit. Foreground queues
// and launchd have no notification socket and keep the same startup behavior.
func notifyQueueReady(path string) error {
	if path == "" {
		return nil
	}
	if !strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "@") {
		return errors.New("invalid queue readiness socket")
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		return errors.New("queue readiness notification failed")
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("READY=1")); err != nil {
		return errors.New("queue readiness notification failed")
	}
	return nil
}
