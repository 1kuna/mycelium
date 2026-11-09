//go:build myce_test

package main

import (
	"context"
	"time"

	"mycelium/internal/myce"
)

// Only synthetic child binaries model enrollment's stability time. Queue,
// worker, transport and service lifetimes keep their real clocks. In particular,
// startup still waits for the installed worker's actual queue registration.
type enrollmentFixtureClock struct{ now time.Time }

func (c *enrollmentFixtureClock) Now() time.Time { return c.now }

func waitEnrollment(ctx context.Context, config myce.ServiceConfig, client myce.Client, options myce.EnrollmentWait) (myce.EnrollmentResult, error) {
	if options.Clock == nil {
		return myce.WaitEnrollment(ctx, config, client, options)
	}
	clock := &enrollmentFixtureClock{now: options.Clock.Now()}
	options.Clock = clock
	pause := options.Wait
	if pause == nil {
		pause = waitPause
	}
	managerObserved := false
	if command := options.Command; command != nil {
		options.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			data, err := command(ctx, name, args...)
			managerObserved = err == nil
			return data, err
		}
	}
	options.Wait = func(ctx context.Context, duration time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Before valid registration reaches the real manager probe, retain the
		// real pause so model time cannot exhaust the startup budget first.
		if !managerObserved {
			if err := pause(ctx, duration); err != nil {
				return err
			}
		}
		managerObserved = false
		clock.now = clock.now.Add(duration)
		return ctx.Err()
	}
	// Keep every observation and the original stable duration. Real queue,
	// service-manager, session and PID checks run on each modelled second;
	// TestEnrollmentWaitRequiresStableManagerAndSupportsPoliteWaiting covers
	// the stable-window boundary and rejects restart loops and stale PIDs.
	return myce.WaitEnrollment(ctx, config, client, options)
}
