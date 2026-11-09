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
	source := options.Clock
	clock := &enrollmentFixtureClock{now: source.Now()}
	options.Clock = clock
	pause := options.Wait
	if pause == nil {
		pause = waitPause
	}
	healthy := false
	observed := options.HealthObserved
	options.HealthObserved = func(valid bool) {
		healthy = valid
		if observed != nil {
			observed(valid)
		}
	}
	options.Wait = func(ctx context.Context, duration time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Only the production verdict, after parsing and receipt PID validation,
		// permits acceleration. Startup and unhealthy services retain real time.
		if !healthy {
			before := source.Now()
			if err := pause(ctx, duration); err != nil {
				return err
			}
			clock.now = clock.now.Add(source.Now().Sub(before))
		} else {
			clock.now = clock.now.Add(duration)
		}
		return ctx.Err()
	}
	// Keep every observation and the original stable duration. Real queue,
	// service-manager, session and PID checks run on each modelled second;
	// TestEnrollmentWaitRequiresStableManagerAndSupportsPoliteWaiting covers
	// the stable-window boundary and rejects restart loops and stale PIDs.
	return myce.WaitEnrollment(ctx, config, client, options)
}
