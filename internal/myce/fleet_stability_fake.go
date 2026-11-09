//go:build myce_test

package myce

import (
	"context"
	"time"
)

// Synthetic release binaries model only service-stability time. They retain
// real queue/worker clocks, proofs, manager commands, processes and transport.
// This state is private and is never serialized as fleet authority.
type fleetStability struct {
	model          *fleetStabilityModelClock
	managerHealthy bool
}

type fleetStabilityModelClock struct{ now time.Time }

func (c *fleetStabilityModelClock) Now() time.Time { return c.now }

func (s *fleetStability) clock(source Clock, reset bool) Clock {
	if reset || s.model == nil {
		s.model = &fleetStabilityModelClock{now: source.Now()}
	} else {
		// Each successful real observation models the next second. Keep the
		// original 30-second window and all intermediate identity checks.
		s.model.now = s.model.now.Add(time.Second)
	}
	return s.model
}

func (s *fleetStability) managerOptions(options EnrollmentWait) EnrollmentWait {
	// Respect explicitly injected clocks in existing focused fixtures. The
	// finite helper's ordinary entry point supplies no health clock.
	if options.Clock != nil {
		return options
	}
	source := RealClock{}
	s.model = &fleetStabilityModelClock{now: source.Now()}
	options.Clock = s.model
	pause := options.Wait
	if pause == nil {
		pause = enrollmentPause
	}
	options.Wait = func(ctx context.Context, duration time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Startup must observe a real healthy, replacement manager PID before
		// dropping real pauses. The unchanged health parser decides readiness.
		if !s.managerHealthy {
			before := source.Now()
			if err := pause(ctx, duration); err != nil {
				return err
			}
			s.model.now = s.model.now.Add(source.Now().Sub(before))
		} else {
			s.model.now = s.model.now.Add(duration)
		}
		return ctx.Err()
	}
	return options
}

func (s *fleetStability) observeManager(healthy bool) { s.managerHealthy = healthy }
