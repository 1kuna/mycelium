package myce

import "slices"

const processTelemetryHeader = "X-Myce-Process-Telemetry"
const processTelemetryProtocol = "attempt-processes-v1"

// Process observations are additive to the CPU/I/O contract. Negotiate them
// separately so a new worker can still deliver CPU/I/O to a strict v1 queue.
func processTelemetryInput(in any, supported bool) any {
	if supported {
		return in
	}
	strip := func(r *AttemptResources) *AttemptResources {
		if r == nil {
			return nil
		}
		copy := *r
		copy.ProcessInitial, copy.ProcessPeak = nil, nil
		return &copy
	}
	switch c := in.(type) {
	case *Checkin:
		if c != nil {
			return processTelemetryInput(*c, false)
		}
	case *Result:
		if c != nil {
			return processTelemetryInput(*c, false)
		}
	case *LogChunk:
		if c != nil {
			return processTelemetryInput(*c, false)
		}
	case Checkin:
		c.Attempts = slices.Clone(c.Attempts)
		for i := range c.Attempts {
			c.Attempts[i].Resources = strip(c.Attempts[i].Resources)
		}
		return c
	case Result:
		c.Resources = strip(c.Resources)
		return c
	case LogChunk:
		c.Resources = strip(c.Resources)
		return c
	}
	return in
}
