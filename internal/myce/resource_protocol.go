package myce

import (
	"slices"
	"sync/atomic"
)

const resourceProtocol = "attempt-resources-v1"
const resourceProtocolHeader = "X-Myce-Resource-Telemetry"
const cpuPressureProtocolHeader = "X-Myce-CPU-Pressure"
const cpuPressureProtocol = "cpu-pressure-v1"

// Shared by every worker lane, including saved-result delivery. A new session
// starts without support; headers negotiate without unknown JSON on old queues.
type resourceWire struct{ supported, cpuPressure, executionEvidence atomic.Bool }

func resourceWireInput(in any, supported bool, cpuPressure ...bool) (any, bool) {
	if supported && len(cpuPressure) > 0 && cpuPressure[0] {
		return in, hasResourceFields(in)
	}
	resources := func(r *AttemptResources) *AttemptResources {
		if !supported {
			return nil
		}
		if r == nil {
			return nil
		}
		copy := *r
		copy.CPU.Pressure = nil
		return &copy
	}
	switch c := in.(type) {
	case *Checkin:
		if c != nil {
			return resourceWireInput(*c, supported)
		}
	case *Result:
		if c != nil {
			return resourceWireInput(*c, supported)
		}
	case *LogChunk:
		if c != nil {
			return resourceWireInput(*c, supported)
		}
	case Checkin:
		if !supported {
			c.Machine.IOPressure = nil
		}
		c.Attempts = slices.Clone(c.Attempts)
		for i := range c.Attempts {
			c.Attempts[i].Resources = resources(c.Attempts[i].Resources)
		}
		return c, hasResourceFields(c)
	case Result:
		c.Resources = resources(c.Resources)
		return c, hasResourceFields(c)
	case LogChunk:
		c.Resources = resources(c.Resources)
		return c, hasResourceFields(c)
	}
	return in, false
}
func hasResourceFields(in any) bool {
	switch c := in.(type) {
	case *Checkin:
		return c != nil && hasResourceFields(*c)
	case *Result:
		return c != nil && c.Resources != nil
	case *LogChunk:
		return c != nil && c.Resources != nil
	case Checkin:
		if c.Machine.IOPressure != nil {
			return true
		}
		for _, a := range c.Attempts {
			if a.Resources != nil {
				return true
			}
		}
	case Result:
		return c.Resources != nil
	case LogChunk:
		return c.Resources != nil
	}
	return false
}
