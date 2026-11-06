package myce

import (
	"slices"
	"sync/atomic"
)

const resourceProtocol = "attempt-resources-v1"
const resourceProtocolHeader = "X-Myce-Resource-Telemetry"

// Shared by every worker lane, including saved-result delivery. A new session
// starts without support; headers negotiate without unknown JSON on old queues.
type resourceWire struct{ supported atomic.Bool }

func resourceWireInput(in any, supported bool) (any, bool) {
	if supported {
		return in, hasResourceFields(in)
	}
	switch c := in.(type) {
	case Checkin:
		c.Machine.IOPressure = nil
		c.Attempts = slices.Clone(c.Attempts)
		for i := range c.Attempts {
			c.Attempts[i].Resources = nil
		}
		return c, false
	case Result:
		c.Resources = nil
		return c, false
	case LogChunk:
		c.Resources = nil
		return c, false
	}
	return in, false
}
func hasResourceFields(in any) bool {
	switch c := in.(type) {
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
