package myce

import "slices"

const faultEvidenceProtocol = "fault-evidence-v1"
const faultEvidenceHeader = "X-Myce-Fault-Evidence"

// Worker client capability negotiation is additive. Saved results,
// log chunks and Checkin all preserve their durable local evidence when an
// older queue cannot decode the additive fields.
func faultWireInput(in any, supported bool) (any, bool) {
	switch c := in.(type) {
	case *Checkin:
		if c != nil {
			return faultWireInput(*c, supported)
		}
	case *Result:
		if c != nil {
			return faultWireInput(*c, supported)
		}
	case *LogChunk:
		if c != nil {
			return faultWireInput(*c, supported)
		}
	case Checkin:
		present := false
		for _, d := range c.Machine.Devices {
			present = present || len(d.FaultEvidence) > 0
		}
		if supported {
			return c, present
		}
		c.Machine.Devices = slices.Clone(c.Machine.Devices)
		for i := range c.Machine.Devices {
			c.Machine.Devices[i].FaultEvidence = nil
		}
		return c, false
	case Result:
		if supported {
			return c, c.SignalEvidence != nil
		}
		c.SignalEvidence = nil
		return c, false
	case LogChunk:
		if supported {
			return c, c.SignalEvidence != nil
		}
		c.SignalEvidence = nil
		return c, false
	}
	return in, false
}
