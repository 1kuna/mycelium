package myce

import "slices"

const unifiedMemoryProtocol = "unified-attempt-memory-v1"

type workerUnifiedMemoryWire struct {
	machine, session string
	supported        bool
}

func (w Worker) unifiedMemoryCheckin(c Checkin) Checkin {
	if r := w.unifiedMemoryWire; r != nil {
		if r.machine != c.Machine.ID || r.session != c.Machine.Session {
			*r = workerUnifiedMemoryWire{machine: c.Machine.ID, session: c.Machine.Session}
		}
		if r.supported {
			return c
		}
	}
	return legacyUnifiedMemoryCheckin(c)
}

// Older queues strictly decode nested samples. They must never interpret an
// ambiguous estimate as a direct device counter. Drain admission while such
// evidence is live, preserving exact attempt custody and local sampling.
func legacyUnifiedMemoryCheckin(c Checkin) Checkin {
	c.Attempts = slices.Clone(c.Attempts)
	for i, r := range c.Attempts {
		if r.CurrentHostMemory == nil {
			continue
		}
		s := *r.CurrentHostMemory
		if s.DeviceSource == deviceUnifiedResidentSource || s.DeviceExclusive {
			c.Machine.Leaving = true
			s.DeviceGB, s.DeviceGrowthGB, s.DeviceAgeSeconds = nil, 0, 0
		}
		s.DeviceSource, s.DeviceExclusive, s.DeviceGrowthRateGB = "", false, 0
		c.Attempts[i].CurrentHostMemory = &s
	}
	return c
}

func hasUnifiedMemoryWireFields(c Checkin) bool {
	for _, r := range c.Attempts {
		if s := r.CurrentHostMemory; s != nil && (s.DeviceSource != "" || s.DeviceExclusive || s.DeviceGrowthRateGB != 0) {
			return true
		}
	}
	return false
}
