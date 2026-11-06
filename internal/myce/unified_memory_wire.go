package myce

import "slices"

const unifiedMemoryProtocol = "unified-attempt-memory-v1"
const probeMemoryProtocol = "cpu-probe-memory-v1"

type workerUnifiedMemoryWire struct {
	machine, session string
	supported        bool
	probeSupported   bool
}

func (w Worker) unifiedMemoryCheckin(c Checkin) Checkin {
	if r := w.unifiedMemoryWire; r != nil {
		if r.machine != c.Machine.ID || r.session != c.Machine.Session {
			*r = workerUnifiedMemoryWire{machine: c.Machine.ID, session: c.Machine.Session}
		}
		if r.supported {
			if r.probeSupported {
				return c
			}
			return legacyProbeMemoryCheckin(c)
		}
	}
	return legacyUnifiedMemoryCheckin(c)
}

// Older queues strictly decode nested samples. They must never interpret an
// ambiguous estimate as a direct device counter. Withhold unified CUDA capacity
// while such evidence is live. Leaving requests stop running jobs, so use the
// existing device-unavailable admission fields and preserve their custody.
func legacyUnifiedMemoryCheckin(c Checkin) Checkin {
	c = legacyProbeMemoryCheckin(c)
	c.Attempts = slices.Clone(c.Attempts)
	for i, r := range c.Attempts {
		if r.CurrentHostMemory == nil {
			continue
		}
		s := *r.CurrentHostMemory
		if s.DeviceSource == deviceUnifiedResidentSource || s.DeviceExclusive {
			c.Machine.Devices = slices.Clone(c.Machine.Devices)
			for n := range c.Machine.Devices {
				d := &c.Machine.Devices[n]
				if d.Platform == "cuda" && d.UnifiedMemory {
					d.FreeMemoryGB = 0
					if d.Reason == "" {
						d.Reason = deviceUnifiedUncertain
					}
				}
			}
			if c.Machine.GPU == "cuda" {
				c.Machine.FreeMemoryGB = 0
				if c.Machine.MemoryError == "" {
					c.Machine.MemoryError = deviceUnifiedUncertain
				}
			}
			s.DeviceGB, s.DeviceGrowthGB, s.DeviceAgeSeconds = nil, 0, 0
		}
		s.DeviceSource, s.DeviceExclusive, s.DeviceGrowthRateGB = "", false, 0
		c.Attempts[i].CurrentHostMemory = &s
	}
	return c
}

func hasUnifiedMemoryWireFields(c Checkin) bool {
	for _, r := range c.Attempts {
		if s := r.CurrentHostMemory; s != nil && (s.DeviceSource != "" || s.DeviceExclusive || s.DeviceGrowthRateGB != 0 || s.GrowthObserved || s.GrowthRateGB != 0 || s.FootprintGB != nil) {
			return true
		}
	}
	return false
}

// Strict older decoders cannot accept new nested telemetry. Negotiate probe
// evidence independently so v1 GPU sharing remains available during an upgrade.
func legacyProbeMemoryCheckin(c Checkin) Checkin {
	c.Attempts = slices.Clone(c.Attempts)
	for i, r := range c.Attempts {
		if r.CurrentHostMemory != nil {
			s := *r.CurrentHostMemory
			s.GrowthObserved, s.FootprintGB = false, nil
			s.GrowthRateGB = 0
			c.Attempts[i].CurrentHostMemory = &s
		}
	}
	return c
}
