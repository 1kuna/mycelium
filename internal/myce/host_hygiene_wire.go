package myce

import "slices"

const hostHygieneProtocol = "host-hygiene-v1"
const hostCollectionProtocol = "worker-collection-v1"
const hostMountProgressProtocol = "mount-progress-v1"

// Negotiated by the serialized check-in lane. Older queues strictly decode
// requests, so local evidence stays durable without sending unknown fields.
type workerHygieneWire struct {
	machine, session       string
	supported              bool
	collectionSupported    bool
	mountProgressSupported bool
}

func (w Worker) hygieneCheckin(c Checkin) Checkin {
	if r := w.hygieneWire; r != nil {
		if r.machine != c.Machine.ID || r.session != c.Machine.Session {
			*r = workerHygieneWire{machine: c.Machine.ID, session: c.Machine.Session}
		}
		if !r.collectionSupported {
			c.Machine.HygieneCollection = nil
		}
		if r.supported {
			if !r.mountProgressSupported {
				c = legacyMountProgressCheckin(c)
			}
			return c
		}
	}
	return w.legacyHygieneCheckin(c)
}

// A v1 queue understands host health, but strictly rejects the added operation
// evidence. Project only those fields away without mutating durable local data.
func legacyMountProgressCheckin(c Checkin) Checkin {
	if c.Machine.HostHealth == nil {
		return c
	}
	project := func(samples []HostHealthSample) []HostHealthSample {
		samples = slices.Clone(samples)
		for i := range samples {
			samples[i].Mounts = slices.Clone(samples[i].Mounts)
			for j := range samples[i].Mounts {
				m := &samples[i].Mounts[j]
				m.OperationAgeSeconds, m.SlowThresholdSeconds, m.StallAllowanceSeconds = 0, 0, 0
			}
		}
		return samples
	}
	h := *c.Machine.HostHealth
	h.History, h.Baseline = project(h.History), project(h.Baseline)
	c.Machine.HostHealth = &h
	return c
}

func hasMountProgressWireFields(c Checkin) bool {
	if c.Machine.HostHealth != nil {
		for _, samples := range [][]HostHealthSample{c.Machine.HostHealth.History, c.Machine.HostHealth.Baseline} {
			for _, s := range samples {
				for _, m := range s.Mounts {
					if m.OperationAgeSeconds != 0 || m.SlowThresholdSeconds != 0 || m.StallAllowanceSeconds != 0 {
						return true
					}
				}
			}
		}
	}
	return false
}

func (w Worker) legacyHygieneCheckin(c Checkin) Checkin {
	if c.Machine.HostHealth != nil && c.Machine.HostHealth.DrainReason != "" {
		// The old leaving signal withholds admission without releasing custody.
		c.Machine.Leaving = true
	}
	c.Machine.HostHealth = nil
	c.Machine.HygieneCollection = nil
	return c
}

func (w Worker) acceptHygieneReply(reply Reply) {
	if w.hygieneWire != nil {
		w.hygieneWire.supported = reply.HostHygieneProtocol == hostHygieneProtocol
		w.hygieneWire.collectionSupported = reply.HostCollectionProtocol == hostCollectionProtocol
		w.hygieneWire.mountProgressSupported = reply.HostMountProgressProtocol == hostMountProgressProtocol
	}
}

func hasHygieneWireFields(c Checkin) bool {
	return c.Machine.HostHealth != nil || c.Machine.HygieneCollection != nil
}
