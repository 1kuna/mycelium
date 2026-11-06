package myce

const hostHygieneProtocol = "host-hygiene-v1"
const hostCollectionProtocol = "worker-collection-v1"

// Negotiated by the serialized check-in lane. Older queues strictly decode
// requests, so local evidence stays durable without sending unknown fields.
type workerHygieneWire struct {
	machine, session    string
	probeSupported      bool
	supported           bool
	collectionSupported bool
}

func (w Worker) hygieneCheckin(c Checkin) Checkin {
	if r := w.hygieneWire; r != nil {
		if r.machine != c.Machine.ID || r.session != c.Machine.Session {
			*r = workerHygieneWire{machine: c.Machine.ID, session: c.Machine.Session}
		}
		if !r.probeSupported {
			c = legacyHostProbeCheckin(c)
		}
		if !r.collectionSupported {
			c.Machine.HygieneCollection = nil
		}
		if r.supported {
			return c
		}
	}
	return w.legacyHygieneCheckin(c)
}

func (w Worker) legacyHygieneCheckin(c Checkin) Checkin {
	c = legacyHostProbeCheckin(c)
	if c.Machine.HostHealth != nil && c.Machine.HostHealth.DrainReason != "" {
		// Availability is reversible; Leaving is a terminal worker lifecycle signal.
		c = withholdLegacyHostAdmission(c)
	}
	c.Machine.HostHealth = nil
	c.Machine.HygieneCollection = nil
	return c
}

func (w Worker) acceptHygieneReply(reply Reply) {
	if w.hygieneWire != nil {
		w.hygieneWire.probeSupported = reply.HostProbeProtocol == hostProbeProtocol
		w.hygieneWire.supported = reply.HostHygieneProtocol == hostHygieneProtocol
		w.hygieneWire.collectionSupported = reply.HostCollectionProtocol == hostCollectionProtocol
	}
}

func hasHygieneWireFields(c Checkin) bool {
	return c.Machine.HostHealth != nil || c.Machine.HygieneCollection != nil || hasHostProbeFields(c)
}

// Copy the nested records before stripping negotiated fields. Local freshness
// and the exact attempt/session remain authoritative during a queue downgrade.
func legacyHostProbeCheckin(c Checkin) Checkin {
	if h := c.Machine.HostHealth; h != nil {
		copy := *h
		if !hostHealthEvidenceValid(h, 0) {
			copy.DrainReason = "host_probe_unknown"
			c = withholdLegacyHostAdmission(c)
		}
		copy.Probe = nil
		c.Machine.HostHealth = &copy
	}
	if p := c.Machine.SharedHostPressure; p != nil {
		copy := *p
		copy.Probe, copy.Learning = nil, nil
		c.Machine.SharedHostPressure = &copy
	}
	if profile := c.Machine.CapabilityProfile; profile != nil && profile.Availability != nil {
		copy := *profile
		a := *profile.Availability
		a.Probe = nil
		a.AgeSeconds = 0
		copy.Availability = &a
		c.Machine.CapabilityProfile = &copy
	}
	return c
}
func hasHostProbeFields(c Checkin) bool {
	p := c.Machine.SharedHostPressure
	a := machineAvailability(&c.Machine)
	h := c.Machine.HostHealth
	return p != nil && (p.Probe != nil || p.Learning != nil) || a != nil && a.Probe != nil || h != nil && h.Probe != nil
}

func withholdLegacyHostAdmission(c Checkin) Checkin {
	profile := CapabilityProfile{}
	if c.Machine.CapabilityProfile != nil {
		profile = *c.Machine.CapabilityProfile
	}
	a := Availability{Mode: "dedicated"}
	if profile.Availability != nil {
		a = *profile.Availability
	}
	a.Available, a.Reason = false, "host health observation unavailable"
	profile.Availability = &a
	c.Machine.CapabilityProfile = &profile
	return c
}
