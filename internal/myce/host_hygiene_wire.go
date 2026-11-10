package myce

import "slices"

const hostHygieneProtocol = "host-hygiene-v1"
const hostCollectionProtocol = "worker-collection-v1"
const hostMountProgressProtocol = "mount-progress-v1"
const workerDiskBudgetProtocol = "worker-disk-budget-v1"

// Negotiated by the serialized check-in lane. Older queues strictly decode
// requests, so local evidence stays durable without sending unknown fields.
type workerHygieneWire struct {
	machine, session       string
	probeSupported         bool
	supported              bool
	collectionSupported    bool
	mountProgressSupported bool
	diskBudgetSupported    bool
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
		if !r.diskBudgetSupported {
			c = legacyDiskBudgetCheckin(c)
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
		w.hygieneWire.mountProgressSupported = reply.HostMountProgressProtocol == hostMountProgressProtocol
		w.hygieneWire.diskBudgetSupported = reply.WorkerDiskBudgetProtocol == workerDiskBudgetProtocol
	}
}

func hasHygieneWireFields(c Checkin) bool {
	return c.Machine.HostHealth != nil || c.Machine.HygieneCollection != nil || hasHostProbeFields(c)
}

func legacyDiskBudgetCheckin(c Checkin) Checkin {
	if c.Machine.HygieneCollection != nil {
		r := *c.Machine.HygieneCollection
		r.DiskBudget, r.Scratch, r.CompilerWarning = nil, nil, ""
		r.UnmanagedCompilerBytes = 0
		c.Machine.HygieneCollection = &r
	}
	return c
}

func hasDiskBudgetWireFields(c Checkin) bool {
	r := c.Machine.HygieneCollection
	return r != nil && (len(r.DiskBudget) > 0 || r.Scratch != nil || r.CompilerWarning != "" || r.UnmanagedCompilerBytes != 0)
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
