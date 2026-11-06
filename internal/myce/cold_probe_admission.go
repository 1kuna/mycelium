package myce

import "time"

// Keep the same bounded sensor history as host health. This is a storage bound,
// not a memory allowance: the reserve is the largest measured adjacent drop.
type hostLaunchMemory struct {
	Machine   string               `json:"machine"`
	Session   string               `json:"session"`
	Samples   []hostHeadroomSample `json:"samples"`
	RefusalGB float64              `json:"refusal_gb,omitempty"`
}

type hostHeadroomSample struct {
	At          time.Time `json:"at"`
	AvailableGB float64   `json:"available_gb"`
}

func (q *Queue) recordHostLaunchMemory(m, old *Machine) {
	if !m.HostMemoryMeasured || m.HostMemoryError != "" {
		return
	}
	if q.s.HostLaunchMemory == nil {
		q.s.HostLaunchMemory = map[string]*hostLaunchMemory{}
	}
	host := hostID(m)
	h := q.s.HostLaunchMemory[host]
	if h == nil || h.Machine != m.ID || h.Session != m.Session {
		h = &hostLaunchMemory{Machine: m.ID, Session: m.Session}
		q.s.HostLaunchMemory[host] = h
		// Existing queues already retain the previous check-in. Seed the first
		// pair on upgrade, without making each new work type fail to learn it.
		if old != nil && !old.Gone && hostID(old) == host && old.Session == m.Session && old.HostMemoryMeasured && old.HostMemoryError == "" {
			h.Samples = append(h.Samples, hostHeadroomSample{old.LastSeen, old.HostAvailableMemoryGB})
		}
	}
	now := q.clock.Now()
	if n := len(h.Samples); n > 0 && !now.After(h.Samples[n-1].At) {
		return // Repeated/future timestamps cannot establish a sampling interval.
	}
	h.Samples = append(h.Samples, hostHeadroomSample{now, m.HostAvailableMemoryGB})
	if len(h.Samples) > hostHealthHistorySize {
		h.Samples = append([]hostHeadroomSample(nil), h.Samples[len(h.Samples)-hostHealthHistorySize:]...)
	}
}

func (q *Queue) coldProbeDriftMarginGB(m *Machine) float64 {
	margin := exploratoryDriftMarginGB(m)
	h := q.s.HostLaunchMemory[hostID(m)]
	if h == nil || h.Machine != m.ID || h.Session != m.Session {
		return margin
	}
	margin = max(margin, h.RefusalGB)
	for i := 1; i < len(h.Samples); i++ {
		margin = max(margin, h.Samples[i-1].AvailableGB-h.Samples[i].AvailableGB+minimumHostMemoryGB)
	}
	return margin
}

// Learn launch error, never payload demand, from an exact unstarted attempt.
// A failed strict comparison measured error beyond the margin already reserved;
// add that deficit so the next cold ceiling does not chase the same boundary.
// Worker checks, member/OOM floors and each assigned ceiling remain unchanged.
func (q *Queue) recordColdProbeRefusal(j *Job, r Result) {
	if j.StartedAt != nil || r.OutOfMemory || r.ReleaseOnly || !provenPrelaunchRefusal(j, r) || j.MemoryEnvelope == nil || j.MemoryEnvelope.Basis != "exploratory" {
		return
	}
	m := q.s.Machines[r.Machine]
	c := r.ExecutionCause
	if m == nil || m.Session != r.Session || c.Predicate != "memory_capacity" || c.Admission == nil {
		return
	}
	d := c.Admission
	if d.Unit != "GiB" || (d.Subcheck != "live_host_ram" && d.Subcheck != "live_host_growth") || d.Unavailable || d.Available == nil || d.Required <= *d.Available || d.EnvelopeHostGB != j.MemoryEnvelope.HostGB || d.EnvelopeDeviceGB != j.MemoryEnvelope.DeviceGB || d.EnvelopeSource != j.MemoryEnvelope.Source {
		return
	}
	q.recordHostLaunchMemory(m, nil)
	h := q.s.HostLaunchMemory[hostID(m)]
	if h != nil {
		margin := q.coldProbeDriftMarginGB(m) + d.Required - *d.Available + minimumHostMemoryGB
		if validMemoryValue(margin) {
			h.RefusalGB = margin
		}
	}
}
