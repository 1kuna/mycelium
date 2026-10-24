package myce

import (
	"fmt"
	"slices"
	"time"
)

// ReferenceSweep runs one extension-owned command at supported concurrency.
// Counts describe internal work in that command, never additional GPU attempts.
type ReferenceSweep struct {
	Command        []string `json:"command" yaml:"command"`
	Counts         []int    `json:"counts" yaml:"counts"`
	MaxConcurrency int      `json:"max_concurrency" yaml:"max_concurrency"`
}

func (s ReferenceSweep) valid() bool {
	if len(s.Command) == 0 || len(s.Counts) == 0 || s.Counts[0] != 1 || s.MaxConcurrency < 1 || s.MaxConcurrency > 64 {
		return false
	}
	last := 0
	for _, c := range s.Counts {
		if c <= last || c > s.MaxConcurrency || c&(c-1) != 0 {
			return false
		}
		last = c
	}
	return true
}
func ewma(old, sample float64) float64 {
	if !validRate(old) {
		return sample
	}
	return .8*old + .2*sample
}

func observationAllowance(m *Machine) string {
	power, mode := "", ""
	if a := machineAvailability(m); a != nil {
		power, mode = a.Signals.Power, a.Mode
	}
	return identityHash([]any{m.CPUReserveCores, m.EffectiveCPUCores, m.EffectiveHostMemoryBudgetGB, m.TotalMemoryGB, m.ReservedMemoryGB, m.UnifiedMemory, power, mode})
}

func curveKey(e Extension, m *Machine, work string) (*ObservationKey, string) {
	v := e.variant(machineTarget(m))
	if v == nil {
		return nil, "recipe target unavailable"
	}
	ref, ok := v.References[work]
	if !ok {
		return nil, "reference unavailable"
	}
	needs := v.Needs
	s := Spec{Needs: needs}
	s.Needs.Platform = Platform{Required: []string{m.GPU}}
	if err := normalizeResources(&s); err != nil {
		return nil, "resource floors unavailable"
	}
	component, hardware := "cpu", m.ExtensionFingerprint
	if m.GPU != "cpu" {
		component = m.Device
		hardware = ""
		for _, d := range m.Devices {
			if d.ID == component {
				hardware = d.Identity
			}
		}
	}
	if hardware == "" || m.ExtensionFingerprint == "" {
		return nil, "component identity unavailable"
	}
	qualified := false
	for _, r := range m.ExtensionReports {
		if r.Work == work && extensionQualificationReason(e, r, m) == "" {
			qualified = true
		}
	}
	if !qualified {
		return nil, "qualification unavailable"
	}
	return referenceObservationKey(e, m, ref, work, s.Needs, nil, nil, component, hardware)
}

// Queue mutation only: retain historical evidence with an explicit reason.
func (q *Queue) invalidateWorkCurves() {
	for id, p := range q.s.WorkObservations {
		if p.StaleReason != "" {
			continue
		}
		reason := "component unavailable"
		for _, owner := range q.s.Machines {
			if hostID(owner) != p.Key.Host || owner.Gone || q.unreported[owner.ID] {
				continue
			}
			for _, m := range q.machineTargets(owner) {
				component := "cpu"
				if m.GPU != "cpu" {
					component = m.Device
				}
				if component != p.Key.Component {
					continue
				}
				reason = "recipe or reference changed"
				for _, e := range q.s.Extensions {
					if e.Digest != p.Key.ExtensionDigest {
						continue
					}
					reason = curveStaleReason(p, e, m, q.clock.Now())
					break
				}
			}
		}
		if reason != "" {
			p.StaleReason = reason
			q.s.WorkObservations[id] = p
		}
	}
}

// Only exact, started benchmark cleanup acknowledgements publish reference points.
func (q *Queue) acceptCurveReports(c Checkin, m *Machine) {
	launch, ok := q.s.PreparationPlans[c.BenchmarkDone]
	if !ok || !launch.Started || launch.Kind != "benchmark" || launch.Machine != m.ID || launch.Session != m.Session {
		return
	}
	for _, p := range c.CurveReports {
		if p.Source != "idle reference" || p.Key.Host != hostID(m) || p.Key.BatchSize != 1 {
			continue
		}
		for _, target := range q.machineTargets(m) {
			for _, e := range q.s.Extensions {
				if e.Digest != p.Key.ExtensionDigest {
					continue
				}
				v := e.variant(machineTarget(target))
				if v == nil {
					continue
				}
				sweep, ok := v.Sweeps[p.Key.Work]
				if !ok || !slices.Contains(sweep.Counts, p.Key.Concurrency) || p.Key.Concurrency > sweep.MaxConcurrency {
					continue
				}
				k, _ := curveKey(e, target, p.Key.Work)
				if k == nil {
					continue
				}
				k.Concurrency = p.Key.Concurrency
				if *k != p.Key || !p.Forbidden && !curveFits(target, v.Needs, p.Key.Concurrency) {
					continue
				}
				if !p.Forbidden && (!validRate(p.Rate) || !validRate(p.LatencySeconds) || !validRate(p.Units) || !validRate(p.Seconds)) {
					continue
				}
				if !validMemoryValue(p.HostMemoryGB) || !validMemoryValue(p.DeviceMemoryGB) || !validMemoryValue(p.IncrementalHostMemoryGB) || !validMemoryValue(p.IncrementalDeviceMemoryGB) {
					continue
				}
				id := identityHash(k)
				old := q.s.WorkObservations[id]
				if p.SampleID == "" || p.SampleID == old.SampleID {
					continue
				}
				if old.StaleReason != "" || q.clock.Now().Sub(old.RecordedAt) > 30*24*time.Hour {
					old.Rate, old.LatencySeconds = 0, 0
				}
				p.Samples = old.Samples
				if !p.Forbidden {
					p.Samples++
					p.Rate = ewma(old.Rate, p.Rate)
					p.LatencySeconds = ewma(old.LatencySeconds, p.LatencySeconds)
				}
				p.Forbidden = p.Forbidden || old.Forbidden
				p.HostMemoryGB = max(old.HostMemoryGB, p.HostMemoryGB)
				p.DeviceMemoryGB = max(old.DeviceMemoryGB, p.DeviceMemoryGB)
				p.IncrementalHostMemoryGB = max(old.IncrementalHostMemoryGB, p.IncrementalHostMemoryGB)
				p.IncrementalDeviceMemoryGB = max(old.IncrementalDeviceMemoryGB, p.IncrementalDeviceMemoryGB)
				if p.RecordedAt.IsZero() || q.clock.Now().Sub(p.RecordedAt) > 30*24*time.Hour {
					p.StaleReason = "measurement older than 30 days"
				}
				p.Machine = m.ID
				p.Session = m.Session
				p.Job = launch.Attempt
				if q.s.WorkObservations == nil {
					q.s.WorkObservations = map[string]WorkObservation{}
				}
				q.s.WorkObservations[id] = p
			}
		}
	}
}

func (p WorkObservation) Summary() string {
	state := "measured"
	if p.StaleReason != "" {
		state = "stale: " + p.StaleReason
	}
	if p.Forbidden {
		state = "forbidden: OOM or memory bound exceeded"
		if p.StaleReason != "" {
			state += "; stale: " + p.StaleReason
		}
	}
	rate := "unknown"
	if state == "measured" && validRate(p.Rate) {
		rate = fmt.Sprintf("%.2f %s/s", p.Rate, p.Key.Reference.Unit)
	}
	return fmt.Sprintf("%s %s/%s c=%d: %s; %s; latency %.2fs; incremental host/device %.2f/%.2f GiB; peak %.2f/%.2f GiB; %d samples", p.Key.Work, p.Key.Host, p.Key.Component, p.Key.Concurrency, rate, state, p.LatencySeconds, p.IncrementalHostMemoryGB, p.IncrementalDeviceMemoryGB, p.HostMemoryGB, p.DeviceMemoryGB, p.Samples)
}

// Failure does not learn speed, but exact-attempt OOM evidence forbids its point.
func (q *Queue) recordWorkOOM(j *Job, r Result) {
	var key *ObservationKey
	if j.Timing != nil {
		key = j.Timing.Key
	}
	if key == nil && j.Spec.Needs.Memory == "auto" {
		provenance := *j
		provenance.Attempt = 0 // Resumed OOMs still establish a conservative floor.
		key, _ = q.workObservationKey(&provenance)
	}
	if key == nil {
		return
	}
	k := *key
	id := identityHash(k)
	p := q.s.WorkObservations[id]
	p.Key = k
	p.Forbidden = true
	p.RecordedAt = q.clock.Now()
	p.Job = j.ID
	p.Attempt = r.Attempt
	p.Machine = r.Machine
	p.Session = r.Session
	if j.Spec.Needs.Memory == "auto" {
		// OOM cannot provide a complete peak, but it can raise a lower bound.
		// Persist it independently of this job's retry so later jobs recover
		// from an underestimated accepted sample too, including a final OOM.
		host, device := j.Spec.Needs.HostMemoryGB*1.25, j.Spec.Needs.MemoryGB*1.25
		p.MemoryPeak = mergeMemoryPeaks(p.MemoryPeak, &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device})
		p.MemoryRecordedAt = q.clock.Now()
		p.MemoryConfiguration = memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
		p.MemoryLayout = "separate"
		if m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices); m != nil && m.UnifiedMemory && !j.Spec.Needs.cpuOnly() {
			p.MemoryLayout = "unified"
		}
		if p.Source != "accepted work" {
			p.Source = "OOM lower bound"
		}
	}
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	q.s.WorkObservations[id] = p
}

func curveStaleReason(p WorkObservation, e Extension, m *Machine, now time.Time) string {
	if m.ExtensionFingerprint != p.Key.Fingerprint {
		return "driver or backend fingerprint changed"
	}
	k, why := curveKey(e, m, p.Key.Work)
	if k == nil {
		return why
	}
	switch {
	case k.Hardware != p.Key.Hardware:
		return "hardware changed"
	case k.Environment != p.Key.Environment || k.Target != p.Key.Target:
		return "environment or target changed"
	case k.Reference != p.Key.Reference:
		return "reference changed"
	case k.Allowance != p.Key.Allowance:
		return "resource allowance or power mode changed"
	case now.Sub(p.RecordedAt) > 30*24*time.Hour:
		return "measurement older than 30 days"
	}
	return ""
}
