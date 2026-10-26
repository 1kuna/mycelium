package myce

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// ReferenceSweep runs one extension-owned command at supported concurrency.
// Counts describe internal work in that command, never additional GPU attempts.
type ReferenceSweep struct {
	Run            []string `json:"run,omitempty" yaml:"run,omitempty"`
	ServerDigest   string   `json:"server_digest,omitempty" yaml:"server_digest,omitempty"`
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
	k, why := curveReferenceKey(e, m, work)
	if k == nil {
		return nil, why
	}
	for _, r := range m.ExtensionReports {
		if r.Work == work && extensionQualificationReason(e, r, m) == "" {
			return k, ""
		}
	}
	return nil, "qualification unavailable"
}

func curveReferenceKey(e Extension, m *Machine, work string) (*ObservationKey, string) {
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
				if strings.HasPrefix(p.Key.Work, "profile:") {
					reason = profileMemoryStaleReason(p, m)
					continue
				}
				for _, e := range q.s.Extensions {
					if e.Digest != p.Key.ExtensionDigest {
						continue
					}
					reason = curveStaleReason(p, e, m)
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

func profileMemoryStaleReason(p WorkObservation, m *Machine) string {
	if m.ExtensionFingerprint != p.Key.Fingerprint {
		return "driver or backend fingerprint changed"
	}
	if m.GPU != "cpu" {
		for _, d := range m.Devices {
			if d.ID == m.Device && d.Identity != p.Key.Hardware {
				return "hardware changed"
			}
		}
	}
	if machineTarget(m) != p.Key.Target {
		return "environment or target changed"
	}
	if observationAllowance(m) != p.Key.Allowance {
		return "resource allowance or power mode changed"
	}
	return ""
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
				// Memory evidence comes only from accepted attempts, never a report.
				p.MemoryPeak, p.MemoryRecordedAt, p.MemoryConfiguration, p.MemoryLayout = old.MemoryPeak, old.MemoryRecordedAt, old.MemoryConfiguration, old.MemoryLayout
				if old.StaleReason != "" {
					p.MemoryPeak = nil
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

func (p WorkObservation) Summary() string { return p.summary("measured") }

// SummaryAt labels throughput past its reader age as expired for display only;
// stored StaleReason still records identity changes alone.
func (p WorkObservation) SummaryAt(now time.Time) string {
	if now.Before(p.RecordedAt) || now.Sub(p.RecordedAt) > workCurveMaxAge {
		return p.summary("throughput expired")
	}
	return p.Summary()
}

func (p WorkObservation) summary(state string) string {
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
	// Accepted memory_peak covers memory-only observations; legacy usage maxima stay the fallback.
	hostPeak, devicePeak := p.HostMemoryGB, p.DeviceMemoryGB
	if p.MemoryPeak != nil && p.MemoryPeak.HostGB != nil {
		hostPeak = *p.MemoryPeak.HostGB
	}
	if p.MemoryPeak != nil && p.MemoryPeak.DeviceGB != nil {
		devicePeak = *p.MemoryPeak.DeviceGB
	}
	return fmt.Sprintf("%s %s/%s c=%d: %s; %s; latency %.2fs; incremental host/device %.2f/%.2f GiB; peak host/device %.2f/%.2f GiB; %d samples", p.Key.Work, p.Key.Host, p.Key.Component, p.Key.Concurrency, rate, state, p.LatencySeconds, p.IncrementalHostMemoryGB, p.IncrementalDeviceMemoryGB, hostPeak, devicePeak, p.Samples)
}

// Explicit OOMs forbid their performance point; auto OOMs and constrained peaks
// only invalidate comparable memory measurements, so exploration can re-teach it.
func (q *Queue) recordWorkOOM(j *Job, r Result, oomFailure bool) {
	memoryOnly := j.Spec.Needs.Memory == "auto" || !oomFailure
	var key *ObservationKey
	if j.Timing != nil {
		key = j.Timing.Key
	}
	if key == nil && memoryOnly {
		provenance := *j
		provenance.Attempt = 0 // Resumed failures invalidate the same memory class.
		key, _ = q.workObservationKey(&provenance)
	}
	if key == nil {
		return
	}
	k := *key
	if memoryOnly {
		configuration := memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
		for id, point := range q.s.WorkObservations {
			if sameMemoryWork(point, k, configuration) {
				point.MemoryPeak, point.MemoryRecordedAt = nil, time.Time{}
				if !oomFailure && r.MemoryPeak != nil && r.MemoryPeak.Constrained {
					// Keep the existing diagnostic as invalidation provenance,
					// never as a learned estimate or a new class-level bound.
					point.MemoryPeak = r.MemoryPeak
				}
				q.s.WorkObservations[id] = point
			}
		}
		return // Memory invalidation does not forbid completion predictions.
	}
	id := identityHash(k)
	p := q.s.WorkObservations[id]
	p.Key = k
	p.Forbidden = true
	p.RecordedAt = q.clock.Now()
	p.Job = j.ID
	p.Attempt = r.Attempt
	p.Machine = r.Machine
	p.Session = r.Session
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	q.s.WorkObservations[id] = p
}

// Identity fences only. Throughput age (RecordedAt) and memory age
// (MemoryRecordedAt) are checked by their own readers, and ordinary readers
// key through current qualification, so neither kind of evidence expires the other.
func curveStaleReason(p WorkObservation, e Extension, m *Machine) string {
	if m.ExtensionFingerprint != p.Key.Fingerprint {
		return "driver or backend fingerprint changed"
	}
	k, why := curveReferenceKey(e, m, p.Key.Work)
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
	}
	return ""
}
