package myce

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

func observationAllowance(m *Machine) string {
	power, mode := "", ""
	if a := machineAvailability(m); a != nil {
		power, mode = a.Signals.Power, a.Mode
	}
	return identityHash([]any{m.CPUReserveCores, m.EffectiveCPUCores, m.EffectiveHostMemoryBudgetGB, m.TotalMemoryGB, m.ReservedMemoryGB, m.UnifiedMemory, power, mode})
}

// Operator allowances and power mode affect throughput comparability, not the
// recipe's memory need. Retain legacy keys; the normal recovery path rechecks
// every physical/recipe identity fence before memory readers use these rows.
func memoryIdentityUsable(p WorkObservation) bool {
	return p.StaleReason == "" || p.StaleReason == "resource allowance or power mode changed"
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
		// Missing inventory is temporary (notably before the first check-in
		// after a queue restart). Revalidate it against fresh identity evidence;
		// actual recipe, hardware and environment changes remain invalidations.
		if !memoryIdentityUsable(p) && p.StaleReason != "component unavailable" {
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
				// Initial inventory and pending/failed backend probes can report
				// a component before its fingerprint. Absence is not a change.
				if m.ExtensionFingerprint == "" {
					reason = "component unavailable"
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
		if reason != p.StaleReason {
			next := p
			next.StaleReason = reason
			if durableWorkObservation(p).StaleReason != durableWorkObservation(next).StaleReason {
				q.touchCatalog("work_observations/" + id)
			} else {
				q.touchCurveLabel(id)
			}
			q.s.WorkObservations[id] = next
			q.evidenceGeneration++
			q.memoryObservationIndex = nil
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

func (p WorkObservation) Summary() string { return p.SummaryAt(p.RecordedAt) }

// SummaryAt shows the kind of work's record: its last accepted duration and
// accepted peak, with any invalidation.
func (p WorkObservation) SummaryAt(time.Time) string {
	state := "current"
	if p.StaleReason != "" {
		state = "stale: " + p.StaleReason
	}
	if p.Forbidden {
		state = "forbidden: OOM or memory bound exceeded"
		if p.StaleReason != "" {
			state += "; stale: " + p.StaleReason
		}
	}
	last := "unknown"
	if validRate(p.LastSeconds) {
		last = fmt.Sprintf("%.2fs", p.LastSeconds)
	}
	peak := "unknown"
	if p.MemoryPeak != nil && p.MemoryPeak.HostGB != nil {
		peak = fmt.Sprintf("host %.2f GiB", *p.MemoryPeak.HostGB)
		if p.MemoryPeak.DeviceGB != nil {
			peak += fmt.Sprintf(", device %.2f GiB", *p.MemoryPeak.DeviceGB)
		}
	}
	return fmt.Sprintf("%s %s/%s: last accepted duration %s; peak %s; %s; %d samples", p.Key.Work, p.Key.Host, p.Key.Component, last, peak, state, p.Samples)
}

// Explicit OOMs forbid their performance point; auto OOMs and failed constrained
// attempts invalidate comparable memory so exploration can re-teach it.
func (q *Queue) recordWorkOOM(j *Job, r Result, oomFailure bool) {
	if !oomFailure && !memoryConstraintResult(j.StartedAt != nil, r) {
		return
	}
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
		layout := memoryLayout(j, allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices))
		for _, id := range slices.Sorted(maps.Keys(q.s.WorkObservations)) {
			point := q.s.WorkObservations[id]
			if j.MemoryEnvelope != nil && boundedMemorySource(j.MemoryEnvelope.Source) && (point.Key.Target.OS != k.Target.OS || !j.Spec.Needs.cpuOnly() && !sameMemoryFamily(point, k, layout)) {
				continue // A target-family probe cannot disprove the transferred prior.
			}
			if sameMemoryWork(point, k, configuration) {
				old := point
				if point.MemoryPeak != nil && !point.MemoryPeak.constrained() && !point.MemoryRecordedAt.IsZero() {
					point.MemoryEscalation = &MemoryEscalationPrior{Peak: point.MemoryPeak, RecordedAt: point.MemoryRecordedAt}
				}
				point.MemoryPeak, point.MemoryRecordedAt = nil, time.Time{}
				point.MemoryRange, point.MemorySamples = nil, 0
				point.GPUHostRange = nil
				if !oomFailure && r.MemoryPeak != nil && r.MemoryPeak.Constrained {
					// Keep the existing diagnostic as invalidation provenance,
					// never as a learned estimate or a new class-level bound.
					point.MemoryPeak = r.MemoryPeak
				}
				q.putWorkObservation(id, point)
				q.recordMemoryInvalidation(j, old, point)
			}
		}
		return // Memory invalidation does not forbid the duration record.
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
	q.putWorkObservation(id, p)
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
