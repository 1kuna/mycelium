package myce

import "time"

// Only immutable evidence is reduced here. Exploratory room, retry floors,
// drift and admission remain in memoryEstimateForState and its caller.
type memorySizingFacts struct {
	exact, sameHost, family, sameOS, other      *AttemptMemoryPeak
	exactN, sameHostN, familyN, sameOSN, otherN int
	constrainedGB                               float64
	invalidated, completeHostRange              bool
	hostRange                                   *MemoryPeakRange
	hostAcceptedAt                              time.Time
	points                                      []*WorkObservation
}

type memorySizingKey struct {
	key                             ObservationKey
	configuration, layout, siblings string
	cpu, unified, escalation        bool
}

func (q *Queue) memorySizingFacts(j *Job, m *Machine, key *ObservationKey, configuration string, escalation bool) memorySizingFacts {
	catalog := q.memoryObservationCatalog()
	catalog.prepareDerivations(q.clock.Now()) // Fences every recorded-time transition and clock rewind.
	cpu, layout := j.Spec.Needs.cpuOnly(), memoryLayout(j, m)
	// Same-host sibling attribution depends on the complete component identity
	// inventory, even when the target's own ObservationKey is unchanged.
	type sibling struct{ ID, Identity, Fingerprint string }
	var siblings []sibling
	for _, d := range m.Devices {
		siblings = append(siblings, sibling{d.ID, d.Identity, d.ExtensionFingerprint})
	}
	cacheKey := memorySizingKey{*key, configuration, layout, identityHash(siblings), cpu, m.UnifiedMemory, escalation}
	if facts, ok := catalog.sizing[cacheKey]; ok {
		return facts
	}
	var exact, sameHost, family, sameOS, other *AttemptMemoryPeak
	var exactN, sameHostN, familyN, sameOSN, otherN, constrainedGB = 0, 0, 0, 0, 0, 0.0
	var hostRange *MemoryPeakRange
	var hostAcceptedAt time.Time
	completeHostRange := true
	gpuHost := !cpu && key.Target.Platform == "xpu" && !m.UnifiedMemory
	invalidated := false
	// Once corrected evidence exists, legacy XPU attribution on another
	// comparable key (including a sibling card) must not restore the old peak.
	correctedXPU := false
	points := catalog.work[memoryWorkIdentity(*key, configuration)]
	if escalation {
		priors := make([]*WorkObservation, 0, len(points))
		for _, point := range points {
			if point.MemoryEscalation == nil {
				continue
			}
			prior := *point
			prior.MemoryPeak, prior.MemoryRecordedAt = point.MemoryEscalation.Peak, point.MemoryEscalation.RecordedAt
			prior.MemoryRange, prior.GPUHostRange = nil, nil
			priors = append(priors, &prior)
		}
		points = priors
	}
	for _, point := range points {
		if memoryIdentityUsable(*point) && !point.MemoryRecordedAt.IsZero() && point.MemoryPeak != nil && !point.MemoryPeak.constrained() && !point.MemoryPeak.PressureUnavailable && point.MemoryPeak.GPUHost != nil && point.MemoryPeak.GPUHost.SiblingAdjusted {
			correctedXPU = true
			break
		}
	}
	for _, point := range points {
		k, p := point.Key, point.MemoryPeak
		if correctedXPU && k.Target.Platform == "xpu" && p != nil && p.GPUHost != nil && !p.GPUHost.SiblingAdjusted && !p.constrained() {
			continue
		}
		age := q.clock.Now().Sub(point.MemoryRecordedAt)
		if memoryIdentityUsable(*point) && p.constrained() {
			invalidated = true
			if p.HostGB != nil && validRate(*p.HostGB) {
				constrainedGB = max(constrainedGB, hostMemoryPeakGB(p))
			}
		}
		if !memoryIdentityUsable(*point) || point.MemoryRecordedAt.IsZero() || age < 0 || p == nil || p.constrained() || p.PressureUnavailable || p.HostGB == nil || !validRate(*p.HostGB) || !validRate(hostMemoryPeakGB(p)) {
			continue
		}
		if m.UnifiedMemory && !cpu && (p.DeviceGB == nil || !validRate(*p.DeviceGB)) {
			continue
		}
		sameFamily := sameMemoryFamily(*point, *key, layout)
		samples := max(1, point.MemorySamples)
		if sameFamily && sameMemoryHost(k, *key, m) {
			sameHost, sameHostN = mergeMemoryPeaks(sameHost, p), sameHostN+samples
			if point.MemoryRecordedAt.After(hostAcceptedAt) {
				hostAcceptedAt = point.MemoryRecordedAt
			}
			rangePeak := point.MemoryRange
			if gpuHost {
				rangePeak = point.GPUHostRange
				_, complete := p.gpuHostPeakGB()
				completeHostRange = completeHostRange && complete
			}
			hostRange = mergeMemoryPeakRanges(hostRange, rangePeak)
			completeHostRange = completeHostRange && rangePeak.valid() && rangePeak.Samples >= 2
		}
		if sameFamily && k.Host == key.Host && k.Component == key.Component && k.Hardware == key.Hardware && k.Fingerprint == key.Fingerprint && k.Environment == key.Environment {
			exact, exactN = mergeMemoryPeaks(exact, p), exactN+samples
		} else if sameFamily {
			family, familyN = mergeMemoryPeaks(family, p), familyN+samples
		} else if cpu && k.Target.OS == key.Target.OS {
			sameOS, sameOSN = mergeMemoryPeaks(sameOS, p), sameOSN+samples
		} else {
			other, otherN = mergeMemoryPeaks(other, p), otherN+samples
		}
	}
	facts := memorySizingFacts{exact, sameHost, family, sameOS, other, exactN, sameHostN, familyN, sameOSN, otherN, constrainedGB, invalidated, completeHostRange, hostRange, hostAcceptedAt, points}
	if catalog.sizing == nil {
		catalog.sizing = map[memorySizingKey]memorySizingFacts{}
	}
	catalog.sizing[cacheKey] = facts
	return facts
}
