package myce

import (
	"reflect"
	"slices"
	"strings"
	"time"

	"golang.org/x/mod/semver"
)

const priorVersionMemorySource = "prior version accepted peaks + measured uncertainty"

// A version change is a prior, not fresh qualification. Keep exact platform,
// physical identity and layout fences; never transfer failed or stale peaks.
// Ordered versions prefer the nearest ordered predecessor; opaque versions
// use acceptance time only when no ordered predecessor is available. Opaque
// current versions use acceptance time throughout. Registration has no dates.
func (q *Queue) priorVersionMemory(j *Job, m *Machine, estimate memoryEstimate) memoryEstimate {
	current := qualifiedExtension(j, m)
	if current == nil || estimate.key == nil {
		return memoryEstimate{}
	}
	key := *estimate.key
	v := current.variant(key.Target)
	if v == nil {
		return memoryEstimate{}
	}
	var chosen *Extension
	var points []*WorkObservation
	var latest time.Time
	for _, old := range q.memoryHistory().s.Extensions {
		if old.Name != current.Name || old.Digest == current.Digest {
			continue
		}
		ordered := extensionSemver(old.Version) != "" && extensionSemver(current.Version) != ""
		if ordered && semver.Compare(extensionSemver(old.Version), extensionSemver(current.Version)) >= 0 {
			continue
		}
		prior := old.variant(key.Target)
		if prior == nil {
			continue
		}
		var accepted []*WorkObservation
		var at time.Time
		for _, p := range q.memoryObservationCatalog().extensions[old.Digest] {
			k := p.Key
			if m.UnifiedMemory && !j.Spec.Needs.cpuOnly() && (p.MemoryPeak == nil || p.MemoryPeak.DeviceGB == nil || !validRate(*p.MemoryPeak.DeviceGB)) {
				continue // No paired device evidence: retain shared-pool probing.
			}
			if !q.acceptedMemoryPoint(p) || k.Work != key.Work || k.Reference != key.Reference || k.Target != key.Target ||
				k.Host != key.Host || k.Component != key.Component || k.Hardware != key.Hardware || k.Fingerprint != key.Fingerprint ||
				k.Environment != key.Environment || p.MemoryLayout != memoryLayout(j, m) || k.Concurrency != key.Concurrency || k.BatchSize != key.BatchSize {
				continue
			}
			// Checks always use accepted predecessor checks. Workload peaks
			// also seed a check when both versions and the check launch the
			// same main command, assets and environment. Incidental helpers
			// alone cannot establish the main payload's memory comparability.
			if j.Spec.ExtensionCheck != nil && !k.Check && !checkSharesWorkload(*current, *v, old, *prior) {
				continue
			}
			if j.Spec.ExtensionCheck == nil && k.Check {
				continue
			}
			accepted = append(accepted, p)
			at = maxTime(at, p.MemoryRecordedAt)
		}
		if len(accepted) == 0 {
			continue
		}
		prefer := chosen == nil
		if chosen != nil {
			chosenOrdered := extensionSemver(current.Version) != "" && extensionSemver(chosen.Version) != ""
			compare := 0
			if ordered && chosenOrdered {
				compare = semver.Compare(extensionSemver(old.Version), extensionSemver(chosen.Version))
			}
			prefer = ordered && !chosenOrdered || ordered == chosenOrdered && (compare > 0 || compare == 0 && (at.After(latest) || at.Equal(latest) && old.Digest < chosen.Digest))
		}
		if prefer {
			copy := old
			chosen, points, latest = &copy, accepted, at
		}
	}
	if len(points) == 0 {
		return memoryEstimate{}
	}
	host, n := observedMemoryBound(points, false)
	device, _ := observedMemoryBound(points, true)
	lower := q.constrainedMemoryLowerFor(j, estimate, !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory)
	refusal := ""
	if lower > 0 {
		// A separate submission has no job-local OOM floor. Retained failures
		// of this version still disprove a smaller predecessor's demand.
		host, refusal = q.recoveryMemoryBound(j, m, estimate, host, 0)
	}
	peak := &AttemptMemoryPeak{HostGB: &host}
	if device > 0 {
		peak.DeviceGB = &device
	}
	return memoryEstimate{key: estimate.key, host: peak, device: peak, hostFactor: 1, deviceFactor: 1,
		measuredHostGB: host, constrainedGB: lower, samples: n, source: priorVersionMemorySource, refusal: refusal,
		deviceUncalibrated: device == 0 && !j.Spec.Needs.cpuOnly()}
}

func checkSharesWorkload(current Extension, v ExtensionVariant, old Extension, prior ExtensionVariant) bool {
	shared := slices.Equal(v.Check, v.Execution.Main.Command) && slices.Equal(v.Execution.Main.Command, prior.Execution.Main.Command)
	return shared && reflect.DeepEqual(current.Assets, old.Assets) && reflect.DeepEqual(v.Environments, prior.Environments) &&
		reflect.DeepEqual(v.Execution, prior.Execution)
}

func extensionSemver(version string) string {
	return semver.Canonical("v" + strings.TrimPrefix(version, "v"))
}
