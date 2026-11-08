package myce

import (
	"reflect"
	"slices"
	"time"

	"golang.org/x/mod/semver"
)

const priorVersionMemorySource = "prior version accepted peaks + measured uncertainty"

// A version change is a prior, not fresh qualification. Keep exact platform,
// physical identity and layout fences; never transfer failed or stale peaks.
// Ordered versions use the nearest predecessor. Opaque version names use the
// most recently accepted predecessor, since registration has no chronology.
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
		ordered := semver.IsValid("v"+old.Version) && semver.IsValid("v"+current.Version)
		if ordered && semver.Compare("v"+old.Version, "v"+current.Version) >= 0 {
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
			if !q.acceptedMemoryPoint(p) || k.Work != key.Work || k.Reference != key.Reference || k.Target != key.Target ||
				k.Host != key.Host || k.Component != key.Component || k.Hardware != key.Hardware || k.Fingerprint != key.Fingerprint ||
				k.Environment != key.Environment || p.MemoryLayout != memoryLayout(j, m) || k.Concurrency != key.Concurrency || k.BatchSize != key.BatchSize {
				continue
			}
			// Checks always use accepted predecessor checks. Workload peaks
			// also seed a check when it launches the identical main command or
			// retained model helpers, with the same assets and environment.
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
			if ordered && semver.IsValid("v"+chosen.Version) {
				prefer = semver.Compare("v"+old.Version, "v"+chosen.Version) > 0
			} else {
				prefer = at.After(latest) || at.Equal(latest) && old.Digest < chosen.Digest
			}
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
	peak := &AttemptMemoryPeak{HostGB: &host}
	if device > 0 {
		peak.DeviceGB = &device
	}
	return memoryEstimate{key: estimate.key, host: peak, device: peak, hostFactor: 1, deviceFactor: 1,
		measuredHostGB: max(host, estimate.constrainedGB), constrainedGB: estimate.constrainedGB, samples: n, source: priorVersionMemorySource,
		deviceUncalibrated: device == 0 && !j.Spec.Needs.cpuOnly()}
}

func checkSharesWorkload(current Extension, v ExtensionVariant, old Extension, prior ExtensionVariant) bool {
	shared := slices.Equal(v.Check, v.Execution.Main.Command) || len(v.Execution.Sidecars) > 0 && reflect.DeepEqual(v.Execution.Sidecars, prior.Execution.Sidecars)
	return shared && reflect.DeepEqual(current.Assets, old.Assets) && reflect.DeepEqual(v.Environments, prior.Environments) &&
		reflect.DeepEqual(v.Execution.Main.Env, prior.Execution.Main.Env)
}
