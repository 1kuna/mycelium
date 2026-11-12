package myce

import "fmt"

// Placement learns from its own accepted work (MY-31, MY-49): among already
// eligible targets, the one whose last accepted run of the same kind of work
// was shorter goes first. A duration ranks placements; it grants no resources
// or slots and is never a deadline.
type completionEstimate struct {
	group   string
	seconds float64
}

func (e completionEstimate) reason() string {
	return fmt.Sprintf("last accepted duration %.2fs on this target", e.seconds)
}

// estimateCompletion is this kind of work's last accepted duration on m, keyed
// exactly as recordWorkObservation records it.
func (q *Queue) estimateCompletion(j *Job, m *Machine) (completionEstimate, bool) {
	if j.State == "queued" && j.Spec.Resumable && j.Attempt > 0 {
		return completionEstimate{}, false
	}
	candidates := []*Job{j}
	if j.Spec.extensionJob() {
		candidates = extensionJobs(j, m, true)
	} else if !plainCommandWork(j.Spec) {
		return completionEstimate{}, false
	}
	for _, candidate := range candidates {
		copy := *candidate
		q.applyLearnedNeeds(&copy)
		copy = *q.memoryCandidate(&copy, m)
		copy.Machine, copy.Session = m.ID, m.Session
		copy.AllocatedDevices = nil
		if m.Devices != nil && !copy.Spec.Needs.cpuOnly() {
			copy.AllocatedDevices = []string{m.Device}
		}
		key, reason := q.workObservationKey(&copy)
		if key == nil || reason != "" {
			continue
		}
		if seconds, ok := q.lastAcceptedSeconds(key); ok {
			return completionEstimate{group: identityHash([]any{key.Work, key.ExtensionDigest, key.Reference}), seconds: seconds}, true
		}
	}
	return completionEstimate{}, false
}

// Known durations precede unknowns; incomparable kinds of work have a stable
// order. When both are unknown the caller executes its original comparator.
func (q *Queue) completionBetter(j *Job, a, b *Machine, stableTie ...bool) (better, known bool) {
	ae, aok := q.estimateCompletion(j, a)
	be, bok := q.estimateCompletion(j, b)
	if aok != bok {
		return aok, true
	}
	if !aok {
		return false, false
	}
	if ae.group != be.group {
		return ae.group < be.group, true
	}
	if ae.seconds != be.seconds {
		return ae.seconds < be.seconds, true
	}
	return (len(stableTie) == 0 || stableTie[0]) && q.deviceUseTieBetter(a, b, targetKey(a) < targetKey(b)), true
}

// Keep existing useful-victim selection and disruption costs authoritative.
// Only otherwise equivalent release plans use completion as their tie-break.
func (q *Queue) completionPlanBetter(j *Job, p, other *placementPlan, scans ...*placementScan) bool {
	if other != nil && p.stops == other.stops && p.cost == other.cost {
		// Preserve call affinity before host value: use an existing engine
		// instead of leaving a call waiting for a cheaper host to load it.
		if c := q.calls[j.ID]; c != nil {
			if a, b := q.modelReadiness(c.Model, p.machine), q.modelReadiness(c.Model, other.machine); a != b {
				return a < b
			}
		}
		if value, _ := q.valueCompare(p.machine, other.machine, scans...); value != 0 {
			return value < 0
		}
		if better, known := q.completionBetter(j, p.machine, other.machine); known {
			return better
		}
	}
	return p.cheaper(other)
}

func (q *Queue) capabilityPlacementBetter(j *Job, a, b *Machine, scans ...*placementScan) bool {
	return q.capabilityPlacementBetterWithTie(j, a, b, true, scans...)
}

func (q *Queue) capabilityPlacementBetterWithTie(j *Job, a, b *Machine, allowTie bool, scans ...*placementScan) bool {
	value, knownValue := q.valueCompare(a, b, scans...)
	if value != 0 {
		return value < 0
	}
	if j.Spec.Needs.cpuOnly() && (a.GPU == "cpu") != (b.GPU == "cpu") {
		return a.GPU == "cpu"
	}
	if a.batchTenants != b.batchTenants {
		return a.batchTenants < b.batchTenants // An idle card beats sharing a busy one (#538).
	}
	if better, known := q.completionBetter(j, a, b, allowTie); known {
		return better
	}
	if knownValue {
		return allowTie && q.deviceUseTieBetter(a, b, targetKey(a) < targetKey(b))
	}
	if q.unmeasuredProgram(j, a) {
		if capabilityBetter(a, b) {
			return true
		}
		if !capabilityBetter(b, a) {
			return allowTie && q.deviceUseTieBetter(a, b, false)
		}
	}
	return false
}
