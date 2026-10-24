package myce

import (
	"fmt"
	"time"
)

const workCurveMaxAge = 30 * 24 * time.Hour

// A forecast ranks already eligible placements. It grants no resources, slots,
// victim choices or reservations. Models deliberately remain unknown until
// their exact server recipe can be bound to qualified measurements (#107/4).
type completionEstimate struct {
	group                    string
	service, setup, transfer float64
	samples                  int
}

func (e completionEstimate) seconds() float64 { return e.service + e.setup + e.transfer }
func (e completionEstimate) reason() string {
	return fmt.Sprintf("measured completion %.2fs (service %.2fs + setup %.2fs + transfer %.2fs; %d samples; concurrency 1)", e.seconds(), e.service, e.setup, e.transfer, e.samples)
}

func (q *Queue) estimateCompletion(j *Job, m *Machine) (completionEstimate, bool) {
	if j.Spec.Workload == nil || !j.Spec.extensionJob() {
		return completionEstimate{}, false
	}
	// Resolve each target's own qualified recipe and resource floors before
	// ranking. #145's quality filter belongs in this eligibility boundary.
	for _, candidate := range extensionJobs(j, m, true) {
		copy := *candidate
		q.applyLearnedNeeds(&copy)
		copy.Machine, copy.Session = m.ID, m.Session
		copy.AllocatedDevices = nil
		if m.Devices != nil && !copy.Spec.Needs.cpuOnly() {
			copy.AllocatedDevices = []string{m.Device}
		}
		key, _ := q.workObservationKey(&copy)
		if key == nil {
			continue
		}
		point, ok := q.s.WorkObservations[identityHash(key)]
		if !ok || point.Key != *key || point.Samples <= 0 || point.Forbidden || point.StaleReason != "" || q.clock.Now().Before(point.RecordedAt) || q.clock.Now().Sub(point.RecordedAt) > workCurveMaxAge {
			continue
		}
		rate := point.Rate
		if !validRate(rate) && validRate(point.Units) && validRate(point.Seconds) {
			rate = point.Units / point.Seconds
		}
		if !validRate(rate) {
			continue
		}
		transfer, known := inputTransferSeconds(&copy, m)
		if !known {
			continue
		}
		e := completionEstimate{
			group:   identityHash([]any{key.Work, key.ExtensionDigest, key.Reference}),
			service: copy.Spec.Workload.Units / rate,
			setup:   environmentSeconds(&copy, m), transfer: transfer + q.loadSeconds(copy.Spec.Needs.Model, m), samples: point.Samples,
		}
		if validRate(e.seconds()) && validMemoryValue(e.setup) && validMemoryValue(e.transfer) {
			return e, true
		}
	}
	return completionEstimate{}, false
}

func inputTransferSeconds(j *Job, m *Machine) (float64, bool) {
	bytes := 0.0
	for _, f := range j.Spec.Files {
		bytes += float64(f.SizeBytes)
	}
	if bytes == 0 {
		return 0, true
	}
	if m.CapabilityProfile != nil && m.CapabilityProfile.Benchmark != nil {
		for _, r := range m.CapabilityProfile.Benchmark.Measurements {
			if r.Resource == "network" && r.Method == "file-store-download-8MiB-sha256-v1" && r.Unit == "bytes/s" && validRate(r.Rate) {
				seconds := bytes / r.Rate
				return seconds, validMemoryValue(seconds)
			}
		}
	}
	return 0, false
}

// Known estimates precede unknowns; incomparable reference groups have a stable
// order. Never mix pairwise speed and legacy ordering into a cyclic comparator.
// When both are unknown the caller executes its original comparator unchanged.
func (q *Queue) completionBetter(j *Job, a, b *Machine) (better, known bool) {
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
	if ae.seconds() != be.seconds() {
		return ae.seconds() < be.seconds(), true
	}
	return targetKey(a) < targetKey(b), true
}

// Keep existing useful-victim selection and disruption costs authoritative.
// Only otherwise equivalent release plans use completion as their tie-break.
func (q *Queue) completionPlanBetter(j *Job, p, other *placementPlan) bool {
	if other != nil && p.stops == other.stops && p.cost == other.cost {
		if better, known := q.completionBetter(j, p.machine, other.machine); known {
			return better
		}
	}
	return p.cheaper(other)
}
