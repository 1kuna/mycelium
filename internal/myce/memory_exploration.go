package myce

import (
	"iter"
	"maps"
	"slices"
	"strings"
	"time"
)

const boundedMemoryExploration = "bounded cross-OS exploration"
const boundedGPUExploration = "bounded cross-family GPU exploration"
const spentMemoryExploration = "transferred prior; bounded exploration already used"

func boundedMemorySource(source string) bool {
	base, _, _ := strings.Cut(source, ";")
	return base == boundedMemoryExploration || base == boundedGPUExploration
}

const memoryCapacityRemedies = "no qualified target can fit this job; reduce the workload's memory use, obtain a clean target-platform measurement for auto memory, or have the operator approve a sufficient allowance on a capable host; retrying unchanged cannot fix capacity"

// One durable claim per comparable CPU work/target OS or GPU target family.
// Unlike diagnostic job history this survives collection. Keeping the prior prevents an OOM from
// erasing the evidence and reopening the exception. Only accepted target-family
// evidence supersedes it. This is negative evidence, not a successful learner.
type memoryExploration struct {
	Execution Execution `json:"execution"`
	PriorGB   float64   `json:"prior_gb"`
}

func memoryExplorationKey(k ObservationKey, configuration, layout string) string {
	work := k.Work
	if k.Check {
		work = "check:" + work // existing workload claims keep their key
	}
	id := identityHash(struct {
		Work, Extension, OS, Configuration string
		Reference                          WorkReference
		Concurrency, BatchSize             int
	}{work, k.ExtensionDigest, k.Target.OS, configuration, k.Reference, k.Concurrency, k.BatchSize})
	if k.Target.Platform != "cpu" {
		// Preserve existing CPU claims; GPU claims use the complete memory family.
		return identityHash(struct {
			Work, Layout string
			Target       ExecutionTarget
		}{id, layout, k.Target})
	}
	return id
}

func (q *Queue) memoryHistory() *Queue {
	if q.memoryOrigin != nil {
		return q.memoryOrigin
	}
	return q
}

// Diagnostic lower bounds may justify one measurement, never an upper bound.
// Require the exact retained execution, fresh comparable configuration, and
// unchanged target identity; a small unrelated/old sample cannot open a probe.
func (q *Queue) sameOSMemoryLowerBound(key ObservationKey, configuration string) float64 {
	return q.memoryLowerBound(key, configuration, false)
}

// Discrete host bounds can use constrained GPU host peaks. Keep this opt-in
// separate from the existing CPU/unified readers and their planning cache.
func (q *Queue) memoryLowerBound(key ObservationKey, configuration string, discrete bool) float64 {
	return q.memoryConstraintEvidence(key, configuration, discrete).lower
}

type memoryConstraintEvidence struct{ lower, margin float64 }

func (q *Queue) memoryConstraintEvidence(key ObservationKey, configuration string, discrete bool) memoryConstraintEvidence {
	if cache := q.memoryPlanning; cache != nil {
		id := memoryLowerIdentity(key, configuration)
		id.discrete = discrete
		if lower, ok := cache.history.lower[id]; ok {
			return lower
		}
		lower := q.memoryConstraintEvidenceFrom(key, configuration, slices.Values(cache.retainedMemoryJobs(q, id)), discrete)
		cache.history.lower[id] = lower
		return lower
	}
	return q.memoryConstraintEvidenceFrom(key, configuration, maps.Values(q.memoryHistory().s.Jobs), discrete)
}
func (q *Queue) sameOSMemoryLowerBoundUncached(key ObservationKey, configuration string) float64 {
	return q.sameOSMemoryLowerBoundFrom(key, configuration, maps.Values(q.memoryHistory().s.Jobs), false)
}

func (q *Queue) sameOSMemoryLowerBoundFrom(key ObservationKey, configuration string, jobs iter.Seq[*Job], discrete bool) float64 {
	return q.memoryConstraintEvidenceFrom(key, configuration, jobs, discrete).lower
}

func (q *Queue) memoryConstraintEvidenceFrom(key ObservationKey, configuration string, jobs iter.Seq[*Job], discrete bool) memoryConstraintEvidence {
	history := q.memoryHistory()
	evidence := memoryConstraintEvidence{}
	for j := range jobs {
		if j.Timing == nil || j.Timing.Key == nil {
			continue
		}
		ended := time.Time{}
		for _, outcome := range j.AttemptHistory {
			if outcome.Attempt == j.Timing.Execution.Attempt && outcome.Machine == j.Timing.Machine && outcome.Session == j.Timing.Session {
				ended = outcome.EndedAt
			}
		}
		if ended.IsZero() && j.StartedAt != nil && j.FinishedAt != nil {
			ended = *j.FinishedAt
		}
		if ended.IsZero() {
			continue
		}
		age := q.clock.Now().Sub(ended)
		k := *j.Timing.Key
		if k.Work != key.Work || age < 0 || k.Target.OS != key.Target.OS {
			continue // Cheap identity filter before hashing the configuration.
		}
		point := WorkObservation{Key: k, MemoryConfiguration: memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)}
		if !sameMemoryWork(point, key, configuration) {
			continue
		}
		owner := history.s.Machines[j.Timing.Machine]
		if owner == nil || owner.Gone || history.unreported[owner.ID] {
			continue
		}
		m := owner
		if owner.Devices != nil {
			m = cpuMachineView(owner)
			if discrete && !owner.UnifiedMemory && k.Target.Platform != "cpu" {
				m = allocationMachine(owner, []string{k.Component})
				// The recorded card must still be present.
				if m == owner {
					continue
				}
			}
		}
		if hostID(m) != k.Host || m.ExtensionFingerprint != k.Fingerprint || machineTarget(m) != k.Target {
			continue
		}
		if k.ExtensionDigest != "" {
			usable := false
			for _, e := range j.Extensions {
				if e.Digest == k.ExtensionDigest {
					point.StaleReason = curveStaleReason(point, e, m)
					usable = memoryIdentityUsable(point)
				}
			}
			if !usable {
				continue
			}
		}
		for _, facts := range j.MemoryAttempts {
			p := facts.MemoryPeak
			if facts.Attempt != j.Timing.Execution.Attempt || j.Timing.Execution.ID != j.ID || facts.Machine != j.Timing.Machine || facts.Session != j.Timing.Session || p == nil || !p.constrained() || p.HostGB == nil || !validRate(*p.HostGB) || !memoryConstraintFacts(facts) {
				continue
			}
			peak := hostMemoryPeakGB(p)
			evidence.lower = max(evidence.lower, peak)
			margin := memoryGrowthGB(p)
			if envelope := facts.MemoryEnvelope; envelope != nil && envelope.HostGB > 0 {
				margin = max(margin, peak-envelope.HostGB)
			}
			if facts.LearningReason == "oom" || facts.LearningReason == "attempt_failed" {
				evidence.margin = max(evidence.margin, margin)
			}
		}
	}
	return evidence
}

// Reuse permanent qualification (including platform, recipe, policy and disk)
// while removing just the prior's memory demand. Busy hosts still qualify;
// live exploration/launch checks below retain their normal admission fences.
func (q *Queue) largestMemoryExplorationHost(j *Job, targetMachine *Machine, prior float64) (float64, bool) {
	history := q.memoryHistory()
	largest, priorFits := 0.0, false
	for _, owner := range history.s.Machines {
		if !history.jobAllowsMachine(j, owner.ID) {
			continue
		}
		for _, target := range history.machineTargets(owner) {
			wanted, actual := machineTarget(targetMachine), machineTarget(target)
			if actual.OS != wanted.OS || !j.Spec.Needs.cpuOnly() && (actual != wanted || memoryLayout(j, target) != memoryLayout(j, targetMachine)) {
				continue
			}
			m := history.qualificationCapacity(target)
			for _, candidate := range extensionJobs(j, &m, true) {
				probe := *candidate
				probe.MemoryEnvelope = nil
				probe.Spec.Needs.HostMemoryGB = minimumHostMemoryGB
				if !fitsResources(&probe, &m, m.TotalMemoryGB) || !hostFits(&m, 0, minimumHostMemoryGB) {
					continue
				}
				largest = max(largest, m.FreeHostMemoryGB)
				priorFits = priorFits || hostFits(&m, 0, prior)
			}
		}
	}
	return largest, priorFits
}

func (q *Queue) boundedMemoryCandidate(j *Job, m *Machine, estimate memoryEstimate) (float64, string, bool) {
	// Fan-out changes the selected piece/configuration during assignment; it
	// cannot share this single-attempt claim. An OOM floor is already negative
	// evidence, never permission to try a smaller exploratory envelope.
	crossGPU := !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory && len(m.groupMembers) == 0 && estimate.uncalibrated
	if !estimate.uncalibrated || estimate.escalation || (!estimate.foreign && !crossGPU) || estimate.key == nil || j.Batch != nil || j.Spec.FanOut != nil || j.MemoryOOMFloor != nil {
		return 0, "", false
	}
	configuration := memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
	if _, spent := q.s.MemoryExplorations[memoryExplorationKey(*estimate.key, configuration, memoryLayout(j, m))]; spent {
		return 0, "", false
	}
	prior := hostMemoryPeakGB(estimate.host) * estimate.hostFactor
	cap, priorFits := q.largestMemoryExplorationHost(j, m, prior)
	if (!estimate.uncalibrated && priorFits) || cap <= 0 || q.qualificationCapacity(m).FreeHostMemoryGB != cap {
		return 0, "", false
	}
	lower := max(q.sameOSMemoryLowerBound(*estimate.key, configuration), q.constrainedMemoryLowerFor(j, estimate, crossGPU))
	if lower <= 0 {
		lower = prior
	}
	if crossGPU {
		// One foreign-family sample is a prior, not a target measurement.
		// Retain the observed total-cgroup lower bound. Unknown transfer
		// error requires a physical probe, never another guessed factor.
		lower = max(lower, hostMemoryPeakGB(estimate.host), estimate.constrainedGB)
	}
	budget := cap - m.MemoryCeilingSlackGB - q.coldProbeDriftMarginGB(m)
	hint := 0.0
	if j.MemoryFallback != nil {
		hint = j.MemoryFallback.HostMemoryGB
		if j.Spec.Needs.cpuOnly() {
			hint = max(hint, j.MemoryFallback.MemoryGB)
		}
	}
	if lower <= 0 || !validRate(lower) || lower > budget || hint > budget {
		return 0, "", false
	}
	room, why := q.exploratoryMemory(j, m)
	if why == "" && room < budget {
		why = "bounded memory exploration needs the largest eligible host allowance free"
	}
	return budget, why, true
}

// Assignment consumes the exception in the same transaction as normal attempt
// custody. Even loss/prelaunch refusal spends it: this costs one possible probe
// but prevents duplicate launches after a lost offer or queue restart.
func (q *Queue) claimMemoryExploration(j *Job, m *Machine) {
	if j.MemoryEnvelope == nil || !boundedMemorySource(j.MemoryEnvelope.Source) {
		return
	}
	estimate := q.memoryEstimate(j, m)
	if estimate.key == nil || estimate.host == nil {
		return
	}
	if q.s.MemoryExplorations == nil {
		q.s.MemoryExplorations = map[string]memoryExploration{}
	}
	id := memoryExplorationKey(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program), memoryLayout(j, m))
	q.s.MemoryExplorations[id] = memoryExploration{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, PriorGB: hostMemoryPeakGB(estimate.host) * estimate.hostFactor}
}
