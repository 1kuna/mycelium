package myce

import (
	"iter"
	"maps"
	"strings"
	"time"
)

const boundedMemoryCapacityWait = "bounded memory exploration needs the largest eligible host allowance free"

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

func (q *Queue) memoryHistoryJobs(indexed bool) map[string]*Job {
	history := q.memoryHistory()
	if history.observeMemoryHistory != nil {
		history.observeMemoryHistory(len(history.s.Jobs), indexed)
	}
	return history.s.Jobs
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

// recovery pairs each failed attempt's uncertainty with that attempt's peak.
// lower is independent measured demand, including constrained successes.
type memoryConstraintEvidence struct{ lower, recovery float64 }

func (q *Queue) memoryConstraintEvidence(key ObservationKey, configuration string, discrete bool) memoryConstraintEvidence {
	return q.memoryConstraintEvidenceAfter(key, configuration, discrete, time.Time{})
}

func (q *Queue) memoryConstraintEvidenceAfter(key ObservationKey, configuration string, discrete bool, after time.Time) memoryConstraintEvidence {
	if cache := q.memoryPlanning; cache != nil {
		id := memoryLowerIdentity(key, configuration)
		id.discrete = discrete
		id.after = after
		if lower, ok := cache.history.lower[id]; ok {
			return lower
		}
		lower := cache.retainedMemoryEvidence(q, id)
		cache.history.lower[id] = lower
		return lower
	}
	return q.memoryConstraintEvidenceFrom(key, configuration, maps.Values(q.memoryHistoryJobs(false)), discrete, after)
}
func (q *Queue) sameOSMemoryLowerBoundUncached(key ObservationKey, configuration string) float64 {
	return q.sameOSMemoryLowerBoundFrom(key, configuration, maps.Values(q.memoryHistoryJobs(false)), false)
}

func (q *Queue) sameOSMemoryLowerBoundFrom(key ObservationKey, configuration string, jobs iter.Seq[*Job], discrete bool) float64 {
	return q.memoryConstraintEvidenceFrom(key, configuration, jobs, discrete, time.Time{}).lower
}

func (q *Queue) memoryConstraintEvidenceFrom(key ObservationKey, configuration string, jobs iter.Seq[*Job], discrete bool, after time.Time) memoryConstraintEvidence {
	history := q.memoryHistory()
	evidence := memoryConstraintEvidence{}
	use := func(f *learnedMemoryFact, extensions []Extension) {
		if f.Ended.Before(after) {
			return
		}
		age := q.clock.Now().Sub(f.Ended)
		k := f.Key
		if k.Work != key.Work || age < 0 || k.Target.OS != key.Target.OS {
			return // Cheap identity filter before comparing the configuration.
		}
		point := WorkObservation{Key: k, MemoryConfiguration: f.Configuration}
		if !sameMemoryWork(point, key, configuration) {
			return
		}
		owner := history.s.Machines[f.Machine]
		if owner == nil || owner.Gone || history.unreported[owner.ID] {
			return
		}
		m := owner
		if owner.Devices != nil {
			m = cpuMachineView(owner)
			if discrete && !owner.UnifiedMemory && k.Target.Platform != "cpu" {
				m = allocationMachine(owner, []string{k.Component})
				// The recorded card must still be present.
				if m == owner {
					return
				}
			}
		}
		if hostID(m) != k.Host || m.ExtensionFingerprint != k.Fingerprint || machineTarget(m) != k.Target {
			return
		}
		if k.ExtensionDigest != "" {
			usable := false
			for _, e := range extensions {
				if e.Digest == k.ExtensionDigest {
					point.StaleReason = curveStaleReason(point, e, m)
					usable = memoryIdentityUsable(point)
				}
			}
			if !usable {
				return
			}
		}
		evidence.lower = max(evidence.lower, f.Lower)
		evidence.recovery = max(evidence.recovery, f.Recovery)
	}
	for j := range jobs {
		if f := learnedMemoryFactOf(j); f != nil {
			use(f, j.Extensions)
		}
	}
	for i := range history.expiredMemory {
		f := &history.expiredMemory[i]
		use(f, f.extensionHolder().Extensions)
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
		why = boundedMemoryCapacityWait
	}
	return budget, why, true
}

// foreignPriorFloor is the floor of CPU work sized only by a peak from
// another OS, with no measured transfer error, and whether it can run here as
// an ordinary first run floored there. The one measured pair in this fleet
// peaked 51% higher on Linux than on macOS (a 7.01 GiB suite reached
// 10.57 GiB), so the foreign peak cannot bound the run; it only floors it,
// with any constrained lower bound, OOM floor and submitter hint. Refusing
// instead kept 100 pieces of about 3 MB off two idle Linux hosts (#1486): the
// bounded exception below needs the largest host's whole allowance free,
// which a host carrying persistent services never has (MY-45). A floor
// larger than this target's allowance still takes that exception or is
// refused.
func (q *Queue) foreignPriorFloor(j *Job, m *Machine, estimate memoryEstimate) (float64, bool) {
	if !j.Spec.Needs.cpuOnly() || !estimate.foreign || !estimate.uncalibrated || estimate.escalation || estimate.host == nil || estimate.source == spentMemoryExploration {
		return 0, false
	}
	floor := max(hostMemoryPeakGB(estimate.host)*estimate.hostFactor, q.constrainedMemoryLower(j, estimate))
	if f := j.MemoryOOMFloor; f != nil {
		floor = max(floor, f.HostMemoryGB)
	}
	if h := j.MemoryFallback; h != nil {
		floor = max(floor, h.HostMemoryGB, h.MemoryGB)
	}
	allowance := hostMemoryAllowanceGB(m)
	return floor, validRate(floor) && allowance > 0 && hostNeed(m, 0, floor) <= allowance
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

// The claim spends the probe to prevent duplicate launches, not to forbid a
// measurement that never happened. Once the claiming attempt has ended and
// released custody without an OOM or any recorded memory peak (a launch
// refused for free memory, or a loss before the first sample), nothing was
// learned. Return the probe so the queue measures instead of parking the job
// behind "bounded exploration already used". Sampled peaks and OOMs keep it.
func (q *Queue) releaseUnusedMemoryExplorations() {
	for id, claim := range q.s.MemoryExplorations {
		if j := q.s.Jobs[claim.Execution.ID]; j != nil && explorationAttemptUnused(j, claim.Execution.Attempt) {
			delete(q.s.MemoryExplorations, id)
		}
	}
}

func explorationAttemptUnused(j *Job, attempt int) bool {
	if j.Attempt == attempt && (j.State == "running" || j.State == "stopping" || !j.ReleaseConfirmed) {
		return false
	}
	for _, c := range j.Copies {
		if c.Attempt == attempt && c.ReleasedAt == nil && c.ReleaseReason == "awaiting_release" {
			return false
		}
	}
	ended := false
	for _, outcome := range j.AttemptHistory {
		if outcome.Attempt == attempt {
			if outcome.OutOfMemory {
				return false
			}
			ended = true
		}
	}
	for _, facts := range j.MemoryAttempts {
		if facts.Attempt == attempt && (facts.MemoryPeak != nil || facts.LearningReason == "oom") {
			return false
		}
	}
	return ended
}
