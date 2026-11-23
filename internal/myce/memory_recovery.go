package myce

import (
	"fmt"
	"slices"
	"strings"
)

const memoryEvidenceRecovering = "memory_evidence_recovering"

const measuredOOMRecoverySource = "exploratory OOM recovery from measured demand"
const censoredHostOOMRecoverySource = "exploratory recovery from ceiling-censored host OOM"

func measuredOOMRecovery(j *Job) (bound, lower float64) {
	bound, lower = minimumHostMemoryGB, minimumHostMemoryGB
	if f := j.MemoryOOMFloor; f != nil {
		bound = max(bound, f.HostMemoryGB)
		lower = max(lower, f.HostMemoryGB)
	}
	for _, facts := range j.MemoryAttempts {
		if facts.LearningReason != "oom" || facts.MemoryPeak == nil || facts.MemoryPeak.HostGB == nil || !validRate(*facts.MemoryPeak.HostGB) {
			continue
		}
		peak := hostMemoryPeakGB(facts.MemoryPeak)
		lower = max(lower, peak)
		margin := max(memoryGrowthGB(facts.MemoryPeak), minimumHostMemoryGB)
		// A run charged its floor grew past it by design, not past a ceiling:
		// its measured growth is the uncertainty, never peak minus floor.
		if e := facts.MemoryEnvelope; e != nil && validRate(e.HostGB) && !floorCharged(e) {
			margin = max(margin, peak-e.HostGB)
		}
		// The durable floor already retains the previous host demand. The
		// historical envelope is a physical total, potentially including a
		// device share; use it only to measure observed ceiling overshoot.
		bound = max(bound, peak+margin)
	}
	return bound, lower
}

// Qualification may defer uncertain sizing, never the author's requirements,
// member containment or an attempt's durable OOM/device retry floor. This copy
// is only a diagnostic; dispatch still uses the complete resolved envelope.
func memoryIntent(j *Job) *Job {
	copy := *j
	copy.MemoryEnvelope = nil
	copy.Spec.Needs.Memory = ""
	n := &copy.Spec.Needs
	n.HostMemoryGB = max(n.HostMemoryGB, minimumHostMemoryGB, j.Spec.Run.MemoryGB)
	for _, side := range j.Spec.Run.Sidecars {
		n.HostMemoryGB = max(n.HostMemoryGB, side.MemoryGB)
	}
	if floor := j.MemoryOOMFloor; floor != nil {
		n.HostMemoryGB = max(n.HostMemoryGB, floor.HostMemoryGB)
		n.MemoryGB = max(n.MemoryGB, floor.MemoryGB)
	}
	n.MemoryGB = max(n.MemoryGB, j.DeviceMemoryFloorGB)
	return &copy
}

func memoryMayAuto(j *Job) bool {
	if j.Spec.Needs.Memory == "auto" {
		return true
	}
	for _, e := range j.Extensions {
		for _, v := range e.Variants {
			if v.Needs.Memory == "auto" {
				return true
			}
		}
	}
	return false
}

func (q *Queue) memorySizingRecoverable(j *Job) bool {
	if !memoryMayAuto(j) {
		return false
	}
	recoverable := false
	q.evaluateTargetsMemory(j, false, func(fit targetFit, candidate *Job, m *Machine) bool {
		auto := j.Spec.Needs.Memory == "auto"
		if e := qualifiedExtension(candidate, m); e != nil {
			if v := e.variant(machineTarget(m)); v != nil {
				auto = auto || v.Needs.Memory == "auto"
			}
		}
		unknownCacheOnly := m.SharedInputCache == nil && fit.failures == 1 && fit.first == "shared input cache"
		if auto && (fit.failures == 0 || unknownCacheOnly) {
			recoverable = true
			return false
		}
		return true
	})
	return recoverable
}

func (q *Queue) memoryEvidenceWaitingReason(j *Job) string {
	if !memoryMayAuto(j) {
		return ""
	}
	if ok, why := q.qualifiedTargetSized(j); !ok && q.memorySizingRecoverable(j) {
		return memoryEvidenceRecovering + ": waiting for safe capacity or fresh comparable memory evidence; " + why
	}
	return ""
}

func (q *Queue) recoveryMemoryBound(j *Job, m *Machine, estimate memoryEstimate, prior, measuredPrior float64) (float64, string) {
	lower := q.constrainedMemoryLowerFor(j, estimate, !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory)
	recovery := 0.0
	if estimate.key != nil {
		facts := q.memoryConstraintEvidence(*estimate.key, memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program), !j.Spec.Needs.cpuOnly() && !m.UnifiedMemory)
		recovery = facts.recovery
	}
	room, why := q.recoveryMemoryRoom(j, m)
	if lower == 0 || recovery <= 0 {
		// A censored peak proves a floor. Keep progress finite and independent
		// of current availability when no larger measured error is retained.
		return max(prior, lower, measuredPrior) + minimumHostMemoryGB, why
	}
	return max(min(max(prior, lower, recovery), room), lower, measuredPrior), why
}

func (q *Queue) recoveryMemoryRoom(j *Job, m *Machine) (float64, string) {
	// Recovery uncertainty is bounded by permanent capacity, never by a
	// first run's bootstrap charge or current free memory.
	physical := q.qualificationCapacity(m)
	room, why := min(hostMemoryAllowanceGB(m), physical.FreeHostMemoryGB)-m.MemoryCeilingSlackGB, ""
	if room <= 0 || !m.HostMemoryMeasured || m.HostMemoryError != "" {
		why = "memory recovery needs measured host memory and a known allowance or physical capacity"
	}
	if m.UnifiedMemory && !j.Spec.Needs.cpuOnly() {
		device := j.Spec.Needs.MemoryGB
		if j.MemoryCensoredOOMGrowthGB != nil && j.MemoryOOMFloor != nil {
			device = j.MemoryOOMFloor.MemoryGB
		}
		room -= device
	}
	return max(0, room), why
}

func (q *Queue) censoredOOMMemory(j *Job, m *Machine, growth float64, inputs *MemorySizingInputs) (float64, string) {
	room, why := q.recoveryMemoryRoom(j, m)
	check := q.constrainedNativeCheckPeak(j, m)
	if m.UnifiedMemory {
		check = max(0, check-j.MemoryOOMFloor.MemoryGB-m.MemoryCeilingSlackGB)
	}
	*inputs, _ = q.hostSizingInputs(m)
	inputs.RecoveryCapacityGB, inputs.ConstrainedCheckHostGB = room, check
	// A check's constrained peak supplies a floor only. If it establishes
	// no demand above the failed ceiling, use the measured physical
	// opportunity: a small observed growth step cannot bound censored work.
	failed := j.MemoryOOMFloor.HostMemoryGB
	bound := 0.0
	if check <= failed {
		opportunity := m.HostAvailableMemoryGB
		if ledger := q.hostMemoryLedgers[m.ID]; ledger != nil {
			opportunity = ledger.CapacityGB + max(0, ledger.ReservedGB-ledger.GrowthGB)
		}
		if inputs.IdleHistoryBoundGB != nil {
			opportunity = *inputs.IdleHistoryBoundGB
		}
		if m.UnifiedMemory {
			opportunity -= j.MemoryOOMFloor.MemoryGB
		}
		bound = max(bound, opportunity-m.MemoryCeilingSlackGB)
	}
	lower := max(failed, check)
	if why == "" && lower > room {
		why = fmt.Sprintf("ceiling-censored recovery needs %.3f GiB host RAM; target capacity is %.3f GiB; %s", lower, room, memoryCapacityRemedies)
	}
	return max(lower, min(room, max(bound, lower+growth))), why
}

// Checks keep their own observation identity. For a censored workload retry,
// use the retained check peaks only as lower bounds for this frozen extension,
// work and native target, with the original measurement's identity verified.
func (q *Queue) constrainedNativeCheckPeak(j *Job, target *Machine) float64 {
	e := qualifiedExtension(j, target)
	if e == nil {
		return 0
	}
	cache := q.memoryPlanning
	if cache == nil {
		cache = newMemoryPlanningCache()
	}
	cache.loadRetainedMemoryIndex(q)
	native := machineTarget(target)
	work := j.Spec.workType()
	history := q.memoryHistory()
	peak := 0.0
	for key, entries := range cache.history.index[memoryHistoryKey{work, native.OS}] {
		if !key.check || key.extension != e.Digest {
			continue
		}
		for _, entry := range entries {
			p := entry.provenance
			if p.key.Target != native || q.clock.Now().Before(p.ended) {
				continue
			}
			owner := history.s.Machines[p.machine]
			if owner == nil || owner.Gone || history.unreported[owner.ID] {
				continue
			}
			m := allocationMachine(owner, []string{p.key.Component})
			if m != nil && m.UnifiedMemory == target.UnifiedMemory && retainedMemoryIdentity(p, m) {
				peak = max(peak, entry.evidence.lower)
			}
		}
	}
	return peak
}

// memorySizingDeadEnd: every target that could otherwise run this auto-memory
// job refuses with the queue's own "retrying unchanged cannot fix capacity"
// judgement. Targets that fail structurally are unrelated; any other failure
// (capacity, preparation, qualification) can clear, so the job keeps waiting.
func (q *Queue) memorySizingDeadEnd(j *Job) string {
	if !memoryMayAuto(j) {
		return ""
	}
	var refusals []string
	live := false
	q.evaluateTargetsMemory(j, true, func(fit targetFit, candidate *Job, m *Machine) bool {
		if fit.excluded {
			return true
		}
		if fit.failures == 0 {
			live = true
			return false
		}
		memory := false
		for _, dimension := range fit.failed {
			switch dimension {
			case "platform", "target", "device", "excluded_machines", "allow-list", "cores", "hardware capacity", "card group":
				return true // this target can never serve the job
			case "memory envelope":
				memory = true
			case "host memory", "GPU memory":
			default:
				live = true
				return false
			}
		}
		refusal := ""
		if candidate.MemoryEnvelope != nil {
			refusal = candidate.MemoryEnvelope.Refusal
		}
		if !memory || !strings.Contains(refusal, memoryCapacityRemedies) {
			live = true
			return false
		}
		refusal = strings.TrimSuffix(strings.TrimSuffix(refusal, memoryCapacityRemedies), "; ")
		if refusal == "" {
			refusal = "no measured comparable demand fits"
		}
		refusals = append(refusals, m.ID+": "+refusal)
		return true
	})
	if live || len(refusals) == 0 {
		return ""
	}
	slices.Sort(refusals)
	return "dead end: no host can size this auto-memory job (" + strings.Join(slices.Compact(refusals), "; ") + "); action: resubmit with an explicit memory figure that fits a host, or reduce the workload's memory use"
}

const checkTransferProbe = "exploratory first run; transferred estimate exceeds physical capacity"

// Native work obtains the missing target measurement. Even paired
// transfer ratios can disagree (fixed overhead need not scale with source
// demand). If their prediction exceeds this host, use its ordinary first-run
// protocol rather than treating that prediction as a physical lower bound.
// Actual OOM recovery keeps its separate measured-demand path.
func (q *Queue) transferNeedsProbe(j *Job, m *Machine, estimate memoryEstimate) bool {
	if j.Spec.ExtensionCheck == nil && j.Spec.Needs.cpuOnly() || !estimate.transferred || estimate.escalation || j.OOMRetries > 0 {
		return false
	}
	accelerator, system, known := jobCapabilityCapacity(j, m)
	host := estimate.measuredHostGB
	device := 0.0
	if !j.Spec.Needs.cpuOnly() && estimate.device != nil && estimate.device.DeviceGB != nil {
		device = *estimate.device.DeviceGB * estimate.deviceFactor
		if m.UnifiedMemory {
			host += device
		}
	}
	return known && (host > system || device > accelerator)
}

// Only authored requirements, local measurements and durable OOM floors can
// prove a native check physically impossible. A transferred estimate or an
// uncertainty forecast can justify a hold/probe, never a terminal verdict.
func (q *Queue) checkMemoryCapacityDeadEnd(j *Job) string {
	for _, id := range j.Spec.AllowedMachines {
		if q.s.Machines[id] == nil {
			return "" // A named host has not reported its capacity yet.
		}
	}
	var refusals []string
	possible := false
	q.evaluateTargetsMemory(j, false, func(fit targetFit, candidate *Job, m *Machine) bool {
		if fit.excluded {
			return true
		}
		for _, dimension := range fit.failed {
			switch dimension {
			case "platform", "target", "device", "allow-list", "excluded_machines", "extension", "cores", "card group", "tools", "environment", "native network isolation":
				return true // Not an eligible execution target.
			}
		}
		accelerator, system, known := jobCapabilityCapacity(candidate, m)
		if !known || !q.inventoryFresh(q.s.Machines[m.ID]) || inventoryWaitingReason(q.s.Machines[m.ID]) != "" {
			possible = true // Unknown capacity is not proof of impossibility.
			return false
		}
		// evaluateTargetsMemory(false) preserves declarations and retry floors,
		// without installing a prediction. Add measurements on this exact
		// destination identity, including constrained peaks, but no transfer or
		// forecast margin. Host evidence can include a currently known sibling;
		// device demand must have been measured on this exact card.
		// The intent-only view clears auto sizing. Restore it only on the
		// identity copy: check memory observations exist only for auto checks.
		// The demand below still starts from the untouched authored intent.
		provenance := *candidate
		provenance.Spec.Needs.Memory = "auto"
		estimate := q.memoryEstimateFor(&provenance, m, false)
		if key := estimate.key; key != nil {
			configuration := memoryConfiguration(candidate.Spec.Needs, candidate.Spec.Args, candidate.Spec.Program)
			facts := q.memorySizingFacts(candidate, m, key, configuration, false)
			for _, point := range facts.points {
				p := point.MemoryPeak
				if !memoryIdentityUsable(*point) || point.MemoryRecordedAt.IsZero() || q.clock.Now().Before(point.MemoryRecordedAt) || p == nil || p.HostGB == nil || !validRate(*p.HostGB) || point.Key.Target != key.Target || !sameMemoryHost(point.Key, *key, m) || !sameMemoryFamily(*point, *key, memoryLayout(candidate, m)) {
					continue
				}
				candidate.Spec.Needs.HostMemoryGB = max(candidate.Spec.Needs.HostMemoryGB, hostMemoryPeakGB(p))
				if !candidate.Spec.Needs.cpuOnly() && point.Key.Component == key.Component && p.DeviceGB != nil && validRate(*p.DeviceGB) {
					candidate.Spec.Needs.MemoryGB = max(candidate.Spec.Needs.MemoryGB, *p.DeviceGB)
				}
			}
		}
		need := candidate.Spec.Needs.HostMemoryGB
		if m.UnifiedMemory && !candidate.Spec.Needs.cpuOnly() {
			need += candidate.Spec.Needs.MemoryGB
		}
		if need <= system && candidate.Spec.Needs.MemoryGB <= accelerator {
			possible = true // Occupancy/allowances/preparation can still clear.
			return false
		}
		refusals = append(refusals, targetFitReason("hardware capacity", candidate, m)+" on "+m.ID)
		return true
	})
	if possible || len(refusals) == 0 {
		return ""
	}
	slices.Sort(refusals)
	return "dead end: native check cannot fit on any eligible host (" + strings.Join(slices.Compact(refusals), "; ") + "); action: reduce the check's memory requirements or provide a capable host and retry qualification"
}
