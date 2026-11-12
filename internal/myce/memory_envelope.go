package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Nil counters mean unavailable. HostGB samples anon + kernel + shmem, excluding
// file cache. NonreclaimableGB additionally includes unevictable and dirty/
// writeback pages. GPUHost adds complete xe host-object and host-wide evidence;
// these cgroup counters alone do not bound driver pins. CgroupGB retains the
// kernel's total high-water mark and sizes incomplete/legacy/transfer estimates.
// Without cgroup accounting (macOS), HostGB is the sampled process-group RSS
// and Source names that unenforced measurement.
type AttemptMemoryPeak struct {
	// Raw counter reads are not completed peaks. Only observeAttemptMemory promotes a complete sample history.
	nonreclaimableSampleGB *float64
	pressureTotalUS        uint64
	pressureAvg10          float64

	// CombinedHostGB is the maximum per-attempt resident-plus-outside figure.
	// Component maxima below remain diagnostics and must not be added across runs.
	// GrowthGB is the largest observed positive memory growth over a sampling
	// horizon. It is measured by the attempt's injected clock, never inferred
	// from a universal percentage. Nil means the worker predates this sensor.
	DeviceGrowthGB      *float64                `json:"device_growth_gb,omitempty"`
	GrowthGB            *float64                `json:"growth_gb,omitempty"`
	CombinedHostGB      *float64                `json:"combined_host_gb,omitempty"`
	GPUHost             *GPUHostMemory          `json:"gpu_host,omitempty"`
	NonreclaimableGB    *float64                `json:"nonreclaimable_gb,omitempty"`
	HostGB              *float64                `json:"host_gb,omitempty"`
	CgroupGB            *float64                `json:"cgroup_gb,omitempty"`
	DeviceGB            *float64                `json:"device_gb,omitempty"`
	DeviceUnavailable   string                  `json:"device_unavailable,omitempty"`
	DeviceSource        string                  `json:"device_source,omitempty"` // Omitted: per-process counters.
	SwapGB              *float64                `json:"swap_gb,omitempty"`
	Source              string                  `json:"source,omitempty"`
	Events              *MemoryEventCounts      `json:"events,omitempty"`
	Constrained         bool                    `json:"constrained,omitempty"`
	PressureUnavailable bool                    `json:"pressure_unavailable,omitempty"`
	Pressure            *MemoryPressureEvidence `json:"pressure,omitempty"`
}

// One bounded comparison per physical attempt, retained on Result and job GET.
// Nil totals mean unavailable/reset, never zero stalls. avg10 is detail only.
// The stall threshold is 1% of ElapsedUS; swap/floor thresholds are in GiB.
type MemoryPressureEvidence struct {
	Reason              string   `json:"reason"`
	ElapsedUS           float64  `json:"elapsed_us"`
	HostStallUS         *uint64  `json:"host_stall_us,omitempty"`
	CgroupStallUS       *uint64  `json:"cgroup_stall_us,omitempty"`
	HostAvg10           float64  `json:"host_avg10"`
	CgroupAvg10         float64  `json:"cgroup_avg10"`
	HostSwapGrowthBytes uint64   `json:"host_swap_growth_bytes"`
	HostSwapOutBytes    uint64   `json:"host_swap_out_bytes"`
	ResidentGB          float64  `json:"resident_gb"`
	MinAvailableGB      *float64 `json:"min_available_gb,omitempty"`
	ReserveGB           float64  `json:"reserve_gb"`
	AvailableFloorGB    float64  `json:"available_floor_gb"`
	SwapBudgetGB        float64  `json:"swap_budget_gb"`
	HostOOMDelta        uint64   `json:"host_oom_delta"`
	HostPressureLevel   int      `json:"host_pressure_level,omitempty"`
	CgroupRefaultAnon   uint64   `json:"cgroup_refault_anon,omitempty"`
	CgroupSwapOutPages  uint64   `json:"cgroup_swap_out_pages,omitempty"`
}

// Attempt-parent memory.events counters. Nil on the peak means they were not
// read; zero is a read count.
type MemoryEventCounts struct {
	High         uint64 `json:"high"`
	Max          uint64 `json:"max"`
	OOM          uint64 `json:"oom"`
	OOMKill      uint64 `json:"oom_kill"`
	OOMGroupKill uint64 `json:"oom_group_kill"`
}

// Recheck counters as well as the flag: older workers and persisted curves
// treated low-resident file-backed ceiling pressure as unconstrained (#303).
func (p *AttemptMemoryPeak) constrained() bool {
	if p == nil {
		return false
	}
	if p.Constrained {
		return true
	}
	if p.Source == processGroupMemorySource {
		return false
	}
	e := p.Events
	return p.SwapGB != nil && *p.SwapGB > 0 || e != nil && (e.Max > 0 || e.High > 0 || e.OOM > 0 || e.OOMKill > 0 || e.OOMGroupKill > 0)
}

const processGroupMemorySource = "process-group RSS samples; unenforced"

// AttemptMemoryFacts keeps observed telemetry independently of learning and the
// bounded diagnostic history. Missing peaks/counters mean unavailable, not zero.
type AttemptMemoryFacts struct {
	Attempt        int                `json:"attempt"`
	Machine        string             `json:"machine"`
	Session        string             `json:"session"`
	MemoryPeak     *AttemptMemoryPeak `json:"memory_peak,omitempty"`
	MemoryEnvelope *JobMemoryEnvelope `json:"memory_envelope,omitempty"`
	LearningReason string             `json:"learning_reason,omitempty"`
}

func (q *Queue) recordAttemptMemory(j *Job, r Result) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	if j.Attempt == 0 || j.Machine == "" || j.Session == "" {
		return
	}
	if j.StartedAt == nil && r.MemoryPeak == nil {
		return // Prelaunch refusals have no runtime memory facts to retain.
	}
	_, reason := q.memoryLearningSample(j, r)
	facts := AttemptMemoryFacts{Attempt: j.Attempt, Machine: j.Machine, Session: j.Session,
		MemoryPeak: sampledMemoryPeak(nil, r.MemoryPeak), LearningReason: reason}
	if facts.MemoryPeak != nil && facts.MemoryPeak.Source != processGroupMemorySource {
		facts.MemoryPeak.Source = "" // Only publish the known measurement label.
	}
	if facts.MemoryPeak != nil && facts.LearningReason == "infrastructure_interrupted" {
		facts.MemoryPeak.Constrained = false
	}
	if j.MemoryEnvelope != nil {
		envelope := *j.MemoryEnvelope
		facts.MemoryEnvelope = &envelope
	}
	for i, old := range j.MemoryAttempts {
		if old.Attempt == facts.Attempt && old.Machine == facts.Machine && old.Session == facts.Session {
			if facts.MemoryPeak != nil {
				// Cleanup acknowledgements add telemetry, never rewrite the
				// eligibility decision of the accepted completion.
				if r.ReleaseOnly && old.LearningReason != "" {
					facts.LearningReason = old.LearningReason
					if facts.LearningReason == "infrastructure_interrupted" {
						facts.MemoryPeak.Constrained = false
					}
					if old.MemoryPeak != nil && old.MemoryPeak.Pressure != nil {
						facts.MemoryPeak.Pressure = old.MemoryPeak.Pressure
					}
				}
				j.MemoryAttempts[i] = facts
			}
			return
		}
	}
	j.MemoryAttempts = append(j.MemoryAttempts, facts)

}

func mergeMemoryEvents(old, next *MemoryEventCounts) *MemoryEventCounts {
	if next == nil {
		next = old
	}
	if next == nil {
		return nil
	}
	merged := *next
	if old != nil {
		merged.High = max(merged.High, old.High)
		merged.Max = max(merged.Max, old.Max)
		merged.OOM = max(merged.OOM, old.OOM)
		merged.OOMKill = max(merged.OOMKill, old.OOMKill)
		merged.OOMGroupKill = max(merged.OOMGroupKill, old.OOMGroupKill)
	}
	return &merged
}

type JobMemoryEnvelope struct {
	// Cold CPU probes retain known need including slack as their charge floor.
	// Supporting native guards contain growth at physical/allowance capacity.
	ProbeFloorGB    float64 `json:"probe_floor_gb,omitempty"`
	LaunchDriftGB   float64 `json:"launch_drift_gb,omitempty"`
	AvailableHostGB float64 `json:"available_host_gb,omitempty"`
	Source          string  `json:"source"`
	Refusal         string  `json:"refusal,omitempty"`
	Machine         string  `json:"machine"`
	Device          string  `json:"device,omitempty"`
	HostGB          float64 `json:"host_gb"`
	DeviceGB        float64 `json:"device_gb"`
	Enforcement     string  `json:"enforcement"`
	// Basis is declared, learned, hint or exploratory; Evidence counts the
	// accepted attempts behind a learned or bounded figure (#585).
	Basis    string `json:"basis,omitempty"`
	Evidence int    `json:"evidence,omitempty"`
	// What learned sizing would reserve on this target, recorded at
	// assignment for explicit figures too, so stale numbers show (#585).
	Learned *LearnedMemory `json:"learned,omitempty"`
	// A typed host figure is the limit, not the charge (MY-31). With accepted
	// evidence of the work on a guarded target, admission charges
	// ChargeHostGB, what the work reached plus measured uncertainty, and the
	// worker still enforces LimitHostGB, the typed figure the job's needs
	// keep. Zero when the needs are the charge.
	ChargeHostGB float64 `json:"charge_host_gb,omitempty"`
	LimitHostGB  float64 `json:"limit_host_gb,omitempty"`
}

// chargedBelowLimit says a started or assigned typed job is charged its
// learned figure below the typed limit its needs keep.
func chargedBelowLimit(j *Job) bool {
	e := j.MemoryEnvelope
	return e != nil && e.ChargeHostGB > 0 && e.ChargeHostGB < e.LimitHostGB && e.LimitHostGB == j.Spec.Needs.HostMemoryGB && j.Spec.Needs.Memory != "auto"
}

// chargedHostGB is the host figure admission charged j: the learned charge
// below a typed limit, else the needs.
func chargedHostGB(j *Job) float64 {
	if chargedBelowLimit(j) {
		return j.MemoryEnvelope.ChargeHostGB
	}
	return j.Spec.Needs.HostMemoryGB
}

type LearnedMemory struct {
	HostGB   float64  `json:"host_gb"`
	DeviceGB *float64 `json:"device_gb,omitempty"`
	Evidence int      `json:"evidence"`
	Source   string   `json:"source"`
	// The accepted peaks of this work on this target, without the measured
	// uncertainty above. A unified CUDA job charged by use never drops below
	// them; the yield backstop covers growth past them (#1044).
	PeakHostGB   float64 `json:"peak_host_gb,omitempty"`
	PeakDeviceGB float64 `json:"peak_device_gb,omitempty"`
}

// Snapshot-to-launch telemetry drift needs headroom even with ceiling slack off.
func exploratoryDriftMarginGB(m *Machine) float64 {
	// The operator's explicit launch slack is preserved. One maximum supported
	// kernel page is the physical rounding floor, not a workload-size reserve.
	return max(m.MemoryCeilingSlackGB, minimumHostMemoryGB)
}

func mergeMemoryPeaks(old, next *AttemptMemoryPeak) *AttemptMemoryPeak {
	if next == nil || next.Constrained {
		return old
	}
	out := AttemptMemoryPeak{}
	if old != nil && !old.Constrained {
		out = *old
	}
	for _, p := range []*AttemptMemoryPeak{old, next} {
		if combined, ok := p.gpuHostPeakGB(); ok && !p.Constrained && (out.CombinedHostGB == nil || combined > *out.CombinedHostGB) {
			out.CombinedHostGB = &combined
		}
	}
	if next.DeviceGB != nil && validMemoryValue(*next.DeviceGB) && (out.DeviceGB == nil || *next.DeviceGB > *out.DeviceGB) {
		out.DeviceSource = ""
		if next.DeviceSource == deviceHostDeltaSource || next.DeviceSource == deviceUnifiedResidentSource || next.DeviceSource == deviceFootprintSource {
			out.DeviceSource = next.DeviceSource
		}
	}
	for _, pair := range [][2]**float64{{&out.DeviceGrowthGB, &next.DeviceGrowthGB}, {&out.GrowthGB, &next.GrowthGB}, {&out.HostGB, &next.HostGB}, {&out.CgroupGB, &next.CgroupGB}, {&out.DeviceGB, &next.DeviceGB}, {&out.NonreclaimableGB, &next.NonreclaimableGB}, {&out.nonreclaimableSampleGB, &next.nonreclaimableSampleGB}} {
		if *pair[1] != nil && validMemoryValue(**pair[1]) && (*pair[0] == nil || **pair[1] > **pair[0]) {
			n := **pair[1]
			*pair[0] = &n
		}
	}
	if next.Source != "" {
		out.Source = next.Source
	}
	if out.DeviceGB != nil {
		out.DeviceUnavailable = ""
	} else {
		// Publish only worker-owned labels, never raw command errors or output.
		switch next.DeviceUnavailable {
		case deviceMemoryNoSample, deviceMemoryUnsupported, deviceMemoryInventory, deviceMemoryTimeout, deviceMemoryNoCgroup, deviceMemoryNotReported, deviceMemoryShared, deviceUnifiedUncertain:
			out.DeviceUnavailable = next.DeviceUnavailable
		}
	}
	out.Events = mergeMemoryEvents(out.Events, next.Events)
	if next.GPUHost != nil {
		p := GPUHostMemory{}
		if out.GPUHost != nil {
			p = *out.GPUHost
		}
		p.SiblingAdjusted = p.SiblingAdjusted && next.GPUHost.SiblingAdjusted
		if out.GPUHost == nil {
			p.SiblingAdjusted = next.GPUHost.SiblingAdjusted
		}
		for _, pair := range [][2]**float64{{&p.DriverGB, &next.GPUHost.DriverGB}, {&p.HostDropGB, &next.GPUHost.HostDropGB}, {&p.OutsideGB, &next.GPUHost.OutsideGB}} {
			if *pair[1] != nil && validMemoryValue(**pair[1]) && (*pair[0] == nil || **pair[1] > **pair[0]) {
				gb := **pair[1]
				*pair[0] = &gb
			}
		}
		out.GPUHost = &p
	}
	if out.HostGB == nil && out.CgroupGB == nil && out.DeviceGB == nil && out.DeviceUnavailable == "" {
		return nil
	}
	return &out
}

// Size qualified target copies in dispatch order, leaving the queued job intact.
func (q *Queue) memoryCandidates(j *Job, m *Machine) []*Job {
	candidates := extensionJobs(j, m, true)
	for i, candidate := range candidates {
		if candidate.State == "running" || candidate.State == "stopping" {
			continue
		}
		if candidate.Spec.Program != nil {
			copy := *candidate
			q.applyLearnedNeeds(&copy)
			candidate = &copy
		}
		candidates[i] = q.memoryCandidate(candidate, m)
	}
	return candidates
}

// Resolve on the existing planner's target copy. Explicit declarations retain
// their behavior. Never resize an attempt that has acquired execution custody.
func (q *Queue) memoryCandidate(j *Job, m *Machine) *Job {
	if j.State == "running" || j.State == "stopping" {
		return j
	}
	copy := *j
	n := &copy.Spec.Needs
	source, refusal, basis, evidence := "declared", "", "declared", 0
	probeFloor, limit := 0.0, 0.0
	if n.Memory != "auto" {
		if charge, why, runs, ok := q.typedMemoryCharge(j, m); ok {
			limit, n.HostMemoryGB = n.HostMemoryGB, charge
			source, basis, evidence = why, "learned", runs
		}
	} else {
		// Explicit optional member caps remain declarations to preserve.
		hostFloor := memberHostFloor(&copy, m)
		estimate := q.memoryEstimate(j, m)
		refusal = estimate.refusal
		if estimate.source == spentMemoryExploration {
			refusal = spentMemoryExploration + "; " + memoryCapacityRemedies
		}
		foreignFloor, foreign := q.foreignPriorFloor(j, m, estimate)
		if foreign {
			// A peak measured on another OS is a floor here, not a bound: with
			// no measured transfer error this is the OS's ordinary first run,
			// offered the room and floored at that peak.
			estimate.measuredHostGB = max(estimate.measuredHostGB, foreignFloor)
		} else if host, why, ok := q.boundedMemoryCandidate(j, m, estimate); ok {
			estimate.measuredHostGB, estimate.source = host, boundedMemoryExploration
			if !n.cpuOnly() {
				estimate.source = boundedGPUExploration
			}
			if why != "" {
				refusal = why
			}
		} else if estimate.uncalibrated && (estimate.foreign || !n.cpuOnly() && !m.UnifiedMemory) {
			refusal = memoryCapacityRemedies
		}
		if q.learnedMemory() && n.cpuOnly() && !estimate.escalation && !boundedMemorySource(estimate.source) && estimate.uncalibrated {
			probeFloor = max(hostFloor, foreignFloor)
			if estimate.host != nil {
				probeFloor = max(probeFloor, hostMemoryPeakGB(estimate.host))
			}
		}
		deviceFloor := j.DeviceMemoryFloorGB
		if floor := j.MemoryOOMFloor; floor != nil {
			deviceFloor = max(deviceFloor, floor.MemoryGB)
		}
		var family familyMemoryBound
		switch {
		case j.OOMRetries > 0 && (n.cpuOnly() || j.Spec.ExtensionCheck != nil):
			// Recovery estimates demand. Free RAM is an admission constraint,
			// never the next attempt's predicted demand.
			bound, lower := measuredOOMRecovery(j)
			if estimate.host != nil {
				peak := hostMemoryPeakGB(estimate.host)
				bound = max(bound, estimate.measuredHostGB, peak*estimate.hostFactor)
				lower = max(lower, peak)
			}
			// A long sampling horizon can forecast more growth than a target
			// can ever contain. Bound that uncertainty by permanent capacity;
			// preserve every measured lower bound and durable retry floor.
			room, why := q.recoveryMemoryRoom(j, m)
			n.HostMemoryGB = max(min(bound, room), lower, q.constrainedMemoryLowerFor(j, estimate, !n.cpuOnly() && !m.UnifiedMemory))
			if why != "" {
				refusal = why
			}
			source, basis = measuredOOMRecoverySource, "exploratory"
			if estimate.escalation {
				source = estimate.source
			}
		case estimate.host != nil:
			source, basis, evidence = estimate.source, "learned", estimate.samples
			if evidence == 0 || estimate.source == priorVersionMemorySource || estimate.uncalibrated || estimate.deviceUncalibrated || estimate.measuredHostGB > 0 && boundedMemorySource(estimate.source) {
				basis = "exploratory"
			}
			host := hostMemoryPeakGB(estimate.host) * estimate.hostFactor
			if estimate.measuredHostGB > 0 {
				host = estimate.measuredHostGB
			}
			if !n.cpuOnly() {
				peak, factor := estimate.device, estimate.deviceFactor
				if peak != nil && peak.DeviceGB != nil && validRate(*peak.DeviceGB) && validRate(*peak.DeviceGB*factor) {
					n.MemoryGB = *peak.DeviceGB * factor
				} else if peak != nil && (estimate.uncalibrated || estimate.deviceUncalibrated) {
					// A missing device measurement cannot authorize sharing or
					// book less than the exclusive card's available capacity.
					n.MemoryGB = q.reclaimableGPUMemory(m, j.Spec.Priority, j.Spec)
					source += "; unknown device peak; exclusive card"
				} else {
					source += "; GPU recipe default"
				}
			}
			// Shared physical RAM does not imply memcg charges CUDA allocations.
			// GB10 exposes process GPU use absent from its job's cgroup. Until
			// overlap and driver pins are bounded, retain both vectors.
			// hostNeed already adds this GPU reservation to the host vector.
			n.HostMemoryGB = max(minimumHostMemoryGB, host)
		case q.familyMemoryOverHint(j, m, estimate, hostFloor, &family):
			// A per-item entry never repeats its exact work, so the hint would
			// otherwise stand forever. Keep the bounded-first-run OOM protocol.
			n.MemoryGB, n.HostMemoryGB, source, basis, evidence = 0, family.host, "submitter hint superseded; "+family.source, "exploratory", family.evidence
		case j.MemoryFallback != nil && j.MemoryFallback.valid():
			n.MemoryGB, n.HostMemoryGB = j.MemoryFallback.MemoryGB, j.MemoryFallback.HostMemoryGB
			if n.cpuOnly() {
				n.HostMemoryGB, n.MemoryGB = max(n.HostMemoryGB, n.MemoryGB), 0
			}
			if !n.cpuOnly() && n.MemoryGB == 0 {
				n.MemoryGB = j.Spec.Needs.MemoryGB
			}
			source, basis = "submitter hint", "hint"
			if !n.cpuOnly() && n.HostMemoryGB == 0 {
				n.HostMemoryGB = j.Spec.Needs.HostMemoryGB
			}
			// On unified hosts a missing figure becomes neither 0 GiB nor host-wide.
			bounded := false
			if !n.cpuOnly() && (n.MemoryGB == 0 || n.HostMemoryGB == 0) {
				var device, host float64
				var why string
				if device, host, why, evidence, bounded = q.unifiedGPUFirstRun(j, m, estimate, max(n.MemoryGB, deviceFloor), n.HostMemoryGB, hostFloor); bounded {
					n.MemoryGB, n.HostMemoryGB, source = device, host, "submitter hint "+why
				}
			}
			if !n.cpuOnly() && !m.UnifiedMemory {
				if n.MemoryGB == 0 {
					n.MemoryGB = q.reclaimableGPUMemory(m, j.Spec.Priority, j.Spec)
					source += "; exploratory device"
				}
				if n.HostMemoryGB == 0 {
					if host, why, similar, ok := q.coldMemoryBound(j, m, estimate, hostFloor); ok {
						n.HostMemoryGB, source, evidence, bounded = host, source+"; "+why, similar, true
					}
				}
			}
			if !bounded && !n.cpuOnly() && n.HostMemoryGB == 0 {
				host, why := q.exploratoryMemory(j, m)
				if m.UnifiedMemory {
					host -= n.MemoryGB
				}
				n.HostMemoryGB, refusal = max(minimumHostMemoryGB, host, q.constrainedMemoryLowerFor(j, estimate, !m.UnifiedMemory)), why
				source = "submitter hint; exploratory host"
			}
		default:
			source, basis = "exploratory first run", "exploratory"
			if estimate.source != "" {
				source = estimate.source
			}
			host, why := 0.0, ""
			probeMachine := m
			if !n.cpuOnly() && j.Spec.ExtensionCheck != nil {
				physical := *m
				physical.FreeHostMemoryGB = min(m.HostAvailableMemoryGB, q.qualificationCapacity(m).FreeHostMemoryGB)
				physical.hostReservationDebtGB = 0
				probeMachine = &physical
			}
			// Choose the physical opportunity before splitting a unified pool.
			// Admission against m still waits for its existing reservations.
			device, unified, bounded, similar, gpuBounded := q.unifiedGPUFirstRun(j, probeMachine, estimate, deviceFloor, 0, hostFloor)
			if gpuBounded {
				n.MemoryGB, host, source, evidence = device, unified, source+" "+bounded, similar
			} else if bound, bounded, similar, peak, ok := q.coldMemoryBoundOrProbe(j, m, estimate, hostFloor); ok {
				host, source, evidence = bound, bounded, similar
			} else {
				host, why = q.exploratoryMemory(j, probeMachine)
				if q.learnedMemory() && n.cpuOnly() {
					probeFloor = hostFloor
				}
				if peak > 0 {
					// Wait for the measured peak; never lend below it.
					host, probeFloor = max(host, peak), max(probeFloor, peak)
				}
			}
			refusal = why
			if len(m.groupMembers) > 0 && refusal == "" {
				refusal = "a first run explores one card; work spreads across cards only after evidence shows one card is too small"
			}
			if !n.cpuOnly() && !gpuBounded {
				n.MemoryGB = q.reclaimableGPUMemory(m, j.Spec.Priority, j.Spec)
				if m.TotalMemoryGB > 0 {
					n.MemoryGB = min(n.MemoryGB, m.TotalMemoryGB)
				}
				if m.UnifiedMemory {
					n.MemoryGB = max(0, n.MemoryGB-exploratoryDriftMarginGB(m))
					// Reserve member containment before filling the GPU share.
					if host < hostFloor && refusal == "" {
						refusal = fmt.Sprintf("exploratory first run needs %.3f GiB host member memory; %.3f GiB shared headroom available", hostFloor, host)
					}
					n.MemoryGB = min(n.MemoryGB, max(0, host-hostFloor))
					host -= n.MemoryGB
				}
			}
			// Physical headroom is a capacity, never permission to lower a
			// retained constrained requirement. Admission refuses when less
			// room is currently free, including the first constrained attempt.
			n.HostMemoryGB = max(minimumHostMemoryGB, host, q.constrainedMemoryLowerFor(j, estimate, !n.cpuOnly() && !m.UnifiedMemory))
		}
		n.HostMemoryGB = max(n.HostMemoryGB, hostFloor)
		if probeFloor > 0 {
			probeFloor = max(probeFloor, q.constrainedMemoryLowerFor(j, estimate, false))
			if j.MemoryOOMFloor != nil {
				probeFloor = max(probeFloor, j.MemoryOOMFloor.HostMemoryGB)
			}
		}
		if floor := j.MemoryOOMFloor; floor != nil {
			n.MemoryGB = floor.MemoryGB // A host OOM is not a GPU measurement.
			n.HostMemoryGB = max(n.HostMemoryGB, floor.HostMemoryGB)
			source += "; OOM retry floor"
		}
		// An OOM floor is a retained requirement, not an exploratory budget.
		// Keep it above smaller cards so qualification rejects impossible retries.
		if j.MemoryOOMFloor == nil && !n.cpuOnly() && m.TotalMemoryGB > 0 {
			n.MemoryGB = min(n.MemoryGB, m.TotalMemoryGB)
		}
	}
	if !n.cpuOnly() && j.DeviceMemoryFloorGB > 0 {
		n.MemoryGB = max(n.MemoryGB, j.DeviceMemoryFloorGB)
		source += "; shared GPU measured growth floor"
	}
	enforcement := m.MemoryEnforcement
	if enforcement == "" {
		enforcement = "admission-only"
	}
	copy.MemoryEnvelope = &JobMemoryEnvelope{Source: source, Refusal: refusal, Machine: m.ID, Device: m.Device, HostGB: hostNeed(m, n.MemoryGB, n.HostMemoryGB), DeviceGB: n.MemoryGB, Enforcement: enforcement, Basis: basis, Evidence: evidence}
	if limit > 0 {
		// The candidate's needs carry the charge for admission; assignment
		// restores the typed limit to the job (assign).
		copy.MemoryEnvelope.ChargeHostGB, copy.MemoryEnvelope.LimitHostGB = n.HostMemoryGB, limit
	}
	if basis == "exploratory" && evidence == 0 && estimatePhysicalSource(source) {
		copy.MemoryEnvelope.LaunchDriftGB = q.jobProbeDriftMarginGB(j, m)
		copy.MemoryEnvelope.AvailableHostGB = m.FreeHostMemoryGB
	}
	if probeFloor > 0 {
		copy.MemoryEnvelope.ProbeFloorGB = hostNeed(m, 0, probeFloor)
	}
	// The envelope acknowledgement keeps new workers on the old stop contract
	// with older queues, and older workers on their existing kernel ceilings.
	if contains(m.Capabilities, learnedMemoryGuardCapability) {
		copy.MemoryEnvelope.Enforcement = learnedMemoryGuardEnforcement
		if adaptiveMemoryCeilingGB(&copy, m) <= 0 {
			copy.MemoryEnvelope.Enforcement = enforcement
		}
	}
	q.observeMemoryAdmission(&copy, m)
	return &copy
}

func memoryConfiguration(needs ResourceNeeds, args []string, program *Program) string {
	needs.Memory, needs.MemoryGB, needs.HostMemoryGB = "", 0, 0
	needs.Platform, needs.GPU = Platform{}, nil
	return identityHash(struct {
		Needs   ResourceNeeds
		Args    []string
		Program *Program
	}{needs, args, program})
}

func memoryLayout(j *Job, m *Machine) string {
	if m != nil && m.UnifiedMemory && !j.Spec.Needs.cpuOnly() {
		return "unified"
	}
	return "separate"
}

func sameMemoryFamily(point WorkObservation, key ObservationKey, layout string) bool {
	return point.Key.Target == key.Target && point.MemoryLayout != "" && point.MemoryLayout == layout
}

func sameMemoryWork(point WorkObservation, key ObservationKey, configuration string) bool {
	a := point.Key
	return point.MemoryConfiguration != "" && point.MemoryConfiguration == configuration && a.Work == key.Work && a.Check == key.Check && a.ExtensionDigest == key.ExtensionDigest && a.Reference == key.Reference && a.Concurrency == key.Concurrency && a.BatchSize == key.BatchSize
}

// RAM and VRAM have independent locality: sibling cards share the host pool,
// but a sibling's device peak still needs the existing transfer margin.
type memoryEstimate struct {
	key                      *ObservationKey
	foreign                  bool
	exact                    bool // Device evidence is this exact work on this target.
	uncalibrated             bool
	deviceUncalibrated       bool
	refusal                  string
	escalation               bool
	measuredHostGB           float64
	host, device             *AttemptMemoryPeak
	hostFactor, deviceFactor float64
	source                   string
	// Accepted attempts behind host, and the largest constrained lower bound
	// of the same work, which only ever raises a first-run bound.
	samples       int
	constrainedGB float64
}

// Range of complete non-reclaimable attempt peaks, not instantaneous samples
// or throughput sample counts. Two independent attempts are the minimum needed
// to observe a range, not statistical confidence. Until then conservative
// total-peak sizing stays in use; sampler growth supplies measured headroom.
type MemoryPeakRange struct {
	Samples uint64  `json:"samples"`
	MinGB   float64 `json:"min_gb"`
	MaxGB   float64 `json:"max_gb"`
}

func (r *MemoryPeakRange) valid() bool {
	return r != nil && r.Samples > 0 && validRate(r.MinGB) && validRate(r.MaxGB) && r.MinGB <= r.MaxGB
}

func mergeMemoryPeakRanges(a, b *MemoryPeakRange) *MemoryPeakRange {
	if !b.valid() {
		return a
	}
	r := *b
	if a.valid() {
		r.MinGB, r.MaxGB = min(a.MinGB, b.MinGB), max(a.MaxGB, b.MaxGB)
		// Saturate rather than wrap after indefinitely repeated observations.
		r.Samples = a.Samples + min(b.Samples, ^uint64(0)-a.Samples)
	}
	return &r
}

func hostMemoryPeakGB(p *AttemptMemoryPeak) float64 {
	host := *p.HostGB
	if p.NonreclaimableGB != nil && validMemoryValue(*p.NonreclaimableGB) {
		host = max(host, *p.NonreclaimableGB)
	}
	if p.CgroupGB != nil && validMemoryValue(*p.CgroupGB) {
		host = max(host, *p.CgroupGB)
	}
	if combined, complete := p.gpuHostPeakGB(); complete {
		// Conservative fallback/transfer must retain observed driver memory
		// too, even when it exceeds the total cgroup high-water mark.
		host = max(host, combined)
	}
	return host
}

func sameMemoryHost(k, target ObservationKey, m *Machine) bool {
	if k.Host != target.Host || k.Environment != target.Environment {
		return false
	}
	if k.Component == target.Component {
		return k.Hardware == target.Hardware && k.Fingerprint == target.Fingerprint
	}
	// Fingerprints hash device identity together with driver/OS (including the
	// Linux kernel). Compare the source against its own current sibling, not
	// against the target card's necessarily different hash. This also permits
	// existing persisted evidence without weakening fingerprint invalidation.
	for _, d := range m.Devices {
		if d.ID == k.Component && d.Identity == k.Hardware && d.ExtensionFingerprint != "" && d.ExtensionFingerprint == k.Fingerprint {
			return true
		}
	}
	return false
}

// Transfer only the same frozen recipe/work/shape. Prefer the same OS/arch/
// accelerator/layout family; this is not a throughput or input-size model.
//
// A qualification check learns only from checks (#618). Work with no accepted
// evidence of its own is still seeded by its checks' peaks (#186); its own
// accepted runs then replace that seed, so a check never shrinks it.
func (q *Queue) memoryEstimate(j *Job, m *Machine) memoryEstimate {
	estimate := q.memoryEstimateFor(j, m, false)
	if estimate.host != nil || estimate.key == nil || !j.Spec.extensionJob() {
		return estimate
	}
	// Current-version qualification evidence supersedes historical workload
	// evidence. Keep the existing invalidation and spent-exploration fences.
	if estimate.source == "" && j.Spec.ExtensionCheck == nil {
		seed := q.memoryEstimateFor(j, m, true)
		if seed.host != nil && seed.source != spentMemoryExploration {
			seed.key, seed.constrainedGB = estimate.key, max(seed.constrainedGB, estimate.constrainedGB)
			seed.source += "; from qualification checks"
			return seed
		}
	}
	if seed := q.priorVersionMemory(j, m, estimate); seed.host != nil {
		return seed
	}
	return estimate
}

const acceptedMemoryEscalation = "exploratory (accepted prior invalidated; measured recovery)"

func (q *Queue) memoryEstimateFor(j *Job, m *Machine, checkSeed bool) memoryEstimate {
	provenance := *j
	// Resumption excludes recording, not using a prior complete-input estimate.
	provenance.Attempt = 0
	provenance.Machine, provenance.Session = m.ID, m.Session
	provenance.AllocatedDevices = nil
	if m.Devices != nil && !j.Spec.Needs.cpuOnly() {
		provenance.AllocatedDevices = []string{m.Device}
	}
	key, _ := q.workObservationKey(&provenance)
	if key == nil {
		return memoryEstimate{}
	}
	if checkSeed {
		seed := *key
		seed.Check = true
		key = &seed
	}
	configuration := memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
	estimate := q.memoryEstimateForState(j, m, key, configuration, false)
	if estimate.host != nil {
		return estimate
	}
	prior := q.memoryEstimateForState(j, m, key, configuration, true)
	if prior.host == nil || prior.source == spentMemoryExploration {
		return estimate
	}
	// Reserve observed demand plus the measured overshoot/sampling error.
	// Capacity bounds uncertainty only; confirmed need and retry floors win.
	prior.measuredHostGB, prior.refusal = q.recoveryMemoryBound(j, m, estimate, prior.measuredHostGB, hostMemoryPeakGB(prior.host))
	prior.samples = 0
	prior.escalation = true
	prior.constrainedGB = max(prior.constrainedGB, estimate.constrainedGB)
	prior.source = acceptedMemoryEscalation
	return prior
}

// The ordinary and escalation passes use exactly the same recipe/target key.
// Only their evidence differs; do not hash all identity inputs again.
func (q *Queue) memoryEstimateForState(j *Job, m *Machine, key *ObservationKey, configuration string, escalation bool) memoryEstimate {
	facts := q.memorySizingFacts(j, m, key, configuration, escalation)
	exact, sameHost, family, sameOS, other := facts.exact, facts.sameHost, facts.family, facts.sameOS, facts.other
	exactN, sameHostN, familyN, sameOSN, otherN := facts.exactN, facts.sameHostN, facts.familyN, facts.sameOSN, facts.otherN
	constrainedGB, invalidated := facts.constrainedGB, facts.invalidated
	hostRange, hostAcceptedAt, completeHostRange := facts.hostRange, facts.hostAcceptedAt, facts.completeHostRange
	points := facts.points
	layout := memoryLayout(j, m)
	gpuHost := !j.Spec.Needs.cpuOnly() && backendFor(key.Target.Platform).deviceHostMemory && !m.UnifiedMemory
	estimate := memoryEstimate{key: key, constrainedGB: constrainedGB}
	switch {
	case exact != nil:
		estimate.device, estimate.deviceFactor, estimate.source, estimate.samples = exact, 1, "accepted peak + measured uncertainty", exactN
		estimate.exact = true
	case family != nil:
		estimate.device, estimate.deviceFactor, estimate.source, estimate.samples = family, 1, "cross-machine estimate", familyN
	case sameOS != nil:
		estimate.device, estimate.deviceFactor, estimate.source, estimate.samples = sameOS, 1, "cross-machine estimate", sameOSN
	case other != nil:
		estimate.device, estimate.deviceFactor, estimate.source, estimate.samples = other, 1, "cross-machine estimate", otherN
		estimate.foreign = j.Spec.Needs.cpuOnly()
	case invalidated:
		estimate.source = "exploratory (previous peak constrained)"
	}
	estimate.host, estimate.hostFactor = estimate.device, estimate.deviceFactor
	// A spent probe cannot lose its prior through OOM invalidation, history
	// collection or restart. Only accepted target-family evidence supersedes it.
	if exact == nil && family == nil && sameOS == nil {
		if spent, ok := q.s.MemoryExplorations[memoryExplorationKey(*key, configuration, layout)]; ok {
			prior := spent.PriorGB
			if estimate.host != nil {
				prior = max(prior, hostMemoryPeakGB(estimate.host)*estimate.hostFactor)
			}
			estimate.host = &AttemptMemoryPeak{HostGB: &prior}
			estimate.hostFactor, estimate.foreign, estimate.samples = 1, true, 0
			estimate.source = spentMemoryExploration
		}
	}
	if sameHost != nil {
		estimate.host, estimate.hostFactor, estimate.samples = sameHost, 1, sameHostN
		if exact == nil {
			estimate.source = "same-host RAM peak + measured uncertainty; cross-machine device estimate"
		} else if hostMemoryPeakGB(sameHost) > hostMemoryPeakGB(exact) {
			estimate.source += "; same-host RAM peak + measured uncertainty"
		}
		if (j.Spec.Needs.cpuOnly() || gpuHost) && key.Target.OS == "linux" && completeHostRange && hostRange.valid() {
			// Keep all observed resident peaks as a floor, including older
			// attempts. Reserve the observed spread and sensor growth horizon;
			// explicit launch slack remains a separately configured floor.
			peak := max(hostRange.MaxGB, *sameHost.HostGB)
			if sameHost.NonreclaimableGB != nil && validMemoryValue(*sameHost.NonreclaimableGB) {
				peak = max(peak, *sameHost.NonreclaimableGB)
			}
			if gpuHost {
				// The range and combined peak contain per-run totals; adding
				// independently merged components invents an unobserved run.
				combined, _ := sameHost.gpuHostPeakGB()
				peak = max(peak, combined)
			}
			estimate.measuredHostGB = peak + max(hostRange.MaxGB-hostRange.MinGB, memoryGrowthGB(sameHost), exploratoryDriftMarginGB(m))
			estimate.source = "non-reclaimable peak + measured range (measured growth); CPU only"
			if gpuHost {
				estimate.source = "non-reclaimable + xe host memory + measured range (measured growth)"
			}
		}
	}
	if estimate.host != nil && estimate.source != spentMemoryExploration {
		if sameHost != nil {
			bound, _ := observedMemoryBoundForHost(acceptedMemoryPoints(q, points), *key, m)
			estimate.measuredHostGB = max(estimate.measuredHostGB, bound)
			if !escalation {
				// Learned RSS is an estimate, not an explicit hard limit. Retain
				// observed guard overshoot as uncertainty after clean recovery.
				facts := q.memoryConstraintEvidence(*key, configuration, gpuHost)
				if facts.recovery > 0 {
					// A newer accepted same-host completion can replace an older
					// constrained class prior, never a later constrained peak or
					// the caller's durable per-job OOM floor.
					lower := q.memoryConstraintEvidenceAfter(*key, configuration, gpuHost, hostAcceptedAt).lower
					bound := max(estimate.measuredHostGB, lower, facts.recovery)
					room, why := q.recoveryMemoryRoom(j, m)
					if why != "" {
						estimate.refusal = why
					}
					// Fresh clean evidence replaces the constrained class prior.
					// Bound uncertainty, never clean or constrained measurements.
					estimate.measuredHostGB = max(estimate.measuredHostGB, lower, min(bound, room))
				}
			}
		} else {
			q.adaptMemoryTransfer(&estimate, j, m, acceptedMemoryPoints(q, points))
		}
		if !j.Spec.Needs.cpuOnly() && exact == nil && !estimate.uncalibrated {
			q.adaptDeviceTransfer(&estimate, m, acceptedMemoryPoints(q, points))
		}
		if estimate.device != nil && estimate.device.DeviceGB != nil {
			device := *estimate.device.DeviceGB
			if exact != nil {
				bound, _ := observedMemoryBound(pointsForExact(acceptedMemoryPoints(q, points), *key), true)
				if device > 0 {
					estimate.deviceFactor = max(1, bound/device)
				}
			} else if estimate.uncalibrated || estimate.deviceUncalibrated {
				if device > 0 {
					estimate.deviceFactor = max(1, m.TotalMemoryGB/device)
				}
			}
		} else if !j.Spec.Needs.cpuOnly() {
			estimate.deviceUncalibrated = true
		}

	}
	return estimate
}

// The cold-probe drift margin's refusal; exploratoryMemoryStatus alone writes it.
const coldProbeMarginRefusal = "exploratory first run needs physical headroom above launch slack"

// The hold kind of a resolved envelope's refusal: the cold-probe drift margin
// is its own kind, every other refusal is ordinary memory.
func memoryRefusalKind(e *JobMemoryEnvelope, fallback string) string {
	if strings.HasPrefix(e.Refusal, coldProbeMarginRefusal) {
		return "cold_probe_margin"
	}
	return fallback
}

// Telemetry already excludes the operator/driver reserve. Use the shared ledger
// and release predicates; the normal planner alone can authorize stops.
func (q *Queue) exploratoryMemory(j *Job, m *Machine) (float64, string) {
	room, why, _ := q.exploratoryMemoryStatus(j, m)
	return room, why
}

// The boolean identifies temporary physical contention, not unknown telemetry or
// a structural inability to run. Startup waiting uses this same sizing evidence.
func (q *Queue) exploratoryMemoryStatus(j *Job, m *Machine) (float64, string, bool) {
	drift := q.jobProbeDriftMarginGB(j, m)
	allowance := hostMemoryAllowanceGB(m)
	if allowance <= 0 || !m.HostMemoryMeasured || m.HostMemoryError != "" {
		return 0, "exploratory first run needs measured host memory and a known allowance or physical capacity", false
	}
	for _, release := range q.s.HostReleases {
		if release.Host != hostID(m) || release.HostChargeGB <= 0 {
			continue
		}
		live := q.s.Jobs[release.Job]
		if live == nil || live.Machine != release.Machine || live.Session != release.Session || live.Attempt != release.Attempt || (live.State != "running" && live.State != "stopping") {
			return 0, "exploratory first run blocked by an unconfirmed memory-heavy release", true
		}
	}
	var jobs []*Job
	var slots []*ModelSlot
	for _, running := range q.liveJobs() {
		owner := q.planningAllocationMachine(q.s.Machines[running.Machine], running.AllocatedDevices)
		if owner == nil || hostID(owner) != hostID(m) || running.ReleaseConfirmed || (running.State != "running" && running.State != "stopping") {
			continue
		}
		if q.batchPreemptible(running, j.Spec) {
			jobs = append(jobs, running)
		}
	}
	for _, slot := range q.s.ModelSlots {
		owner := slotMachine(q.s.Machines[slot.Machine], slot)
		if owner == nil || hostID(owner) != hostID(m) || !slotReservesMemory(slot) {
			continue
		}
		if q.slotReclaimableFor(slot, j.Spec) {
			slots = append(slots, slot)
		}
	}
	room := min(q.hostRoomAfter(m, jobs, slots), allowance)
	// hostNeed adds ceiling slack back. Leave the named drift margin unused
	// between the inventory snapshot and the strict launch check.
	slack := m.MemoryCeilingSlackGB + drift
	q.observeMemoryAdmissionDrift(j, m, drift)
	if room <= slack {
		return 0, fmt.Sprintf(coldProbeMarginRefusal+" (%.1f GiB available; %.1f GiB allowance)", room, allowance), true
	}
	return room - slack, "", false
}

// Use the same candidates, policy and telemetry as dispatch. This diagnostic
// grants no resources and does not initiate preparation or release work.
func (q *Queue) memoryWait(j *Job, machines map[string]*Machine) *Hold {
	var reasons []string
	// One kind when every blocked target agrees, as targetWait reports.
	kinds := map[string]bool{}
	for _, m := range machines {
		if m.Gone || m.Leaving || !j.Spec.allowsMachine(m.ID) {
			continue
		}
		for _, candidate := range extensionJobs(j, m, true) {
			candidate = q.memoryCandidate(candidate, m)
			n := candidate.Spec.Needs
			// A host the job can never use (target, backend, environment,
			// device, allow-list) is not a RAM candidate, so never name it.
			var eligibility targetFit
			fitsResourcesChecked(candidate, m, max(m.FreeMemoryGB, m.TotalMemoryGB), &eligibility)
			if eligibility.ineligible > 0 {
				continue
			}
			if candidate.MemoryEnvelope.Refusal != "" {
				reasons = append(reasons, m.ID+": "+candidate.MemoryEnvelope.Refusal)
				kinds[memoryRefusalKind(candidate.MemoryEnvelope, "host_memory_headroom")] = true
				continue
			}
			if !fitsResources(candidate, m, max(m.FreeMemoryGB, m.TotalMemoryGB)) {
				continue
			}
			if hostFits(m, n.MemoryGB, n.HostMemoryGB) {
				return nil
			}
			why := fmt.Sprintf("%s: %.1f GiB needed, %.1f GiB available (live available %.1f GiB; reserved %.1f GiB; allowance %.1f GiB; %s)", m.ID, hostNeed(m, n.MemoryGB, n.HostMemoryGB), m.FreeHostMemoryGB, m.HostAvailableMemoryGB, m.HostMemoryInUseGB, m.EffectiveHostMemoryBudgetGB, candidate.MemoryEnvelope.Origin())
			if candidate.MemoryEnvelope.Source == spentMemoryExploration {
				why += "; " + memoryCapacityRemedies
			}
			if unifiedCUDA(m) {
				why += fmt.Sprintf("; unified CUDA physical RAM floor %.3f GiB", q.hostMemoryMargin(m, m))
			}
			if !m.HostMemoryMeasured || m.HostMemoryError != "" {
				why += "; host telemetry unavailable"
			}
			reasons = append(reasons, why)
			kinds["host_memory_headroom"] = true
		}
	}
	slices.Sort(reasons)
	if len(reasons) == 0 {
		return nil
	}
	kind := "host_memory_headroom"
	if len(kinds) == 1 && kinds["cold_probe_margin"] {
		kind = "cold_probe_margin"
	}
	return newHold(kind, "waiting for RAM envelope: "+strings.Join(reasons, "; "))
}

func estimatePhysicalSource(source string) bool {
	return source == "exploratory first run" || strings.Contains(source, "physical headroom") || source == "exploratory (previous peak constrained)"
}
