package myce

import (
	"fmt"
	"strings"
	"time"
)

// Nil counters mean unavailable. HostGB samples anon + kernel + shmem, excluding
// file cache. NonreclaimableGB additionally includes unevictable and dirty/
// writeback pages. GPUHost adds complete driver/proc and host-wide evidence;
// these cgroup counters alone do not bound driver pins. CgroupGB retains the
// kernel's total high-water mark and sizes incomplete/legacy/transfer estimates.
// Without cgroup accounting (macOS), HostGB is the sampled process-group RSS
// and Source names that unenforced measurement.
type AttemptMemoryPeak struct {
	// CombinedHostGB is the maximum per-attempt resident-plus-outside figure.
	// Component maxima below remain diagnostics and must not be added across runs.
	// GrowthGB is the largest observed positive memory growth over a sampling
	// horizon. It is measured by the attempt's injected clock, never inferred
	// from a universal percentage. Nil means the worker predates this sensor.
	DeviceGrowthGB      *float64                `json:"device_growth_gb,omitempty"`
	HostSampling        *HostMemorySampling     `json:"host_sampling,omitempty"`
	DeviceSampling      *DeviceMemorySampling   `json:"device_sampling,omitempty"`
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
	reason := "eligible"
	switch {
	case r.ReleaseOnly && r.MemoryPeak != nil && queuedPoolStop(j):
		reason = "pressure_peak"
	case r.ReleaseOnly:
		reason = "release_only"
	case r.OutOfMemory || j.Reason == memoryPressureCause:
		reason = "oom"
	case !memoryConstraintResult(j.StartedAt != nil, r):
		reason = "infrastructure_interrupted"
	case !r.Success:
		reason = "attempt_failed"
	case r.MemoryPeak == nil:
		reason = "peak_unavailable"
	case pooledSampleLimited(r.MemoryPeak):
		reason = "constrained"
	}
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
				if r.ReleaseOnly && old.LearningReason != "" && !(facts.LearningReason == "pressure_peak" && old.MemoryPeak == nil) {
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
	ChargeHostGB float64             `json:"charge_host_gb,omitempty"`
	LimitHostGB  float64             `json:"limit_host_gb,omitempty"`
	SizingInputs *MemorySizingInputs `json:"sizing_inputs,omitempty"`
}

// Immutable inputs to a physical offer or a ceiling-censored recovery. The
// attempt's envelope retains the result and its source after later admissions.
type MemorySizingInputs struct {
	LedgerRemainderGB      float64  `json:"ledger_remainder_gb"`
	AllowanceGB            float64  `json:"allowance_gb"`
	IdleHistoryBoundGB     *float64 `json:"idle_history_bound_gb,omitempty"`
	SlackGB                float64  `json:"slack_gb"`
	LaunchGapGB            float64  `json:"launch_gap_gb"`
	ProjectedRemainderGB   *float64 `json:"projected_remainder_gb,omitempty"`
	DriftGB                float64  `json:"drift_gb,omitempty"`
	ResidentEngineHostGB   float64  `json:"resident_engine_host_gb,omitempty"`
	RecoveryCapacityGB     float64  `json:"recovery_capacity_gb,omitempty"`
	ConstrainedCheckHostGB float64  `json:"constrained_check_host_gb,omitempty"`
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
	for _, pair := range [][2]**float64{{&out.DeviceGrowthGB, &next.DeviceGrowthGB}, {&out.GrowthGB, &next.GrowthGB}, {&out.HostGB, &next.HostGB}, {&out.CombinedHostGB, &next.CombinedHostGB}, {&out.CgroupGB, &next.CgroupGB}, {&out.DeviceGB, &next.DeviceGB}, {&out.NonreclaimableGB, &next.NonreclaimableGB}} {
		if *pair[1] != nil && validMemoryValue(**pair[1]) && (*pair[0] == nil || **pair[1] > **pair[0]) {
			n := **pair[1]
			*pair[0] = &n
		}
	}
	if next.DeviceSource == ownedFootprintSource {
		out.DeviceSource = ownedFootprintSource
	}
	if next.Source != "" {
		out.Source = next.Source
	}
	out.HostSampling = nil // An aggregate cannot inherit another attempt's coverage.
	if next.HostSampling != nil {
		s := *next.HostSampling
		out.HostSampling = &s
	}
	out.DeviceSampling = nil // A new row without coverage cannot inherit an old window.
	if next.DeviceSampling != nil {
		s := *next.DeviceSampling
		out.DeviceSampling = &s // Latest attempt's coverage, never a merged window.
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
		if next.GPUHost.Source == "CUDA proc physical upper bound" {
			p.Source = next.GPUHost.Source
		}
		for _, pair := range [][2]**float64{{&p.PinnedGB, &next.GPUHost.PinnedGB}, {&p.PhysicalGB, &next.GPUHost.PhysicalGB}, {&p.DriverGB, &next.GPUHost.DriverGB}, {&p.HostDropGB, &next.GPUHost.HostDropGB}, {&p.OutsideGB, &next.GPUHost.OutsideGB}} {
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
		candidates[i] = q.sizeQualifiedCandidate(j, candidate, m, i)
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
	return q.memoryCandidateOwned(&copy, m)
}

// The variant index is in the target's qualified extension order. Lowering
// already owns its target-specific copy, so only copy the original job.
func (q *Queue) sizeQualifiedCandidate(j, candidate *Job, m *Machine, variant int) *Job {
	if candidate == j {
		copy := *j
		candidate = &copy
	}
	return q.applyPooledSize(candidate, m, q.pooledSize(j, candidate, m, variant))
}

// The caller owns this candidate. A modified copy gets its own memory estimate.
func (q *Queue) memoryCandidateOwned(j *Job, m *Machine) *Job {
	return q.applyPooledSize(j, m, q.resolvePooledSize(j, m))
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

// RAM and VRAM have independent locality: sibling cards share the host pool,
// but a sibling's device peak still needs the existing transfer margin.
type MemoryPeakRange struct {
	Samples uint64  `json:"samples"`
	MinGB   float64 `json:"min_gb"`
	MaxGB   float64 `json:"max_gb"`
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

// Transfer only the same frozen recipe/work/shape. Prefer the same OS/arch/
// accelerator/layout family; this is not a throughput or input-size model.
//
// A qualification check learns only from checks (#618). Work with no accepted
// evidence of its own is still seeded by its checks' peaks (#186); its own
// accepted runs then replace that seed, so a check never shrinks it.
// Use the same candidates, policy and telemetry as dispatch. This diagnostic
// grants no resources and does not initiate preparation or release work.

type learnedMemoryFact struct {
	Key           ObservationKey `json:"key"`
	Configuration string         `json:"configuration"`
	Machine       string         `json:"machine"`
	Ended         time.Time      `json:"ended"`
	Extension     *Extension     `json:"extension,omitempty"`
	Lower         float64        `json:"lower,omitempty"`
	Recovery      float64        `json:"recovery,omitempty"`
}

func (e *JobMemoryEnvelope) Origin() string {
	switch {
	case e.Basis == "learned":
		return fmt.Sprintf("learned from %d comparable accepted attempts: %s", e.Evidence, e.Source)
	case e.Basis == "exploratory" && e.Evidence > 0:
		return fmt.Sprintf("exploratory, bounded by %d similar accepted attempts: %s", e.Evidence, e.Source)
	case e.Basis == "exploratory" && !strings.Contains(e.Source, "exploratory"):
		return "exploratory: " + e.Source
	}
	return e.Source
}
