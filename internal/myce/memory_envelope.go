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
	// Raw counter reads are not completed peaks (notably Docker release
	// evidence). Only observeAttemptMemory promotes a complete sample history.
	nonreclaimableSampleGB *float64

	GPUHost           *GPUHostMemory     `json:"gpu_host,omitempty"`
	NonreclaimableGB  *float64           `json:"nonreclaimable_gb,omitempty"`
	HostGB            *float64           `json:"host_gb,omitempty"`
	CgroupGB          *float64           `json:"cgroup_gb,omitempty"`
	DeviceGB          *float64           `json:"device_gb,omitempty"`
	DeviceUnavailable string             `json:"device_unavailable,omitempty"`
	SwapGB            *float64           `json:"swap_gb,omitempty"`
	Source            string             `json:"source,omitempty"`
	Events            *MemoryEventCounts `json:"events,omitempty"`
	Constrained       bool               `json:"constrained,omitempty"`
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

// Bound snapshot/persistence cost while preserving the first observation and
// the latest 31. Intermediate facts can be lost; the dropped count exposes it.
const memoryAttemptsLimit = 32

// AttemptMemoryFacts keeps observed telemetry independently of learning and the
// bounded diagnostic history. Missing peaks/counters mean unavailable, not zero.
type AttemptMemoryFacts struct {
	Attempt        int                `json:"attempt"`
	Machine        string             `json:"machine"`
	Session        string             `json:"session"`
	MemoryPeak     *AttemptMemoryPeak `json:"memory_peak,omitempty"`
	MemoryEnvelope *JobMemoryEnvelope `json:"memory_envelope,omitempty"`
}

func recordAttemptMemory(j *Job, r Result) {
	if j.Attempt == 0 || j.Machine == "" || j.Session == "" {
		return
	}
	if j.StartedAt == nil && r.MemoryPeak == nil {
		return // Prelaunch refusals have no runtime memory facts to retain.
	}
	facts := AttemptMemoryFacts{Attempt: j.Attempt, Machine: j.Machine, Session: j.Session,
		MemoryPeak: sampledMemoryPeak(nil, r.MemoryPeak)}
	if facts.MemoryPeak != nil && facts.MemoryPeak.Source != processGroupMemorySource {
		facts.MemoryPeak.Source = "" // Only publish the known measurement label.
	}
	if j.MemoryEnvelope != nil {
		envelope := *j.MemoryEnvelope
		facts.MemoryEnvelope = &envelope
	}
	for i, old := range j.MemoryAttempts {
		if old.Attempt == facts.Attempt && old.Machine == facts.Machine && old.Session == facts.Session {
			if facts.MemoryPeak != nil {
				j.MemoryAttempts[i] = facts
			}
			return
		}
	}
	j.MemoryAttempts = append(j.MemoryAttempts, facts)
	if dropped := len(j.MemoryAttempts) - memoryAttemptsLimit; dropped > 0 {
		kept := make([]AttemptMemoryFacts, 1, memoryAttemptsLimit)
		kept[0] = j.MemoryAttempts[0]
		j.MemoryAttempts = append(kept, j.MemoryAttempts[1+dropped:]...)
		j.MemoryAttemptsDropped += dropped
	}
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
	Source      string  `json:"source"`
	Refusal     string  `json:"refusal,omitempty"`
	Machine     string  `json:"machine"`
	Device      string  `json:"device,omitempty"`
	HostGB      float64 `json:"host_gb"`
	DeviceGB    float64 `json:"device_gb"`
	Enforcement string  `json:"enforcement"`
}

// Snapshot-to-launch telemetry drift needs headroom even with ceiling slack off.
func exploratoryDriftMarginGB(m *Machine) float64 {
	return max(m.MemoryCeilingSlackGB, .125) // 128 MiB minimum.
}

func mergeMemoryPeaks(old, next *AttemptMemoryPeak) *AttemptMemoryPeak {
	if next == nil || next.Constrained {
		return old
	}
	out := AttemptMemoryPeak{}
	if old != nil && !old.Constrained {
		out = *old
	}
	for _, pair := range [][2]**float64{{&out.HostGB, &next.HostGB}, {&out.CgroupGB, &next.CgroupGB}, {&out.DeviceGB, &next.DeviceGB}, {&out.NonreclaimableGB, &next.NonreclaimableGB}, {&out.nonreclaimableSampleGB, &next.nonreclaimableSampleGB}} {
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
		case deviceMemoryNoSample, deviceMemoryUnsupported, deviceMemoryInventory, deviceMemoryTimeout, deviceMemoryNoCgroup:
			out.DeviceUnavailable = next.DeviceUnavailable
		}
	}
	out.Events = mergeMemoryEvents(out.Events, next.Events)
	if next.GPUHost != nil {
		p := GPUHostMemory{}
		if out.GPUHost != nil {
			p = *out.GPUHost
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
		copy := *candidate
		q.applyLearnedNeeds(&copy)
		candidates[i] = q.memoryCandidate(&copy, m)
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
	source, refusal := "declared", ""
	if n.Memory == "auto" {
		// Explicit optional member caps remain declarations to preserve.
		hostFloor := max(minimumHostMemoryGB, copy.Spec.Run.MemoryGB, copy.Spec.Run.DockerMemoryGB)
		for _, side := range copy.Spec.Run.Sidecars {
			hostFloor = max(hostFloor, side.MemoryGB)
		}
		for _, side := range copy.Spec.Run.DockerSidecars {
			hostFloor = max(hostFloor, side.MemoryGB)
		}
		if e := qualifiedExtension(&copy, m); e != nil {
			if v := e.variant(machineTarget(m)); v != nil {
				hostFloor = max(hostFloor, v.Execution.Main.MemoryGB)
				for _, side := range v.Execution.Sidecars {
					hostFloor = max(hostFloor, side.MemoryGB)
				}
			}
		}
		estimate := q.memoryEstimate(j, m)
		switch {
		case estimate.host != nil:
			source = estimate.source
			host := hostMemoryPeakGB(estimate.host) * estimate.hostFactor
			if estimate.measuredHostGB > 0 {
				host = estimate.measuredHostGB
			}
			if !n.cpuOnly() {
				peak, factor := estimate.device, estimate.deviceFactor
				if peak.DeviceGB != nil && validRate(*peak.DeviceGB) && validRate(*peak.DeviceGB*factor) {
					n.MemoryGB = *peak.DeviceGB * factor
				} else if factor > 1.25 {
					// Unknown device counters cannot become zero reservations.
					n.MemoryGB = max(n.MemoryGB, hostMemoryPeakGB(peak)*factor)
					source += "; GPU recipe default with host estimate floor"
				} else {
					source += "; GPU recipe default"
				}
			}
			// Shared physical RAM does not imply memcg charges CUDA allocations.
			// GB10 exposes process GPU use absent from its job's cgroup. Until
			// overlap and driver pins are bounded, retain both vectors.
			// hostNeed already adds this GPU reservation to the host vector.
			n.HostMemoryGB = max(minimumHostMemoryGB, host)
		case j.MemoryFallback != nil && j.MemoryFallback.valid():
			n.MemoryGB, n.HostMemoryGB = j.MemoryFallback.MemoryGB, j.MemoryFallback.HostMemoryGB
			if n.cpuOnly() {
				n.HostMemoryGB, n.MemoryGB = max(n.HostMemoryGB, n.MemoryGB), 0
			}
			if !n.cpuOnly() && n.MemoryGB == 0 {
				n.MemoryGB = j.Spec.Needs.MemoryGB
			}
			source = "submitter hint"
			if !n.cpuOnly() && n.HostMemoryGB == 0 {
				n.HostMemoryGB = j.Spec.Needs.HostMemoryGB
				if n.HostMemoryGB == 0 {
					host, why := q.exploratoryMemory(j, m)
					if m.UnifiedMemory {
						host -= n.MemoryGB
					}
					n.HostMemoryGB, refusal = max(minimumHostMemoryGB, host), why
					source = "submitter hint; exploratory host"
				}
			}
		default:
			source = "exploratory first run"
			if estimate.source != "" {
				source = estimate.source
			}
			host, why := q.exploratoryMemory(j, m)
			refusal = why
			if !n.cpuOnly() {
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
			n.HostMemoryGB = max(minimumHostMemoryGB, host)
		}
		n.HostMemoryGB = max(n.HostMemoryGB, hostFloor)
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
	enforcement := m.MemoryEnforcement
	if enforcement == "" {
		enforcement = "admission-only"
	}
	if copy.Spec.Run.Docker {
		enforcement = "Docker member-only"
		if dockerNeedsEnvelope(copy.Spec) && contains(m.Capabilities, "docker_envelope") {
			enforcement = "Docker shared parent"
		}
	}
	copy.MemoryEnvelope = &JobMemoryEnvelope{Source: source, Refusal: refusal, Machine: m.ID, Device: m.Device, HostGB: hostNeed(m, n.MemoryGB, n.HostMemoryGB), DeviceGB: n.MemoryGB, Enforcement: enforcement}
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

func sameMemoryWork(point WorkObservation, key ObservationKey, configuration string) bool {
	a := point.Key
	return point.MemoryConfiguration != "" && point.MemoryConfiguration == configuration && a.Work == key.Work && a.ExtensionDigest == key.ExtensionDigest && a.Reference == key.Reference && a.Concurrency == key.Concurrency && a.BatchSize == key.BatchSize
}

// RAM and VRAM have independent locality: sibling cards share the host pool,
// but a sibling's device peak still needs the existing transfer margin.
type memoryEstimate struct {
	measuredHostGB           float64
	host, device             *AttemptMemoryPeak
	hostFactor, deviceFactor float64
	source                   string
}

// Range of complete non-reclaimable attempt peaks, not instantaneous samples
// or throughput sample counts. Two independent attempts are the minimum needed
// to observe a range, not statistical confidence. The existing 25% headroom
// stays as a floor for unobserved growth between the 200ms samples; a larger
// observed range increases it. Until then total-peak sizing stays in use.
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
	if _, complete := p.gpuHostPeakGB(); complete {
		// Conservative fallback/transfer must retain observed driver memory
		// too, even when it exceeds the total cgroup high-water mark.
		host = max(host, max(*p.HostGB, *p.NonreclaimableGB)+*p.GPUHost.OutsideGB, *p.GPUHost.HostDropGB)
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
func (q *Queue) memoryEstimate(j *Job, m *Machine) memoryEstimate {
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
	var exact, sameHost, family, other *AttemptMemoryPeak
	var hostRange *MemoryPeakRange
	completeHostRange := true
	gpuHost := !j.Spec.Needs.cpuOnly() && key.Target.Platform == "xpu" && !m.UnifiedMemory
	invalidated := false
	configuration := memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
	layout := "separate"
	if m.UnifiedMemory && !j.Spec.Needs.cpuOnly() {
		layout = "unified"
	}
	for _, point := range q.s.WorkObservations {
		k, p := point.Key, point.MemoryPeak
		age := q.clock.Now().Sub(point.MemoryRecordedAt)
		if sameMemoryWork(point, *key, configuration) && memoryIdentityUsable(point) && p.constrained() {
			invalidated = true
		}
		if !sameMemoryWork(point, *key, configuration) || !memoryIdentityUsable(point) || point.MemoryRecordedAt.IsZero() || age < 0 || age > workCurveMaxAge || p == nil || p.constrained() || p.HostGB == nil || !validRate(*p.HostGB) || !validRate(*p.HostGB*2.5) || p.CgroupGB != nil && validMemoryValue(*p.CgroupGB) && !validMemoryValue(*p.CgroupGB*2.5) {
			continue
		}
		if m.UnifiedMemory && !j.Spec.Needs.cpuOnly() && (p.DeviceGB == nil || !validRate(*p.DeviceGB)) {
			continue
		}
		sameFamily := k.Target == key.Target && point.MemoryLayout != "" && point.MemoryLayout == layout
		if sameFamily && sameMemoryHost(k, *key, m) {
			sameHost = mergeMemoryPeaks(sameHost, p)
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
			exact = mergeMemoryPeaks(exact, p)
		} else if sameFamily {
			family = mergeMemoryPeaks(family, p)
		} else {
			other = mergeMemoryPeaks(other, p)
		}
	}
	estimate := memoryEstimate{}
	switch {
	case exact != nil:
		estimate.device, estimate.deviceFactor, estimate.source = exact, 1.25, "accepted peak × 1.25"
	case family != nil:
		estimate.device, estimate.deviceFactor, estimate.source = family, 1.25*1.5, "cross-machine estimate"
	case other != nil:
		estimate.device, estimate.deviceFactor, estimate.source = other, 1.25*2, "cross-machine estimate"
	case invalidated:
		estimate.source = "exploratory (previous peak constrained)"
	}
	estimate.host, estimate.hostFactor = estimate.device, estimate.deviceFactor
	if sameHost != nil {
		estimate.host, estimate.hostFactor = sameHost, 1.25
		if exact == nil {
			estimate.source = "same-host RAM peak × 1.25; cross-machine device estimate"
		} else if hostMemoryPeakGB(sameHost) > hostMemoryPeakGB(exact) {
			estimate.source += "; same-host RAM peak × 1.25"
		}
		if (j.Spec.Needs.cpuOnly() || gpuHost) && key.Target.OS == "linux" && completeHostRange && hostRange.valid() {
			// Keep all observed resident peaks as a floor, including older
			// attempts. A range of two peaks cannot justify eliminating the
			// existing growth allowance; the launch-drift floor alone is tiny
			// for multi-GiB work. Cost: at least 25% over retained components.
			peak := max(hostRange.MaxGB, *sameHost.HostGB)
			if sameHost.NonreclaimableGB != nil && validMemoryValue(*sameHost.NonreclaimableGB) {
				peak = max(peak, *sameHost.NonreclaimableGB)
			}
			if gpuHost {
				// Retain independent component/driver maxima, even if from
				// different attempts. Never discount an older resident floor.
				peak = max(peak, max(*sameHost.HostGB, *sameHost.NonreclaimableGB)+*sameHost.GPUHost.OutsideGB, *sameHost.GPUHost.HostDropGB)
			}
			estimate.measuredHostGB = peak + max(hostRange.MaxGB-hostRange.MinGB, peak*.25, exploratoryDriftMarginGB(m))
			estimate.source = "non-reclaimable peak + measured range (25% minimum); CPU only"
			if gpuHost {
				estimate.source = "non-reclaimable + xe host memory + measured range (25% minimum)"
			}
		}
	}
	return estimate
}

// Telemetry already excludes the operator/driver reserve. Use the shared ledger
// and release predicates; the normal planner alone can authorize stops.
func (q *Queue) exploratoryMemory(j *Job, m *Machine) (float64, string) {
	allowance := m.EffectiveHostMemoryBudgetGB
	if allowance <= 0 && m.CapabilityProfile != nil {
		allowance = float64(m.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
	}
	if allowance <= 0 || !m.HostMemoryMeasured || m.HostMemoryError != "" || m.HostMemoryPolicyMismatch {
		return 0, "exploratory first run needs measured host memory and a known allowance or physical capacity"
	}
	room := m.FreeHostMemoryGB - m.hostReservationDebtGB
	for _, release := range q.s.HostReleases {
		if release.Host != hostID(m) || release.HostChargeGB < allowance/4 {
			continue
		}
		live := q.s.Jobs[release.Job]
		if live == nil || live.Machine != release.Machine || live.Session != release.Session || live.Attempt != release.Attempt || (live.State != "running" && live.State != "stopping") {
			return 0, "exploratory first run blocked by an unconfirmed memory-heavy release"
		}
	}
	for _, running := range q.liveJobs() {
		owner := allocationMachine(q.s.Machines[running.Machine], running.AllocatedDevices)
		if owner == nil || hostID(owner) != hostID(m) || running.ReleaseConfirmed || (running.State != "running" && running.State != "stopping") {
			continue
		}
		charge := hostNeed(owner, running.Spec.Needs.MemoryGB, running.Spec.Needs.HostMemoryGB)
		if q.batchPreemptible(running, j.Spec) {
			room += charge
		} else if charge >= allowance/4 {
			return 0, "exploratory first run blocked by a protected memory-heavy attempt"
		}
	}
	for _, slot := range q.s.ModelSlots {
		owner := slotMachine(q.s.Machines[slot.Machine], slot)
		if owner == nil || hostID(owner) != hostID(m) || !slotReservesMemory(slot) {
			continue
		}
		model, _ := q.model(slot.Model)
		charge := hostNeed(owner, model.MemoryGB, model.HostMemoryGB)
		if q.slotReclaimableFor(slot, j.Spec) {
			room += charge
		} else if charge >= allowance/4 {
			return 0, "exploratory first run blocked by protected memory-heavy model residency"
		}
	}
	room = min(room, allowance)
	// hostNeed adds ceiling slack back. Leave the named drift margin unused
	// between the inventory snapshot and the strict launch check.
	slack := m.MemoryCeilingSlackGB + exploratoryDriftMarginGB(m)
	if room < allowance/2 || room <= slack {
		return 0, fmt.Sprintf("exploratory first run needs half the host allowance free (%.1f GiB available; %.1f GiB allowance)", room, allowance)
	}
	return room - slack, ""
}

// Use the same candidates, policy and telemetry as dispatch. This diagnostic
// grants no resources and does not initiate preparation or release work.
func (q *Queue) memoryWaitingReason(j *Job, machines map[string]*Machine) string {
	var reasons []string
	for _, m := range machines {
		if m.Gone || m.Leaving || !machineAllowed(j.Spec.AllowedMachines, m.ID) {
			continue
		}
		for _, candidate := range extensionJobs(j, m, true) {
			if !dockerRuntimeFits(candidate, m) {
				continue
			}
			candidate = q.memoryCandidate(candidate, m)
			n := candidate.Spec.Needs
			if candidate.MemoryEnvelope.Refusal != "" {
				reasons = append(reasons, m.ID+": "+candidate.MemoryEnvelope.Refusal)
				continue
			}
			if !fitsResources(candidate, m, max(m.FreeMemoryGB, m.TotalMemoryGB)) {
				continue
			}
			if hostFits(m, n.MemoryGB, n.HostMemoryGB) {
				return ""
			}
			why := fmt.Sprintf("%s: %.1f GiB needed, %.1f GiB available (live %.1f GiB; reserved %.1f GiB; allowance %.1f GiB; %s)", m.ID, hostNeed(m, n.MemoryGB, n.HostMemoryGB), m.FreeHostMemoryGB, m.HostAvailableMemoryGB, m.HostMemoryInUseGB, m.EffectiveHostMemoryBudgetGB, candidate.MemoryEnvelope.Source)
			if unifiedCUDA(m) {
				if floor, ok := unifiedCUDAFloor(m); ok {
					why += fmt.Sprintf("; unified CUDA physical RAM floor %.3f GiB", floor)
				}
			}
			if m.HostMemoryPolicyMismatch {
				why += "; host allowance policy mismatch"
			}
			if !m.HostMemoryMeasured || m.HostMemoryError != "" {
				why += "; host telemetry unavailable"
			}
			reasons = append(reasons, why)
		}
	}
	slices.Sort(reasons)
	if len(reasons) == 0 {
		return ""
	}
	return "waiting for RAM envelope: " + strings.Join(reasons, "; ")
}
