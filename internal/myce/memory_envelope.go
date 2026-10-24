package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Nil counters mean unavailable, never zero. HostGB is the whole attempt's
// cgroup high-water mark; DeviceGB is an attempt-scoped sampled lower bound.
type AttemptMemoryPeak struct {
	HostGB      *float64 `json:"host_gb,omitempty"`
	DeviceGB    *float64 `json:"device_gb,omitempty"`
	Constrained bool     `json:"constrained,omitempty"`
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

func mergeMemoryPeaks(old, next *AttemptMemoryPeak) *AttemptMemoryPeak {
	if next == nil || next.Constrained {
		return old
	}
	out := AttemptMemoryPeak{}
	if old != nil && !old.Constrained {
		out = *old
	}
	for _, pair := range [][2]**float64{{&out.HostGB, &next.HostGB}, {&out.DeviceGB, &next.DeviceGB}} {
		if *pair[1] != nil && validMemoryValue(**pair[1]) && (*pair[0] == nil || **pair[1] > **pair[0]) {
			n := **pair[1]
			*pair[0] = &n
		}
	}
	if out.HostGB == nil && out.DeviceGB == nil {
		return nil
	}
	return &out
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
	if n.Memory == "auto" && j.OOMRetries == 0 {
		peak, factor, label := q.memoryEstimate(j, m)
		switch {
		case peak != nil:
			host := *peak.HostGB * factor
			if !n.cpuOnly() {
				if peak.DeviceGB != nil && validRate(*peak.DeviceGB) {
					n.MemoryGB = *peak.DeviceGB * factor
				} else if label == "cross-machine estimate" {
					// Unknown device counters cannot become zero reservations.
					n.MemoryGB = max(n.MemoryGB, host)
				}
			}
			if m.UnifiedMemory {
				// Existing needs encode additional CPU-side RAM. Translate the
				// overlapping whole-pool envelope back to that vector once.
				host = max(host, n.MemoryGB) - n.MemoryGB
			}
			n.HostMemoryGB = max(minimumHostMemoryGB, host)
			source = label
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
			host, why := q.exploratoryMemory(j, m)
			refusal = why
			if !n.cpuOnly() {
				n.MemoryGB = q.reclaimableGPUMemory(m, j.Spec.Priority, j.Spec)
				if m.TotalMemoryGB > 0 {
					n.MemoryGB = min(n.MemoryGB, m.TotalMemoryGB)
				}
				if m.UnifiedMemory {
					n.MemoryGB = min(n.MemoryGB, max(0, host-minimumHostMemoryGB))
					host -= n.MemoryGB
				}
			}
			n.HostMemoryGB = max(minimumHostMemoryGB, host)
		}
		// Explicit optional member caps remain declarations to preserve.
		n.HostMemoryGB = max(n.HostMemoryGB, copy.Spec.Run.MemoryGB, copy.Spec.Run.DockerMemoryGB)
		for _, side := range copy.Spec.Run.Sidecars {
			n.HostMemoryGB = max(n.HostMemoryGB, side.MemoryGB)
		}
		for _, side := range copy.Spec.Run.DockerSidecars {
			n.HostMemoryGB = max(n.HostMemoryGB, side.MemoryGB)
		}
		if e := qualifiedExtension(&copy, m); e != nil {
			if v := e.variant(machineTarget(m)); v != nil {
				n.HostMemoryGB = max(n.HostMemoryGB, v.Execution.Main.MemoryGB)
				for _, side := range v.Execution.Sidecars {
					n.HostMemoryGB = max(n.HostMemoryGB, side.MemoryGB)
				}
			}
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

func sameMemoryWork(a, b ObservationKey) bool {
	return a.MemoryConfiguration != "" && a.MemoryConfiguration == b.MemoryConfiguration && a.Work == b.Work && a.ExtensionDigest == b.ExtensionDigest && a.Reference == b.Reference && a.Concurrency == b.Concurrency && a.BatchSize == b.BatchSize
}

// Transfer only the same frozen recipe/work/shape. Prefer the same OS/arch/
// accelerator/layout family; this is not a throughput or input-size model.
func (q *Queue) memoryEstimate(j *Job, m *Machine) (*AttemptMemoryPeak, float64, string) {
	if j.Spec.Resumable && j.Attempt > 0 {
		return nil, 0, ""
	}
	provenance := *j
	provenance.Machine, provenance.Session = m.ID, m.Session
	provenance.AllocatedDevices = nil
	if m.Devices != nil && !j.Spec.Needs.cpuOnly() {
		provenance.AllocatedDevices = []string{m.Device}
	}
	key, _ := q.workObservationKey(&provenance)
	if key == nil {
		return nil, 0, ""
	}
	var exact, family, other *AttemptMemoryPeak
	for _, point := range q.s.WorkObservations {
		k, p := point.Key, point.MemoryPeak
		age := q.clock.Now().Sub(point.MemoryRecordedAt)
		if !sameMemoryWork(k, *key) || point.Source != "accepted work" || point.Samples <= 0 || point.Forbidden || point.StaleReason != "" || point.MemoryRecordedAt.IsZero() || age < 0 || age > workCurveMaxAge || p == nil || p.Constrained || p.HostGB == nil || !validRate(*p.HostGB) || !validRate(*p.HostGB*2.5) {
			continue
		}
		sameFamily := k.Target == key.Target && k.MemoryLayout != "" && k.MemoryLayout == key.MemoryLayout
		if sameFamily && k.Host == key.Host && k.Component == key.Component && k.Hardware == key.Hardware && k.Fingerprint == key.Fingerprint && k.Environment == key.Environment {
			exact = mergeMemoryPeaks(exact, p)
		} else if sameFamily {
			family = mergeMemoryPeaks(family, p)
		} else {
			other = mergeMemoryPeaks(other, p)
		}
	}
	if exact != nil {
		return exact, 1.25, "accepted peak × 1.25"
	}
	if family != nil {
		return family, 1.25 * 1.5, "cross-machine estimate"
	}
	if other != nil {
		return other, 1.25 * 2, "cross-machine estimate"
	}
	return nil, 0, ""
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
	for _, running := range q.s.Jobs {
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
	if room < allowance/2 || room <= m.MemoryCeilingSlackGB {
		return 0, fmt.Sprintf("exploratory first run needs half the host allowance free (%.1f GiB available; %.1f GiB allowance)", room, allowance)
	}
	return room - m.MemoryCeilingSlackGB, ""
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
			why := fmt.Sprintf("%s: %.1f GiB needed, %.1f GiB available (live %.1f GiB; allowance %.1f GiB; %s)", m.ID, hostNeed(m, n.MemoryGB, n.HostMemoryGB), m.FreeHostMemoryGB, m.HostAvailableMemoryGB, m.EffectiveHostMemoryBudgetGB, candidate.MemoryEnvelope.Source)
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
