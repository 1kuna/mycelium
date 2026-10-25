package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Nil counters mean unavailable. HostGB samples anon + kernel + shmem, excluding
// reclaimable file cache. CgroupGB retains the kernel's total high-water mark.
type AttemptMemoryPeak struct {
	HostGB      *float64 `json:"host_gb,omitempty"`
	CgroupGB    *float64 `json:"cgroup_gb,omitempty"`
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
	for _, pair := range [][2]**float64{{&out.HostGB, &next.HostGB}, {&out.CgroupGB, &next.CgroupGB}, {&out.DeviceGB, &next.DeviceGB}} {
		if *pair[1] != nil && validMemoryValue(**pair[1]) && (*pair[0] == nil || **pair[1] > **pair[0]) {
			n := **pair[1]
			*pair[0] = &n
		}
	}
	if out.HostGB == nil && out.CgroupGB == nil && out.DeviceGB == nil {
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
		peak, factor, label := q.memoryEstimate(j, m)
		switch {
		case peak != nil:
			source = label
			host := *peak.HostGB * factor
			if !n.cpuOnly() {
				if peak.DeviceGB != nil && validRate(*peak.DeviceGB) && validRate(*peak.DeviceGB*factor) {
					n.MemoryGB = *peak.DeviceGB * factor
				} else if label == "cross-machine estimate" {
					// Unknown device counters cannot become zero reservations.
					n.MemoryGB = max(n.MemoryGB, host)
					source += "; GPU recipe default with host estimate floor"
				} else {
					source += "; GPU recipe default"
				}
			}
			// Until unified memcg/GPU overlap is qualified on Spark, count both.
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
			if label != "" {
				source = label
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

// Transfer only the same frozen recipe/work/shape. Prefer the same OS/arch/
// accelerator/layout family; this is not a throughput or input-size model.
func (q *Queue) memoryEstimate(j *Job, m *Machine) (*AttemptMemoryPeak, float64, string) {
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
		return nil, 0, ""
	}
	var exact, family, other *AttemptMemoryPeak
	invalidated := false
	configuration := memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
	layout := "separate"
	if m.UnifiedMemory && !j.Spec.Needs.cpuOnly() {
		layout = "unified"
	}
	for _, point := range q.s.WorkObservations {
		k, p := point.Key, point.MemoryPeak
		age := q.clock.Now().Sub(point.MemoryRecordedAt)
		if sameMemoryWork(point, *key, configuration) && point.StaleReason == "" && p != nil && p.Constrained && point.MemoryRecordedAt.IsZero() {
			invalidated = true
		}
		if !sameMemoryWork(point, *key, configuration) || point.StaleReason != "" || point.MemoryRecordedAt.IsZero() || age < 0 || age > workCurveMaxAge || p == nil || p.Constrained || p.HostGB == nil || !validRate(*p.HostGB) || !validRate(*p.HostGB*2.5) {
			continue
		}
		if m.UnifiedMemory && !j.Spec.Needs.cpuOnly() && (p.DeviceGB == nil || !validRate(*p.DeviceGB)) {
			continue
		}
		sameFamily := k.Target == key.Target && point.MemoryLayout != "" && point.MemoryLayout == layout
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
	if invalidated {
		return nil, 0, "exploratory (previous peak constrained)"
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
