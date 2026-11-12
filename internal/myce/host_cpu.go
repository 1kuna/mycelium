package myce

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

func (n ResourceNeeds) cpuOnly() bool {
	return len(n.Platform.Required) == 1 && n.Platform.Required[0] == "cpu"
}
func (n ResourceNeeds) cores() int { return max(1, n.CPUCores) }

// Lower the convenient CPU input to the existing GPU/RAM reservation vector.
// Stored CPU jobs have zero accelerator memory and an explicit cpu target;
// repeated validation/resolution never moves or charges RAM twice.
func normalizeResources(s *Spec) error {
	n := &s.Needs
	if n.Memory != "" && n.Memory != "auto" {
		return reject("job_memory_invalid", errors.New("needs.memory must be auto or omitted"))
	}
	if n.GPU != nil && n.GPU.Devices != nil {
		if len(n.GPU.Devices) == 0 {
			return reject("gpu_selector_invalid", errors.New("devices must be a nonempty set of canonical device IDs"))
		}
		seen := map[string]bool{}
		for _, id := range n.GPU.Devices {
			if !canonicalDeviceID(id) || seen[id] {
				return reject("gpu_selector_invalid", errors.New("devices must be distinct canonical device IDs"))
			}
			seen[id] = true
		}
	}
	if n.GPU != nil && n.GPU.Count != nil {
		if *n.GPU.Count > 1 {
			return reject("gpu_count_unsupported", errors.New("omit needs.gpu.count: state memory_gb, and work no single card holds gets several cards of one host"))
		}
		if *n.GPU.Count <= 0 {
			return reject("gpu_selector_invalid", errors.New("GPU count must be positive"))
		}
	}
	if !validMemoryValue(n.HostMemoryGB) {
		return reject("job_memory_invalid", errors.New("host_memory_gb must be finite and nonnegative"))
	}
	if n.CPUCores < 0 {
		return reject("job_capacity_invalid", errors.New("needs.cpu_cores must be nonnegative (omitted defaults to one)"))
	}
	if n.CPUCores > 0 && !n.Platform.Any && len(n.Platform.Required)+len(n.Platform.Preferred) == 0 {
		n.Platform = Platform{Required: []string{"cpu"}}
	}
	if n.cpuOnly() {
		if n.GPU != nil {
			return reject("gpu_selector_invalid", errors.New("CPU work cannot select a GPU"))
		}
		if n.Model != "" {
			return reject("job_capacity_invalid", errors.New("CPU commands cannot request a GPU model"))
		}
		if !validMemoryValue(n.MemoryGB) {
			return reject("job_memory_invalid", errors.New("CPU memory_gb must be finite and nonnegative"))
		}
		n.HostMemoryGB = max(n.HostMemoryGB, n.MemoryGB)
		n.MemoryGB = 0
	}
	return nil
}

func cpuCapacity(m *Machine) (int, string) {
	p := m.CapabilityProfile
	if p == nil || p.LogicalCPUs <= 0 {
		return 0, "logical CPU capacity unavailable"
	}
	capacity := max(0, p.LogicalCPUs-m.CPUReserveCores)
	if m.CPUBudgetCores > 0 {
		capacity = min(capacity, m.CPUBudgetCores)
	}
	return capacity, ""
}

// Each physical host has one vector, projected onto its worker views. R1 still
// runs one batch per worker; R2 can admit a collection against this same ledger.
func (q *Queue) refreshHostCPU() {
	for _, m := range q.s.Machines {
		capacity, reason := cpuCapacity(m)
		policy := q.s.HostMaintenance[hostID(m)]
		for _, other := range q.s.Machines {
			if hostID(other) != hostID(m) || other.Gone {
				continue
			}
			n, why := cpuCapacity(other)
			capacity = min(capacity, n)
			if why != "" {
				reason = why
			}
			if policy.CPUBudgetCores != nil && other.CPUBudgetCores != *policy.CPUBudgetCores {
				reason = "CPU allowance awaiting worker acknowledgement"
			}
		}
		if policy.CPUBudgetCores != nil && *policy.CPUBudgetCores > 0 {
			capacity = min(capacity, *policy.CPUBudgetCores)
		}
		used := q.hostCPUInUse(m, nil, nil, false)
		m.cpuLedger = true
		m.EffectiveCPUCores, m.CPUInUseCores = capacity, used
		m.FreeCPUCores, m.CPUCapacityReason = max(0, capacity-used), reason
	}
}

// Live admission and release projections share this ledger. Environment builds
// run beside unrelated CPU commands and retain their own single core.
func (q *Queue) hostCPUInUse(m *Machine, releasedJobs map[*Job]bool, releasedSlots map[*ModelSlot]bool, releasePreparation bool) int {
	used := 0
	covered := map[string]bool{}
	for _, j := range q.liveJobs() {
		owner := q.s.Machines[j.Machine]
		if !releasedJobs[j] && !j.ReleaseConfirmed && owner != nil && hostID(owner) == hostID(m) && (j.State == "running" || j.State == "stopping") {
			used += jobCPUCharge(j, m)
			covered[j.Machine] = true
		}
	}
	for _, r := range q.s.HostReleases {
		if r.Host != hostID(m) {
			continue
		}
		j := q.s.Jobs[r.Job]
		if j != nil && releasedJobs[j] && j.Machine == r.Machine && j.Session == r.Session && j.Attempt == r.Attempt {
			continue
		}
		if j != nil && j.Machine == r.Machine && j.Session == r.Session && j.Attempt == r.Attempt && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") {
			continue
		}
		used += max(1, r.CPUCores) // Older receipts predate the one-core default.
	}
	for _, s := range q.s.ModelSlots {
		owner := q.s.Machines[s.Machine]
		if !releasedSlots[s] && owner != nil && hostID(owner) == hostID(m) && slotReservesMemory(s) {
			used++ // One admitted core per GPU model residency, shared by its calls.
			covered[s.Machine] = true
		}
	}
	for _, p := range q.s.PreparationPlans {
		owner := q.s.Machines[p.Machine]
		if p.Started && !(releasePreparation && p.Machine == m.ID) && owner != nil && hostID(owner) == hostID(m) {
			if p.CPUCores > 0 {
				used += p.CPUCores
			} else if !covered[p.Machine] {
				used++ // Legacy/shared preparation inherits one execution core.
				covered[p.Machine] = true
			}
		}
	}
	return used
}

func cpuFits(j *Job, m *Machine) bool {
	if m.CapabilityProfile == nil || m.CapabilityProfile.LogicalCPUs <= 0 {
		return false
	}
	if j.Spec.Needs.cpuOnly() && cpuTargetReason(m) != "" {
		return false
	}
	if m.CPUCapacityReason != "" {
		return false
	}
	if !m.cpuLedger {
		capacity, reason := cpuCapacity(m)
		return reason == "" && capacity >= jobCPUCharge(j, m)
	}
	if j.State == "running" || j.State == "stopping" {
		return m.CPUInUseCores <= m.EffectiveCPUCores
	}
	return m.FreeCPUCores >= jobCPUCharge(j, m)
}

type batchCPUKey struct {
	host string
	tier int
}

// A necessary condition only: imagine every model/build, stopping attempt,
// resumable/preemptible job and lower-tier job could release immediately. Even
// that deliberately generous projection cannot reclaim a running, non-yielding
// same/higher-tier command. If its residual cores cannot fit the demand, memory
// sizing and the complete release planner cannot produce an eligible candidate.
// No release or placement is authorized here; all successful cases still use
// the existing planner. Unknown ledgers and capacity-refusal projections fall
// through to that planner without this shortcut.
func (q *Queue) batchCPUCeilingFits(j *Job, m *Machine, scans ...*placementScan) bool {
	if !m.cpuLedger || len(j.LaunchCapacityWait) > 0 {
		return true
	}
	key := batchCPUKey{hostID(m), q.priorityMatch(j.Spec).Index}
	var cache map[batchCPUKey]int
	if len(scans) > 0 {
		if scans[0].unreleasableCPU == nil {
			scans[0].unreleasableCPU = map[batchCPUKey]int{}
		}
		cache = scans[0].unreleasableCPU
	}
	blocked, found := cache[key]
	if !found {
		for _, r := range q.liveJobs() {
			if r.State != "running" || r.ReleaseConfirmed || r.Spec.Resumable || r.Spec.Preemptible || cancelled(r) || priorityStop(r) != nil || q.comparePriority(r.Spec, j.Spec) > 0 {
				continue
			}
			if owner := q.s.Machines[r.Machine]; owner != nil && hostID(owner) == key.host {
				blocked += r.Spec.Needs.cores()
			}
		}
		if cache != nil {
			cache[key] = blocked
		}
	}
	return j.Spec.Needs.cores() <= max(0, m.EffectiveCPUCores-blocked)
}

// Planning may credit only the exact releases chosen by the existing planner.
// The live ledger stays charged until acknowledgement, including release debt.
func (q *Queue) cpuAfterReleases(m *Machine, jobs []*Job, slots []*ModelSlot, preparationCores int) Machine {
	copy := *m
	releasedJobs, releasedSlots := map[*Job]bool{}, map[*ModelSlot]bool{}
	for _, j := range jobs {
		releasedJobs[j] = true
	}
	for _, s := range slots {
		releasedSlots[s] = true
	}
	copy.CPUInUseCores = max(0, q.hostCPUInUse(m, releasedJobs, releasedSlots, preparationCores > 0))
	copy.FreeCPUCores = max(0, copy.EffectiveCPUCores-copy.CPUInUseCores)
	return copy
}

// CPU capacity that holds this job on every eligible host. A host that cannot
// isolate CPU work from its accelerators makes it a CPU isolation wait.
func cpuWait(j *Job, machines map[string]*Machine) *Hold {
	reasons := []string{}
	kind := "capacity"
	for _, m := range machines {
		if m.Gone || m.Leaving || !j.Spec.allowsMachine(m.ID) || !environmentCanRun(j, m) {
			continue
		}
		if !j.Spec.Needs.cpuOnly() && !j.Spec.backendAllows(m.GPU) {
			continue
		}
		if cpuFits(j, m) {
			return nil
		}
		reason := m.CPUCapacityReason
		if j.Spec.Needs.cpuOnly() && cpuTargetReason(m) != "" {
			reason = cpuTargetReason(m)
			if cpuIsolationDiagnostic(m.CPUIsolation) == cpuIsolationUnavailable {
				kind = "cpu_isolation"
			}
		}
		if reason == "" {
			_, reason = cpuCapacity(m)
		}
		if reason == "" {
			reason = fmt.Sprintf("%d cores unreserved of %d admitted (%d charged; shared hosts include measured CPU overuse)", m.FreeCPUCores, m.EffectiveCPUCores, m.CPUInUseCores)
		}
		reasons = append(reasons, m.ID+": "+reason)
	}
	slices.Sort(reasons)
	if len(reasons) == 0 {
		if !j.Spec.Needs.cpuOnly() {
			return nil
		}
		return newHold("capacity", "waiting for CPU capacity: no eligible Linux/macOS host online")
	}
	return newHold(kind, fmt.Sprintf("waiting for CPU capacity (%d cores): %s", j.Spec.Needs.cores(), strings.Join(reasons, "; ")))
}

func cpuTargetReason(m *Machine) string {
	if cpuIsolationDiagnostic(m.CPUIsolation) == cpuIsolationUnavailable {
		return cpuIsolationUnavailable + ": " + rejectionMessages[cpuIsolationUnavailable]
	}
	if m.CapabilityProfile == nil || (m.CapabilityProfile.OS != "linux" && m.CapabilityProfile.OS != "darwin") {
		return "CPU execution requires Linux or macOS (Windows/WSL qualification is pending)"
	}
	return ""
}

func cpuAllocationFits(m *Machine, cores int) bool {
	if m.CapabilityProfile == nil || m.CapabilityProfile.LogicalCPUs <= 0 {
		return false
	}
	if m.CPUCapacityReason != "" {
		return false
	}
	if !m.cpuLedger {
		n, why := cpuCapacity(m)
		return why == "" && n >= cores
	}
	return m.FreeCPUCores >= cores
}
func (q *Queue) workerHasExecutionCPUReservation(m *Machine) bool {
	for _, j := range q.liveJobs() {
		if j.Machine == m.ID && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") {
			return true
		}
	}
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && slotReservesMemory(s) {
			return true
		}
	}
	return false
}
func (q *Queue) workerHasCPUReservation(m *Machine) bool {
	if q.workerHasExecutionCPUReservation(m) {
		return true
	}
	for _, p := range q.s.PreparationPlans {
		if p.Machine == m.ID && p.Started && p.CPUCores == 0 {
			return true
		}
	}
	return false
}
