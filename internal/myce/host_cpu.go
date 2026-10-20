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
		used := 0
		covered := map[string]bool{}
		for _, j := range q.s.Jobs {
			owner := q.s.Machines[j.Machine]
			if !j.ReleaseConfirmed && owner != nil && hostID(owner) == hostID(m) && (j.State == "running" || j.State == "stopping") {
				used += j.Spec.Needs.cores()
				covered[j.Machine] = true
			}
		}
		for _, r := range q.s.HostReleases {
			if r.Host != hostID(m) {
				continue
			}
			j := q.s.Jobs[r.Job]
			if j != nil && j.Machine == r.Machine && j.Session == r.Session && j.Attempt == r.Attempt && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") {
				continue
			}
			used += max(1, r.CPUCores) // Older receipts predate the one-core default.
		}
		for _, s := range q.s.ModelSlots {
			owner := q.s.Machines[s.Machine]
			if owner != nil && hostID(owner) == hostID(m) && slotReservesMemory(s) {
				used++ // One admitted core per GPU model residency, shared by its calls.
				covered[s.Machine] = true
			}
		}
		for _, p := range q.s.PreparationPlans {
			owner := q.s.Machines[p.Machine]
			if p.Started && owner != nil && hostID(owner) == hostID(m) && !covered[p.Machine] {
				used++ // Preparation on a reserved execution is already charged once.
				covered[p.Machine] = true
			}
		}
		m.cpuLedger = true
		m.EffectiveCPUCores, m.CPUInUseCores = capacity, used
		m.FreeCPUCores, m.CPUCapacityReason = max(0, capacity-used), reason
	}
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
		return reason == "" && capacity >= j.Spec.Needs.cores()
	}
	if j.State == "running" || j.State == "stopping" {
		return m.CPUInUseCores <= m.EffectiveCPUCores
	}
	return m.FreeCPUCores >= j.Spec.Needs.cores()
}

// Planning may credit only the exact releases chosen by the existing planner.
// The live ledger stays charged until acknowledgement, including release debt.
func cpuAfterReleases(m *Machine, jobs []*Job, slots []*ModelSlot) Machine {
	copy := *m
	for _, j := range jobs {
		if !j.ReleaseConfirmed {
			copy.CPUInUseCores -= j.Spec.Needs.cores()
		}
	}
	for _, s := range slots {
		if slotReservesMemory(s) {
			copy.CPUInUseCores--
		}
	}
	copy.CPUInUseCores = max(0, copy.CPUInUseCores)
	copy.FreeCPUCores = max(0, copy.EffectiveCPUCores-copy.CPUInUseCores)
	return copy
}

func cpuWaitingReason(j *Job, machines map[string]*Machine) string {
	reasons := []string{}
	for _, m := range machines {
		if m.Gone || m.Leaving || !machineAllowed(j.Spec.AllowedMachines, m.ID) || !environmentCanRun(j, m) {
			continue
		}
		if !j.Spec.Needs.cpuOnly() && len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU) {
			continue
		}
		if cpuFits(j, m) {
			return ""
		}
		reason := m.CPUCapacityReason
		if j.Spec.Needs.cpuOnly() && cpuTargetReason(m) != "" {
			reason = cpuTargetReason(m)
		}
		if reason == "" {
			_, reason = cpuCapacity(m)
		}
		if reason == "" {
			reason = fmt.Sprintf("%d cores free of %d admitted", m.FreeCPUCores, m.EffectiveCPUCores)
		}
		reasons = append(reasons, m.ID+": "+reason)
	}
	slices.Sort(reasons)
	if len(reasons) == 0 {
		if !j.Spec.Needs.cpuOnly() {
			return ""
		}
		return "waiting for CPU capacity: no eligible Linux/macOS host online"
	}
	return fmt.Sprintf("waiting for CPU capacity (%d cores): %s", j.Spec.Needs.cores(), strings.Join(reasons, "; "))
}

func cpuTargetReason(m *Machine) string {
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
func (q *Queue) workerHasCPUExecution(m *Machine) bool {
	for _, j := range q.s.Jobs {
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
	if q.workerHasCPUExecution(m) {
		return true
	}
	for _, p := range q.s.PreparationPlans {
		if p.Machine == m.ID && p.Started {
			return true
		}
	}
	return false
}
