package myce

import (
	"fmt"
)

// Memory needs name accelerator-reachable memory and additional system memory.
// A profile bounds capacity; live free-memory reports still bound admission.
func capabilityCapacity(m *Machine) (accelerator, system float64, ok bool) {
	p := m.CapabilityProfile
	if p == nil {
		return 0, 0, false
	}
	if p.MemoryLayout != "unified" && p.MemoryLayout != "separate" {
		return 0, 0, false
	}
	system = float64(p.SystemMemoryBytes) / (1 << 30)
	for _, a := range p.Accelerators {
		if a.Type == m.GPU {
			accelerator = max(accelerator, float64(a.MemoryBytes)/(1<<30))
		}
	}
	// Fake resources explicitly use an operator allowance, never a native guess.
	if m.GPU == "fake" && accelerator == 0 {
		accelerator = m.TotalMemoryGB
	} else if len(m.groupMembers) > 0 {
		accelerator *= float64(len(m.groupMembers))
	}
	if p.MemoryLayout == "unified" {
		accelerator = min(accelerator, system)
	}
	return accelerator, system, (accelerator > 0 || m.GPU == "cpu") && system > 0
}

func capabilityUnavailableReason(m *Machine) string {
	if m.CapabilityProfile == nil {
		return "hardware capability profile unavailable"
	}
	if p := m.CapabilityProfile; p.MemoryLayout != "unified" && p.MemoryLayout != "separate" {
		return "memory layout unavailable"
	}
	_, system, ok := capabilityCapacity(m)
	if system == 0 {
		return "system RAM capacity unavailable"
	}
	if !ok {
		return "accelerator memory capacity unavailable"
	}
	return ""
}

func capabilityFits(j *Job, m *Machine) bool {
	if !j.Spec.extensionJob() && !j.Spec.Needs.cpuOnly() {
		return true
	}
	accelerator, system, ok := jobCapabilityCapacity(j, m)
	systemNeed := j.Spec.Needs.HostMemoryGB
	if m.CapabilityProfile != nil && m.CapabilityProfile.MemoryLayout == "unified" {
		systemNeed += j.Spec.Needs.MemoryGB
	}
	if !ok || accelerator < j.Spec.Needs.MemoryGB || system < systemNeed {
		return false
	}
	if j.OOMCapacity != nil {
		// Larger means no smaller in either pool, strictly larger in at least one.
		old := j.OOMCapacity
		return accelerator >= old.MemoryGB && system >= old.HostMemoryGB && (accelerator > old.MemoryGB || system > old.HostMemoryGB)
	}
	return true
}

func jobCapabilityCapacity(j *Job, m *Machine) (accelerator, system float64, ok bool) {
	if j.Spec.Needs.cpuOnly() {
		copy := *m
		copy.GPU = "cpu"
		return capabilityCapacity(&copy)
	}
	return capabilityCapacity(m)
}

// Profile v0 has no throughput measurement. Memory-first ordering is provisional
// until #33 supplies measurements. IDs make equal profiles deterministic.
func capabilityBetter(a, b *Machine) bool {
	aa, as, aok := capabilityCapacity(a)
	ba, bs, bok := capabilityCapacity(b)
	if aok != bok {
		return aok
	}
	if aa != ba {
		return aa > ba
	}
	if as != bs {
		return as > bs
	}
	if a.CapabilityProfile != nil && b.CapabilityProfile != nil && a.CapabilityProfile.LogicalCPUs != b.CapabilityProfile.LogicalCPUs {
		return a.CapabilityProfile.LogicalCPUs > b.CapabilityProfile.LogicalCPUs
	}
	return a.ID < b.ID
}

// NAS references must be immutable. Input content addressing belongs to #24.
func capabilityWaitingReason(j *Job) string {
	if j.OOMCapacity != nil {
		return fmt.Sprintf("waiting for a larger qualified hypha after out of memory (at least %.1f GiB accelerator-reachable and %.1f GiB system, with one pool larger)", j.OOMCapacity.MemoryGB, j.OOMCapacity.HostMemoryGB)
	}
	return ""
}

// A killed peak is a lower bound, not a successful demand estimate. Retain
// the killed peak and previous reservation; uncertainty is sized separately,
// bounded by target headroom, rather than becoming a permanent requirement.
// Strictly larger floors prevent another execution of the same failed bound;
// physical qualification, rather than an attempt count, bounds a leaking job.
