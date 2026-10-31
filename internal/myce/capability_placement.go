package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
func programNeedsKey(j *Job, e Extension, platform string) string {
	if j.Spec.Program == nil {
		return ""
	}
	data, _ := json.Marshal(struct {
		Project             string
		Program             *Program
		Inputs              map[string]string
		Files               FileManifest `json:",omitempty"`
		Env                 map[string]string
		Extension, Platform string
	}{j.Spec.Project, j.Spec.Program, j.Spec.Inputs, j.Spec.Files, j.Spec.Run.Env, e.Digest, platform})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (q *Queue) applyLearnedNeeds(j *Job) {
	if j.Spec.Program == nil {
		return
	}
	for _, e := range j.Extensions {
		for _, p := range e.platforms() {
			if len(j.Spec.Needs.Platform.Required) > 0 && !contains(j.Spec.Needs.Platform.Required, p) {
				continue
			}
			if needs, ok := q.s.ProgramNeeds[programNeedsKey(j, e, p)]; ok {
				if j.Spec.Needs.cpuOnly() {
					j.Spec.Needs.HostMemoryGB = max(j.Spec.Needs.HostMemoryGB, needs.MemoryGB)
				} else {
					j.Spec.Needs.MemoryGB = max(j.Spec.Needs.MemoryGB, needs.MemoryGB)
				}
				j.Spec.Needs.HostMemoryGB = max(j.Spec.Needs.HostMemoryGB, needs.HostMemoryGB)
			}
		}
	}
}

func (q *Queue) unmeasuredProgram(j *Job, m *Machine) bool {
	if j.Spec.Program == nil || j.OOMRetries > 0 {
		return false
	}
	e := qualifiedExtension(j, m)
	if e == nil {
		return false
	}
	_, learned := q.s.ProgramNeeds[programNeedsKey(j, *e, m.GPU)]
	return !learned
}

func (q *Queue) retryOOM(j *Job) {
	if j.MemoryEnvelope != nil && j.MemoryEnvelope.Source == boundedMemoryExploration {
		j.State, j.Reason = "failed", "bounded cross-OS exploration exhausted after out of memory; "+memoryCapacityRemedies
		return
	}
	if j.Spec.Needs.Memory == "auto" {
		if j.OOMRetries > 0 {
			j.State, j.Reason = "failed", "out of memory after one retry above the OOM envelope"
			return
		}
		j.OOMRetries++
		j.MemoryOOMFloor = &SizeHint{MemoryGB: j.Spec.Needs.MemoryGB, HostMemoryGB: j.Spec.Needs.HostMemoryGB * 1.25}
		j.OOMCapacity = nil
		probe := cloneJob(j)
		probe.State, probe.MemoryEnvelope = "queued", nil
		probe.Spec.Needs.MemoryGB, probe.Spec.Needs.HostMemoryGB = j.MemoryOOMFloor.MemoryGB, j.MemoryOOMFloor.HostMemoryGB
		if !q.hasQualifiedTarget(&probe) {
			need := max(probe.Spec.Needs.MemoryGB, probe.Spec.Needs.HostMemoryGB)
			if m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices); m != nil {
				need = max(need, hostNeed(m, probe.Spec.Needs.MemoryGB, probe.Spec.Needs.HostMemoryGB))
			}
			j.State, j.Reason = "failed", fmt.Sprintf("needs more memory than any eligible host allows (%.1f GiB)", need)
			return
		}
		q.requeue(j, "out of memory; waiting for room above the previous envelope", false)
		return
	}
	if j.OOMRetries > 0 {
		j.State, j.Reason = "failed", "out of memory after one retry on a larger hypha"
		return
	}
	m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	if m == nil {
		j.State, j.Reason = "failed", "out of memory; previous capability unavailable"
		return
	}
	accelerator, system, ok := jobCapabilityCapacity(j, m)
	if !ok {
		j.State, j.Reason = "failed", "out of memory; previous capability unavailable"
		return
	}
	j.OOMRetries++
	j.OOMCapacity = &SizeHint{MemoryGB: accelerator, HostMemoryGB: system}
	// Failed observations are lower bounds, never successful learned needs.
	j.Spec.Needs.MemoryGB *= 2
	j.Spec.Needs.HostMemoryGB *= 2
	j.ProductionHost = ""
	q.retry(j, "out of memory; waiting for one retry on a larger hypha")
}

func capabilityWaitingReason(j *Job) string {
	if j.OOMCapacity != nil {
		return fmt.Sprintf("waiting for a larger qualified hypha after out of memory (at least %.1f GiB accelerator-reachable and %.1f GiB system, with one pool larger)", j.OOMCapacity.MemoryGB, j.OOMCapacity.HostMemoryGB)
	}
	return ""
}
