package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
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
	original := j
	needsBefore := j.Spec.Needs
	candidate := *j
	j = &candidate
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
	if j.Spec.Needs.MemoryGB != needsBefore.MemoryGB || j.Spec.Needs.HostMemoryGB != needsBefore.HostMemoryGB {
		if q.s.Jobs[j.ID] == original {
			q.touchJob(j.ID)
		}
		original.Spec.Needs = j.Spec.Needs
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
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	if j.MemoryEnvelope != nil && boundedMemorySource(j.MemoryEnvelope.Source) {
		j.State, j.Reason = "failed", j.MemoryEnvelope.Source+" exhausted after out of memory; "+memoryCapacityRemedies
		return
	}
	if j.Spec.Needs.Memory == "auto" && (j.Spec.Needs.cpuOnly() || j.Spec.ExtensionCheck != nil && hostOOMAttempt(j)) {
		q.retryHostOOM(j)
		return
	}
	if j.Spec.Needs.Memory == "auto" {
		// A bounded first run doubles without spending the one retry; at half
		// the allowance, host-wide exploration and that retry take over.
		bounded := j.MemoryEnvelope != nil && strings.Contains(j.MemoryEnvelope.Source, boundedFirstRun)
		if j.OOMRetries > 0 && !bounded {
			j.State, j.Reason = "failed", "out of memory after one retry above the OOM envelope"
			return
		}
		growth := 2.0
		if !bounded {
			j.OOMRetries++
			growth = 1.25
		}
		j.MemoryOOMFloor = &SizeHint{MemoryGB: j.Spec.Needs.MemoryGB, HostMemoryGB: j.Spec.Needs.HostMemoryGB * growth}
		if floor := footprintOOMFloor(j); floor != nil {
			j.MemoryOOMFloor = floor
		}
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
	if why := q.oomCapacityDeadEnd(j); why != "" {
		j.State, j.Reason = "failed", why
		return
	}
	q.retry(j, "out of memory; waiting for one retry on a larger hypha")
}

// oomCapacityDeadEnd: the job waits for a strictly larger host than the one it
// ran out of memory on, and no known host (online or not) is larger. Hosts
// with an unknown capability profile could be larger, so they keep the wait.
func (q *Queue) oomCapacityDeadEnd(j *Job) string {
	if j.OOMCapacity == nil {
		return ""
	}
	// retryOOM runs before requeue; judge the job as the next placement will.
	probe := *j
	probe.State, probe.MemoryEnvelope = "queued", nil
	j = &probe
	for _, owner := range q.s.Machines {
		if owner.Replacement != nil || !q.jobAllowsMachine(j, owner.ID) {
			continue
		}
		if owner.CapabilityProfile == nil {
			return ""
		}
		for _, m := range q.placementTargets(owner) {
			for _, candidate := range extensionJobs(j, m, false) {
				required := candidate.Spec.Needs.Platform.Required
				if (len(required) == 0 || slices.Contains(required, m.GPU)) && capabilityFits(candidate, m) {
					return ""
				}
			}
		}
	}
	old := j.OOMCapacity
	return fmt.Sprintf("dead end: out of memory, and no known host is larger than the one it ran on (%.1f GiB accelerator-reachable, %.1f GiB system); action: reduce the workload's memory use and resubmit", old.MemoryGB, old.HostMemoryGB)
}

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
func (q *Queue) retryHostOOM(j *Job) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	need := j.Spec.Needs.HostMemoryGB
	if f := j.MemoryOOMFloor; f != nil {
		need = max(need, f.HostMemoryGB)
	}
	for _, facts := range j.MemoryAttempts {
		if facts.Attempt != j.Attempt || facts.Machine != j.Machine || facts.Session != j.Session {
			continue
		}
		if p := facts.MemoryPeak; p != nil && p.HostGB != nil && validRate(*p.HostGB) {
			peak := hostMemoryPeakGB(p)
			need = max(need, peak)
		}
	}
	j.MemoryOOMFloor = &SizeHint{MemoryGB: j.Spec.Needs.MemoryGB, HostMemoryGB: need + minimumHostMemoryGB}
	j.OOMCapacity = nil
	j.OOMRetries++
	probe := cloneJob(j)
	probe.State, probe.MemoryEnvelope = "queued", nil
	probe.Spec.Needs.HostMemoryGB = j.MemoryOOMFloor.HostMemoryGB
	if ok, _ := q.qualifiedTargetMemory(&probe, false); !ok {
		j.State, j.Reason = "failed", fmt.Sprintf("needs more memory than any eligible host allows (%.3f GiB measured OOM lower bound)", j.MemoryOOMFloor.HostMemoryGB)
		return
	}
	q.requeue(j, "out of memory; waiting for measured peak plus growth and physical headroom", false)
}

// GPU allocation failure is not evidence of a host-RAM shortage. Only local
// memcg OOM events select host recovery for an accelerator extension check.
func hostOOMAttempt(j *Job) bool {
	for _, f := range j.MemoryAttempts {
		if f.Attempt == j.Attempt && f.Machine == j.Machine && f.Session == j.Session && f.MemoryPeak != nil && f.MemoryPeak.Events != nil {
			e := f.MemoryPeak.Events
			return e.OOM > 0 || e.OOMKill > 0 || e.OOMGroupKill > 0
		}
	}
	return false
}
