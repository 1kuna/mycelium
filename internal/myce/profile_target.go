package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Prefer eligible targets, then the fewest failed checks, then the smallest
// proportional resource shortfall. Machine/device names break equal-fit ties.
type targetFit struct {
	excluded   bool
	first      string
	failures   int
	ineligible int
	shortfall  float64
	key        string
}

func (f *targetFit) check(ok bool, dimension string) bool {
	if f == nil {
		return ok
	}
	if !ok {
		if f.first == "" {
			f.first = dimension
		}
		f.failures++
		switch dimension {
		case "target", "platform", "device", "environment", "extension", "allow-list", "native network isolation":
			f.ineligible++
		}
	}
	return true // Collect all failures in this evaluation.
}

func (f targetFit) better(other targetFit) bool {
	if f.excluded != other.excluded {
		return !f.excluded
	}
	if f.ineligible != other.ineligible {
		return f.ineligible < other.ineligible
	}
	if f.failures != other.failures {
		return f.failures < other.failures
	}
	if f.shortfall != other.shortfall {
		return f.shortfall < other.shortfall
	}
	return f.key < other.key
}

func resourceShortfall(need, capacity float64) float64 {
	if need <= 0 {
		return 0
	}
	return max(0, need-capacity) / need
}

func targetFitReason(dimension string, j *Job, m *Machine) string {
	n := j.Spec.Needs
	target := machineTarget(m)
	if n.cpuOnly() {
		target.Platform = "cpu"
	}
	formatTarget := func(t ExecutionTarget) string {
		// Only identifier fields are published; never arbitrary worker errors,
		// environment lock paths, command arguments or recipe contents.
		parts := []string{t.OS, t.Arch, t.Platform}
		for i, p := range parts {
			if !extensionName.MatchString(p) {
				parts[i] = "unknown"
			}
		}
		return strings.Join(parts, "/")
	}
	switch dimension {
	case "native network isolation":
		if !j.Spec.Needs.Network {
			return "native_isolation_unavailable: this host has not reported a supported native kernel boundary; upgrade or repair the worker"
		}
		return "needs.network: true requires protected native outbound execution; this host only supports offline native jobs or has not reported isolation capability"
	case "disk headroom":
		return diskHeadroomReason(j, m)
	case "target":
		if !sharedInputsFit(j.Spec, m) {
			return "shared_inputs_v1 worker capability required"
		}
		required := make([]string, 0, len(j.Spec.Targets))
		for _, t := range j.Spec.Targets {
			if t.Arch == "" {
				t.Arch = "any"
			}
			if t.Platform == "" {
				t.Platform = "any"
			}
			required = append(required, formatTarget(t))
		}
		slices.Sort(required)
		return fmt.Sprintf("target %s does not match required %s", formatTarget(target), strings.Join(required, " or "))
	case "platform":
		required := slices.Clone(n.Platform.Required)
		slices.Sort(required)
		if len(required) == 0 {
			required = []string{"a GPU platform"}
		}
		return fmt.Sprintf("platform %s does not match required %s", formatTarget(target), strings.Join(required, " or "))
	case "host memory":
		base := n.HostMemoryGB
		if m.UnifiedMemory && m.HostMemoryMeasured {
			base += n.MemoryGB
		}
		return fmt.Sprintf("host memory need %g GiB (%g + %g slack) exceeds allowance %g GiB", hostNeed(m, n.MemoryGB, n.HostMemoryGB), base, m.MemoryCeilingSlackGB, m.FreeHostMemoryGB)
	case "GPU memory":
		return fmt.Sprintf("GPU memory need %g GiB exceeds allowance %g GiB", n.MemoryGB, m.TotalMemoryGB)
	case "cores":
		reason := ""
		if n.cpuOnly() {
			reason = cpuTargetReason(m)
		}
		if reason != "" {
			return "CPU target unqualified: " + reason
		}
		return fmt.Sprintf("CPU need %d cores exceeds allowance %d cores (logical %d, reserve %d, configured budget %d)", n.cores(), m.EffectiveCPUCores, logicalCPUs(m), m.CPUReserveCores, m.CPUBudgetCores)
	case "device":
		devices := []string{}
		if n.GPU != nil {
			devices = slices.Clone(n.GPU.Devices)
		}
		slices.Sort(devices)
		device := m.Device
		if device != "" && !canonicalDeviceID(device) {
			device = "unknown"
		}
		return fmt.Sprintf("device %q does not match required devices %v or CPU select-none target", device, devices)
	case "allow-list":
		return "machine excluded by job or project allow-list"
	case "environment":
		key := environmentKey(j, m)
		if key == "" {
			platform := m.GPU
			if !validGPU(platform) {
				platform = "unknown"
			}
			return "environment has no recipe for platform " + platform
		} else if !extensionName.MatchString(key) {
			key = "private recipe"
		}
		return fmt.Sprintf("environment recipe %q unavailable for %s (installed=false, builder=%t)", key, formatTarget(target), m.EnvironmentBuilder)
	case "hardware capacity":
		accelerator, system, _ := jobCapabilityCapacity(j, m)
		systemNeed := n.HostMemoryGB
		if m.CapabilityProfile != nil && m.CapabilityProfile.MemoryLayout == "unified" {
			systemNeed += n.MemoryGB
		}
		return fmt.Sprintf("hardware capacity insufficient or unqualified: need %g GiB GPU and %g GiB host, capacity %g GiB GPU and %g GiB host", n.MemoryGB, systemNeed, accelerator, system)
	case "memory envelope":
		return "memory envelope unavailable within target allowance: " + j.MemoryEnvelope.Refusal
	case "extension":
		return "no qualified extension variant for " + formatTarget(target)
	default:
		return dimension + " prevents execution"
	}
}

func logicalCPUs(m *Machine) int {
	if m.CapabilityProfile != nil {
		return m.CapabilityProfile.LogicalCPUs
	}
	return 0
}

func executionTargetMatches(s Spec, m *Machine) bool {
	if !sharedInputsFit(s, m) {
		return false
	}
	if len(s.Targets) == 0 {
		return true
	}
	target := machineTarget(m)
	if s.Needs.cpuOnly() {
		target.Platform = "cpu"
	}
	return slices.ContainsFunc(s.Targets, func(requirement ExecutionTarget) bool {
		return requirement.matches(target)
	})
}

// Submission checks qualification and total admitted capacity, not free slots.
// Busy qualified hosts can queue work. Dispatch and launch still check live
// admission through fitsResources; this check grants no capacity reservation.
func (q *Queue) hasQualifiedTarget(j *Job) bool {
	ok, _ := q.qualifiedTarget(j)
	return ok
}

func (q *Queue) qualifiedTarget(j *Job) (bool, string) {
	// Resolve envelopes with the planner's code, but without temporary
	// occupancy or release holds. Retain observations for learned needs.
	capacity := Queue{clock: q.clock, s: q.s}
	capacity.s.Jobs, capacity.s.ModelSlots, capacity.s.HostReleases = nil, nil, nil
	var best *targetFit
	reason := "no known machine targets"
	consider := func(fit targetFit, candidate *Job, m *Machine) {
		fit.key = targetKey(m)
		if best == nil || fit.better(*best) {
			best = &fit
			reason = targetFitReason(fit.first, candidate, m)
			if fit.first == "allow-list" {
				jobAllowed := slices.Clone(j.Spec.AllowedMachines)
				projectAllowed := slices.Clone(q.s.Policies[j.Spec.Project].AllowedMachines)
				slices.Sort(jobAllowed)
				slices.Sort(projectAllowed)
				reason += fmt.Sprintf(" (job=%v, project=%v; empty means unrestricted)", jobAllowed, projectAllowed)
			}
			reason += " on " + m.ID
		}
	}
	for _, m := range q.s.Machines {
		if !q.jobAllowsMachine(j, m.ID) {
			fit := targetFit{excluded: true}
			fit.check(false, "allow-list")
			consider(fit, j, m)
			continue
		}
		for _, target := range q.machineTargets(m) {
			copy := cloneMachine(target)
			copy.DiskVolumes = diskCapacityVolumes(copy.DiskVolumes)
			copy.Gone, copy.Leaving, copy.Recovering, copy.CheckpointUnavailable = false, false, false, false
			copy.MemoryError, copy.HostMemoryError, copy.CPUCapacityReason = "", "", ""
			copy.HostMemoryPolicyMismatch = false
			policy := q.s.HostMaintenance[hostID(m)]
			if policy.CPUBudgetCores != nil {
				copy.CPUBudgetCores = *policy.CPUBudgetCores
			}
			copy.EffectiveCPUCores, _ = cpuCapacity(&copy)
			if availability := machineAvailability(&copy); availability != nil {
				availability.Available = true
			}
			copy.cpuLedger = true
			copy.CPUInUseCores = 0
			copy.FreeCPUCores = copy.EffectiveCPUCores
			if copy.CapabilityProfile != nil {
				copy.FreeHostMemoryGB = float64(copy.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
				allowance := copy.HostMemoryBudgetGB
				if policy.MemoryBudget != nil {
					allowance = policy.MemoryBudget.GiB
				}
				copy.EffectiveHostMemoryBudgetGB = allowance
				if allowance > 0 {
					copy.FreeHostMemoryGB = min(copy.FreeHostMemoryGB, allowance)
				}
				copy.HostMemoryMeasured = true
			}
			copy.FreeMemoryGB = copy.TotalMemoryGB
			// Observation identity must see the same available target copy,
			// including when the live owner is temporarily unavailable.
			capacity.s.Machines = map[string]*Machine{copy.ID: &copy}
			candidates, refusal := extensionJobsWithRefusal(j, &copy, true)
			if len(candidates) == 0 {
				fit := targetFit{}
				fit.check(false, refusal)
				consider(fit, j, &copy)
			}
			for _, candidate := range candidates {
				candidate = capacity.memoryCandidate(candidate, &copy)
				fit := targetFit{}
				fitsResourcesChecked(candidate, &copy, copy.TotalMemoryGB, &fit)
				hostFailure := hostFitFailure(&copy, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB)
				fit.check(hostFailure == "", hostFailure)
				if fit.failures == 0 {
					return true, ""
				}
				fit.shortfall = resourceShortfall(hostNeed(&copy, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB), copy.FreeHostMemoryGB) + resourceShortfall(candidate.Spec.Needs.MemoryGB, copy.TotalMemoryGB) + resourceShortfall(float64(candidate.Spec.Needs.cores()), float64(copy.EffectiveCPUCores))
				consider(fit, candidate, &copy)
			}
		}
	}
	return false, reason
}
