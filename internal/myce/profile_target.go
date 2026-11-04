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
	failed     []string
}

func (f *targetFit) check(ok bool, dimension string) bool {
	if f == nil {
		return ok
	}
	if !ok {
		f.failed = append(f.failed, dimension)
		if f.first == "" {
			f.first = dimension
		}
		f.failures++
		switch dimension {
		case "tools", "target", "platform", "device", "environment", "extension", "allow-list", "native network isolation":
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
	case "tools":
		return missingToolsReason(m.ID, missingTools(j, m))
	case "native network isolation":
		if !j.Spec.Needs.Network {
			return "native_isolation_unavailable: this host has not reported a supported native kernel boundary; upgrade or repair the worker"
		}
		return "needs.network: true requires native_network for protected native outbound execution; this host only supports offline native jobs or has not reported isolation capability"
	case "disk headroom":
		return diskHeadroomReason(j, m)
	case "shared host profile":
		return sharedHostRefusal(j, m)
	case "target":
		if !sharedInputsFit(j.Spec, m) {
			if len(j.Spec.ModelSnapshots) > 0 {
				return modelSnapshotsCapability + " worker capability required (a worker with a shared model root)"
			}
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
		if len(m.groupMembers) > 0 {
			return fmt.Sprintf("GPU memory need %g GiB exceeds %g GiB across all %d cards of this host; one job runs on one host", n.MemoryGB, m.TotalMemoryGB, len(m.groupMembers))
		}
		return fmt.Sprintf("GPU memory need %g GiB exceeds allowance %g GiB", n.MemoryGB, m.TotalMemoryGB)
	case "card group":
		if n.MemoryGB <= m.groupFloorGB {
			return fmt.Sprintf("one card holds %g GiB; work spreads across cards only when no single card can hold it", n.MemoryGB)
		}
		return fmt.Sprintf("this host holds %g GiB only across %d cards; Docker, model-server and extension-check jobs run on one card", n.MemoryGB, len(m.groupMembers))
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
	case "excluded_machines":
		return "excluded by job excluded_machines"
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
	var best *targetFit
	reason := "no known machine targets"
	q.evaluateTargets(j, func(fit targetFit, candidate *Job, m *Machine) bool {
		if fit.failures == 0 {
			best, reason = nil, ""
			return false
		}
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
		return true
	})
	if reason == "" {
		return true, ""
	}
	if best != nil && (best.first == "host memory" || best.first == "GPU memory" || best.first == "memory envelope") {
		reason += "; " + memoryCapacityRemedies
	}
	return false, reason
}

// evaluateTargets runs qualification's permanent-capacity fit on every known
// target, ignoring temporary occupancy and release holds. Submission and
// resolve share it so their explanations cannot drift. visit returns false to stop.
func (q *Queue) evaluateTargets(j *Job, visit func(fit targetFit, candidate *Job, m *Machine) bool) {
	// Resolve envelopes with the planner's code, but without temporary
	// occupancy or release holds. Retain observations for learned needs.
	capacity := Queue{clock: q.clock, s: q.s, memoryOrigin: q, memoryPlanning: newMemoryPlanningCache()}
	if q.memoryPlanning != nil {
		// Retained lower bounds depend on origin history and identity, not
		// the temporary capacity fields removed by this view.
		capacity.memoryPlanning.history = q.memoryPlanning.history
	}
	capacity.s.Jobs, capacity.s.ModelSlots, capacity.s.HostReleases = nil, nil, nil
	for _, m := range q.s.Machines {
		if !q.jobAllowsMachine(j, m.ID) {
			fit := targetFit{excluded: true, key: targetKey(m)}
			if contains(j.Spec.ExcludedMachines, m.ID) {
				fit.check(false, "excluded_machines")
			} else {
				fit.check(false, "allow-list")
			}
			if !visit(fit, j, m) {
				return
			}
			continue
		}
		for _, target := range q.placementTargets(m) {
			copy := q.qualificationCapacity(target)
			// Observation identity must see the same available target copy,
			// including when the live owner is temporarily unavailable.
			capacity.s.Machines = map[string]*Machine{copy.ID: &copy}
			candidates, refusal := extensionJobsWithRefusal(j, &copy, true)
			if len(candidates) == 0 {
				fit := targetFit{key: targetKey(&copy)}
				fit.check(false, refusal)
				if !visit(fit, j, &copy) {
					return
				}
			}
			for _, candidate := range candidates {
				candidate = capacity.memoryCandidate(candidate, &copy)
				fit := targetFit{key: targetKey(&copy)}
				fitsResourcesChecked(candidate, &copy, copy.TotalMemoryGB, &fit)
				hostFailure := hostFitFailure(&copy, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB)
				fit.check(hostFailure == "", hostFailure)
				fit.shortfall = resourceShortfall(hostNeed(&copy, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB), copy.FreeHostMemoryGB) + resourceShortfall(candidate.Spec.Needs.MemoryGB, copy.TotalMemoryGB) + resourceShortfall(float64(candidate.Spec.Needs.cores()), float64(copy.EffectiveCPUCores))
				if !visit(fit, candidate, &copy) {
					return
				}
			}
		}
	}
}

// Shared permanent-capacity projection for qualification and bounded exploration.
func (q *Queue) qualificationCapacity(target *Machine) Machine {
	// Only scalar capacity fields, disk volumes and availability are changed.
	// All other referenced inventory remains read-only for this planning pass.
	copy := *target
	copy.hostBudgetGB, copy.modelReservationDebtGB, copy.hostReservationDebtGB = 0, 0, 0
	copy.batchTenants = 0
	if target.CapabilityProfile != nil {
		profile := *target.CapabilityProfile
		copy.CapabilityProfile = &profile
		if profile.Availability != nil {
			availability := *profile.Availability
			profile.Availability = &availability
		}
	}
	copy.group, copy.groupMembers, copy.groupFloorGB = target.group, target.groupMembers, target.groupFloorGB
	copy.DiskVolumes = diskCapacityVolumes(copy.DiskVolumes)
	copy.Gone, copy.Leaving, copy.Recovering, copy.CheckpointUnavailable = false, false, false, false
	copy.MemoryError, copy.HostMemoryError, copy.CPUCapacityReason = "", "", ""
	copy.HostMemoryPolicyMismatch = false
	// Owner pressure is temporary; busy qualified targets queue (#605).
	// Structural shared-host reasons are checked first and still refuse.
	if copy.SharedHostReason != "" && copy.SharedHostReason == q.hostPressureReason(target) {
		copy.SharedHostReason = ""
	}
	policy := q.s.HostMaintenance[hostID(target)]
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
	return copy
}
