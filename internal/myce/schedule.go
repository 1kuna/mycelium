package myce

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

func fits(j *Job, m *Machine, memory float64) bool {
	for _, candidate := range extensionJobs(j, m, true) {
		if hostFits(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) && fitsResources(candidate, m, memory) {
			return true
		}
	}
	return false
}
func fitsWithoutHostMemory(j *Job, m *Machine, memory float64) bool {
	for _, candidate := range extensionJobs(j, m, true) {
		if fitsResources(candidate, m, memory) {
			return true
		}
	}
	return false
}
func fitsResources(j *Job, m *Machine, memory float64) bool {
	return fitsResourcesChecked(j, m, memory, nil)
}

// Memory sizing changes only the memory vector/envelope. These structural
// eligibility predicates are identical before and after it.
func memorySizingTargetFits(j *Job, m *Machine) bool {
	return executionTargetMatches(j.Spec, m) && deviceAllowed(j, m) &&
		(j.Spec.Needs.cpuOnly() || m.GPU != "cpu" && (len(j.Spec.Needs.Platform.Required) == 0 || slices.Contains(j.Spec.Needs.Platform.Required, m.GPU)))
}

// A nil report retains short-circuit dispatch/launch evaluation. Qualification
// collects failures from these very same checks, without a second fit path.
func fitsResourcesChecked(j *Job, m *Machine, memory float64, report *targetFit) bool {
	return report.check(sharedHostJobFits(j, m), "shared host profile") &&
		report.check(sharedInputCacheReason(j.Spec, m, j.State != "queued") == "", "shared input cache") &&
		report.check(diskHeadroomReason(j, m) == "", "disk headroom") &&
		fitsTargetResourcesChecked(j, m, memory, report)
}

// Shared target eligibility without the disk check. Storage diagnostics must
// evaluate a shortage instead of discarding its host because of that shortage.
func fitsTargetResourcesChecked(j *Job, m *Machine, memory float64, report *targetFit) bool {
	return report.check(toolsFit(j, m), "tools") &&
		report.check(groupFits(j, m), "card group") &&
		report.check(executionTargetMatches(j.Spec, m), "target") &&
		report.check(nativeNetworkFits(j, m), "native network isolation") &&
		report.check(j.MemoryEnvelope == nil || j.MemoryEnvelope.Refusal == "", "memory envelope") &&
		report.check(!m.Recovering, "recovering") &&
		report.check(deviceAllowed(j, m), "device") &&
		report.check(j.Spec.Needs.cpuOnly() || m.GPU != "cpu", "platform") &&
		report.check(cpuFits(j, m), "cores") &&
		report.check(!cancelled(j), "cancelled") &&
		report.check(logPolicyFits(j.Spec.Logs, m), "log policy") &&
		report.check(!availabilityBlocked(m), "availability") &&
		report.check(capabilityFits(j, m), "hardware capacity") &&
		report.check(extensionCanRun(j, m), "extension") &&
		report.check(j.Spec.Needs.cpuOnly() || m.MemoryError == "", "GPU memory telemetry") &&
		report.check(!m.Gone, "gone") && report.check(!m.Leaving, "leaving") &&
		report.check(!needsCheckpoint(j) || !m.CheckpointUnavailable, "checkpoint") &&
		report.check(memory >= j.Spec.Needs.MemoryGB, "GPU memory") &&
		report.check(machineAllowed(j.Spec.AllowedMachines, m.ID), "allow-list") &&
		report.check(!contains(j.Spec.ExcludedMachines, m.ID), "excluded_machines") &&
		report.check(j.Spec.Needs.cpuOnly() || len(j.Spec.Needs.Platform.Required) == 0 || slices.Contains(j.Spec.Needs.Platform.Required, m.GPU), "platform") &&
		report.check(environmentCanRun(j, m), "environment")
}

// Spread across cards only work that no single card of the host can hold.
// Model-server and extension-check jobs keep one card.
func groupFits(j *Job, m *Machine) bool {
	return len(m.groupMembers) == 0 || !j.Spec.Needs.cpuOnly() && j.Spec.Needs.MemoryGB > m.groupFloorGB && j.Spec.Needs.Model == "" && j.Spec.ExtensionCheck == nil
}

func affinity(j *Job, m *Machine) int {
	n := 0
	if j.Spec.Needs.Model != "" && slices.Contains(m.Models, j.Spec.Needs.Model) {
		n++
	}
	if j.Spec.Needs.Model != "" && !slices.Contains(m.Models, j.Spec.Needs.Model) && slices.Contains(m.CachedModels, j.Spec.Needs.Model) {
		n++
	}
	if j.Spec.Environment.Key() != "" && environmentReady(j, m) {
		n++
	}
	if len(j.Spec.SharedInputs) > 0 {
		ready := true
		for _, input := range j.Spec.SharedInputs {
			ready = ready && slices.Contains(m.CachedSharedInputs, input.key())
		}
		if ready {
			n++
		}
	}
	return n
}
func beforeWithinTier(a, b *Job, m *Machine) bool {
	if affinity(a, m) != affinity(b, m) {
		return affinity(a, m) > affinity(b, m)
	}
	if modelLoaded(a, m) != modelLoaded(b, m) {
		return modelLoaded(a, m)
	}
	if a.Spec.Deadline != nil && b.Spec.Deadline == nil {
		return true
	}
	if a.Spec.Deadline == nil && b.Spec.Deadline != nil {
		return false
	}
	if a.Spec.Deadline != nil && !a.Spec.Deadline.Equal(*b.Spec.Deadline) {
		return a.Spec.Deadline.Before(*b.Spec.Deadline)
	}
	if a.Front != b.Front {
		return a.Front > b.Front
	}
	if !a.Queued.Equal(b.Queued) {
		return a.Queued.Before(b.Queued)
	}
	return a.ID < b.ID
}

// The generic wait, when no specific mechanism holds the job.
func waitingHold(j *Job, machines map[string]*Machine, jobs map[string]*Job) *Hold {
	if h := sharedInputWait(j, machines); h != nil {
		return h
	}
	if h := cpuWait(j, machines); h != nil {
		return h
	}
	if reason := extensionWaitingReason(j, machines); reason != "" {
		return newHold("extension", reason)
	}
	if reason := capabilityWaitingReason(j); reason != "" {
		return newHold("capability", reason)
	}
	for _, m := range machines {
		memory := m.FreeMemoryGB
		for _, r := range jobs {
			if r.Machine == m.ID && (r.State == "running" || r.State == "stopping") {
				memory += r.Spec.Needs.MemoryGB
			}
		}
		if fits(j, m, memory) {
			if memory == m.FreeMemoryGB {
				if !environmentReady(j, m) {
					return newHold("unprepared", environmentWaitingReason(j, m))
				}
				return newHold("waiting_for_worker", "waiting for a worker to pull this job")
			}
			return newHold("capacity", reservationWaitReason(m, false, 0))
		}
	}
	diskReasons := []string{}
	for _, m := range machines {
		copy := *m
		copy.DiskVolumes = nil
		if fits(j, &copy, max(m.TotalMemoryGB, m.FreeMemoryGB)) {
			if reason := diskHeadroomReason(j, m); reason != "" {
				diskReasons = append(diskReasons, reason)
			} else {
				diskReasons = nil
				break
			}
		}
	}
	if len(diskReasons) > 0 {
		slices.Sort(diskReasons)
		return newHold("disk_headroom", strings.Join(slices.Compact(diskReasons), "; "))
	}
	unknownCapacity := []string{}
	knownCapacity := false
	for _, m := range machines {
		if m.Gone || m.Leaving || !j.Spec.allowsMachine(m.ID) || !environmentCanRun(j, m) || len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU) {
			continue
		}
		if reason := capabilityUnavailableReason(m); reason != "" {
			unknownCapacity = append(unknownCapacity, m.ID+": "+reason)
		} else {
			knownCapacity = true
		}
	}
	if len(unknownCapacity) > 0 && !knownCapacity {
		slices.Sort(unknownCapacity)
		return newHold("unknown_observation", "waiting for hardware capacity readings: "+strings.Join(unknownCapacity, "; "))
	}
	needs := []string{}
	if j.Spec.Needs.HostMemoryGB > 0 {
		needs = append(needs, fmt.Sprintf("waiting for RAM: %.1f GiB host memory (or telemetry unavailable)", j.Spec.Needs.HostMemoryGB))
	}
	if len(j.Spec.Needs.Platform.Required) > 0 {
		needs = append(needs, "needs "+strings.Join(j.Spec.Needs.Platform.Required, " or "))
	}
	if len(j.Spec.ExcludedMachines) > 0 {
		needs = append(needs, "excluded machines: "+strings.Join(j.Spec.ExcludedMachines, ", "))
	}
	if len(j.Spec.AllowedMachines) > 0 {
		needs = append(needs, "allowed machines: "+strings.Join(j.Spec.AllowedMachines, ", "))
	}
	if j.Spec.Environment.Key() != "" {
		if len(j.EnvironmentRecipes) == 0 {
			needs = append(needs, "environment not ready: "+j.Spec.Environment.Key())
		} else {
			platforms := []string{}
			for p := range j.EnvironmentRecipes {
				platforms = append(platforms, p)
			}
			slices.Sort(platforms)
			needs = append(needs, "needs "+j.Spec.Environment.Key()+"; no eligible "+strings.Join(platforms, " or ")+" machine online")
		}
	}
	if !j.Spec.Needs.cpuOnly() {
		needs = append(needs, fmt.Sprintf("%.1f GB free GPU memory on one host (one card, or several cards of one host)", j.Spec.Needs.MemoryGB))
	}
	return newHold("no_eligible_host", "no machine in the fleet can run this right now ("+strings.Join(needs, "; ")+")")
}

// The explanation for a job no specific mechanism holds: either every host
// that could fit it is unavailable, or no host fits it at all (idle is false),
// or an eligible worker simply has not taken it yet (idle is true). Sizing
// reuses placement's fitting at each host's reported capacity without making
// a dispatch choice.
func (q *Queue) noFitDetail(j *Job, idle bool) string {
	blocked := []string{}
	sizedNeeds := []string{}
	possible := 0
	for _, m := range q.plannerMachines() {
		if !q.projectAllows(j.Spec.Project, m.ID) {
			continue
		}
		copy := cloneMachine(m)
		copy.Gone, copy.Leaving, copy.CheckpointUnavailable = false, false, false
		copy.MemoryError = ""
		if a := machineAvailability(&copy); a != nil {
			a.Available = true
		}
		// Reuse fitting at the reported capacity, without making a dispatch choice.
		fit := false
		for _, candidate := range q.memoryCandidates(j, &copy) {
			n := candidate.Spec.Needs
			if n.Memory == "auto" {
				detail := fmt.Sprintf("%s needs %.1f GiB free accelerator memory and %.1f GiB host RAM", m.ID, n.MemoryGB, hostNeed(&copy, n.MemoryGB, n.HostMemoryGB))
				if n.cpuOnly() {
					detail = fmt.Sprintf("%s needs %d logical cores and %.1f GiB host RAM", m.ID, n.cores(), hostNeed(&copy, n.MemoryGB, n.HostMemoryGB))
				}
				detail += " (" + candidate.MemoryEnvelope.Origin() + ")"
				if candidate.MemoryEnvelope.Refusal != "" {
					detail = m.ID + ": " + candidate.MemoryEnvelope.Refusal
				}
				sizedNeeds = append(sizedNeeds, detail)
			}
			fit = fit || hostFits(&copy, n.MemoryGB, n.HostMemoryGB) && fitsResources(candidate, &copy, max(m.TotalMemoryGB, m.FreeMemoryGB))
		}
		if !fit {
			continue
		}
		possible++
		state := HyphaAvailability(m)
		if q.maintenanceBlocked(hostID(m)) {
			state = "planned host maintenance"
		}
		if state == "available for work" && q.machineLimitReason(m) != "" {
			state = admissionWords(q.machineLimitReason(m))
		}
		if state == "available for work" && m.CheckpointUnavailable && needsCheckpoint(j) {
			state = "waiting for saved progress storage"
		}
		if state != "available for work" {
			blocked = append(blocked, m.ID+": "+state)
		}
	}
	if possible > 0 && len(blocked) == possible {
		slices.Sort(blocked)
		return strings.Join(blocked, "; ")
	}
	if idle {
		return "waiting for a hypha to take this job"
	}
	qualifier := ""
	if j.Spec.extensionJob() {
		qualifier = "qualified "
	}
	if len(sizedNeeds) > 0 {
		slices.Sort(sizedNeeds)
		return "no " + qualifier + "hypha can run this job right now; " + strings.Join(slices.Compact(sizedNeeds), "; ")
	}
	autoMemory := j.Spec.Needs.Memory == "auto"
	for _, extension := range j.Extensions {
		for _, variant := range extension.Variants {
			autoMemory = autoMemory || variant.Needs.Memory == "auto"
		}
	}
	if autoMemory {
		return "memory is sized per hypha; no " + qualifier + "hypha is available to size it against"
	}
	resources := []string{}
	for _, platform := range j.Spec.Needs.Platform.Required {
		resources = append(resources, resourceWords(platform))
	}
	required := ""
	if len(resources) > 0 {
		required = " with " + strings.Join(resources, " or ")
	}
	// Authored placement limits stay named: they are often why nothing fits.
	limits := ""
	if len(j.Spec.ExcludedMachines) > 0 {
		limits += "; excluded machines: " + strings.Join(j.Spec.ExcludedMachines, ", ")
	}
	if len(j.Spec.AllowedMachines) > 0 {
		limits += "; allowed machines: " + strings.Join(j.Spec.AllowedMachines, ", ")
	}
	if j.Spec.Needs.cpuOnly() {
		return fmt.Sprintf("no %shypha can run this CPU job right now; it needs %d logical cores and %.1f GiB host RAM", qualifier, j.Spec.Needs.cores(), j.Spec.Needs.HostMemoryGB) + limits
	}
	return fmt.Sprintf("no %shypha%s can run this job right now; it needs %.1f GiB free accelerator memory and %.1f GiB free system memory", qualifier, required, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) + limits
}
func expired(t *time.Time, now time.Time) bool { return t != nil && !now.Before(*t) }

func modelLoaded(j *Job, m *Machine) bool {
	return j.Spec.Needs.Model != "" && slices.Contains(m.Models, j.Spec.Needs.Model)
}

// Extension checks have no job-level Environment name. Explain the frozen
// preparation identity, including a negative-cached failure, instead of an
// empty software label that suggests a build is still making progress.
func environmentWaitingReason(j *Job, m *Machine) string {
	label := j.Spec.Environment.Key()
	if e := qualifiedExtension(j, m); e != nil {
		label = extensionKey(*e)
		key := extensionPreparationKey(*e, m)
		for _, r := range m.EnvironmentReports {
			if r.Key == key && (r.State == "failed" || r.State == "uploading") {
				cause := r.Error
				if cause == "" {
					cause = "previous preparation failed; explicit setup required"
				}
				return "environment preparation blocked: " + label + ": " + cause
			}
		}
	}
	if label == "" {
		label = j.Spec.workType()
	}
	return "building environment: " + label
}

// A started build owns this worker until completion or its exact planned-stop
// receipt. Name that custody rather than suggesting an idle worker can pull.
func (q *Queue) preparationWaitingReason(j *Job, machines map[string]*Machine) string {
	if len(q.s.PreparationPlans) == 0 {
		return ""
	}
	blocked := []string{}
	var preparationTurns map[string]*Job
	for _, m := range machines {
		if !q.placementAllowed(j, m) {
			continue
		}
		active := q.preparationActive(m)
		var fence *Job
		// Use dispatch's global plan: the same cold winner cannot reserve
		// preparation independently on every compatible worker or target view.
		if !active {
			if preparationTurns == nil {
				preparationTurns = map[string]*Job{}
				scan := q.placementScan()
				plan := q.assignmentPlan(scan)
				for _, target := range scan.machines {
					if winner := plan[targetKey(target)]; winner != nil && !environmentReady(winner, target) && preparationOwnsTurn(winner) {
						preparationTurns[target.ID] = winner
					}
				}
			}
			fence = preparationTurns[m.ID]
		}
		copy := *m
		if active {
			copy = q.cpuAfterReleases(m, nil, nil, 1)
		}
		fit := false
		for _, candidate := range q.memoryCandidates(j, &copy) {
			fit = fit || fits(candidate, &copy, copy.FreeMemoryGB)
		}
		if !fit {
			continue
		}
		if !active && fence == nil {
			return "" // Another compatible target is not held by preparation.
		}
		found := false
		for _, l := range q.s.PreparationPlans {
			if l.Kind != "environment" || l.Machine != m.ID || l.Session != m.Session || !l.Started && (fence == nil || l.Demand != fence.ID) {
				continue
			}
			state := "offered; waiting for builder report"
			if l.Started {
				state = "building"
			}
			if l.PriorityStop != nil {
				state = "stopping; waiting for exact release"
			}
			blocked = append(blocked, fmt.Sprintf("waiting for software preparation for job %s on %s (environment %s; %s)", l.Demand, m.ID, l.Key, state))
			found = true
		}
		if !found && fence != nil {
			blocked = append(blocked, fmt.Sprintf("waiting for software preparation for job %s on %s (environment %s; reserved after priority preemption)", fence.ID, m.ID, environmentKey(fence, m)))
		}
	}
	slices.Sort(blocked)
	return strings.Join(slices.Compact(blocked), "; ")
}
