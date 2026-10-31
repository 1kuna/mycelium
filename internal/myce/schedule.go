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

// A nil report retains short-circuit dispatch/launch evaluation. Qualification
// collects failures from these very same checks, without a second fit path.
func fitsResourcesChecked(j *Job, m *Machine, memory float64, report *targetFit) bool {
	return report.check(sharedHostJobFits(j, m), "shared host profile") &&
		report.check(diskHeadroomReason(j, m) == "", "disk headroom") &&
		report.check(executionTargetMatches(j.Spec, m), "target") &&
		report.check(nativeNetworkFits(j, m), "native network isolation") &&
		report.check(j.MemoryEnvelope == nil || j.MemoryEnvelope.Refusal == "", "memory envelope") &&
		report.check(!m.Recovering, "recovering") &&
		report.check(deviceAllowed(j, m), "device") &&
		report.check(j.Spec.Needs.cpuOnly() || m.GPU != "cpu", "platform") &&
		report.check(cpuFits(j, m), "cores") &&
		report.check(!cancelled(j), "cancelled") &&
		report.check(dockerRuntimeFits(j, m), "Docker runtime") &&
		report.check(logPolicyFits(j.Spec.Logs, m), "log policy") &&
		report.check(!availabilityBlocked(m), "availability") &&
		report.check(capabilityFits(j, m), "hardware capacity") &&
		report.check(extensionCanRun(j, m), "extension") &&
		report.check(j.Spec.Needs.cpuOnly() || m.MemoryError == "", "GPU memory telemetry") &&
		report.check(!m.Gone, "gone") && report.check(!m.Leaving, "leaving") &&
		report.check(!needsCheckpoint(j) || !m.CheckpointUnavailable, "checkpoint") &&
		report.check(memory >= j.Spec.Needs.MemoryGB, "GPU memory") &&
		report.check(machineAllowed(j.Spec.AllowedMachines, m.ID), "allow-list") &&
		report.check(j.Spec.Needs.cpuOnly() || len(j.Spec.Needs.Platform.Required) == 0 || slices.Contains(j.Spec.Needs.Platform.Required, m.GPU), "platform") &&
		report.check(environmentCanRun(j, m), "environment")
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
func waitingReason(j *Job, machines map[string]*Machine, jobs map[string]*Job) string {
	if reason := cpuWaitingReason(j, machines); reason != "" {
		return reason
	}
	if reason := dockerRuntimeWaitingReason(j, machines); reason != "" {
		return reason
	}
	if reason := extensionWaitingReason(j, machines); reason != "" {
		return reason
	}
	if reason := capabilityWaitingReason(j); reason != "" {
		return reason
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
					return environmentWaitingReason(j, m)
				}
				return "waiting for a worker to pull this job"
			}
			return reservationWaitReason(m, false)
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
		return strings.Join(slices.Compact(diskReasons), "; ")
	}
	unknownCapacity := []string{}
	knownCapacity := false
	for _, m := range machines {
		if m.Gone || m.Leaving || !machineAllowed(j.Spec.AllowedMachines, m.ID) || !environmentCanRun(j, m) || len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU) {
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
		return "waiting for hardware capacity readings: " + strings.Join(unknownCapacity, "; ")
	}
	needs := []string{}
	if j.Spec.Needs.HostMemoryGB > 0 {
		needs = append(needs, fmt.Sprintf("waiting for RAM: %.1f GiB host memory (or telemetry unavailable)", j.Spec.Needs.HostMemoryGB))
	}
	if len(j.Spec.Needs.Platform.Required) > 0 {
		needs = append(needs, "needs "+strings.Join(j.Spec.Needs.Platform.Required, " or "))
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
		needs = append(needs, fmt.Sprintf("%.1f GB free GPU memory", j.Spec.Needs.MemoryGB))
	}
	return "no machine in the fleet can run this right now (" + strings.Join(needs, "; ") + ")"
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
