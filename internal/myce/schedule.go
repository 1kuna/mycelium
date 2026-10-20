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
	return (j.Spec.Needs.cpuOnly() || m.GPU != "cpu") && cpuFits(j, m) && !cancelled(j) && dockerRuntimeFits(j, m) && logPolicyFits(j.Spec.Logs, m) && !availabilityBlocked(m) && capabilityFits(j, m) && extensionCanRun(j, m) && (j.Spec.Needs.cpuOnly() || m.MemoryError == "") && !m.Gone && !m.Leaving && (!needsCheckpoint(j) || !m.CheckpointUnavailable) && memory >= j.Spec.Needs.MemoryGB && machineAllowed(j.Spec.AllowedMachines, m.ID) && (j.Spec.Needs.cpuOnly() || len(j.Spec.Needs.Platform.Required) == 0 || slices.Contains(j.Spec.Needs.Platform.Required, m.GPU)) && environmentCanRun(j, m)
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
					return "building environment: " + j.Spec.Environment.Key()
				}
				return "waiting for a worker to pull this job"
			}
			return "waiting for a busy machine to make room"
		}
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
