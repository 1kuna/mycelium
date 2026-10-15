package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Status presentation consumes measured facts and the existing queue projection.
// It never selects a hypha or changes an admission decision.
func HyphaAvailability(m *Machine) string {
	switch {
	case m.Disabled != "":
		return "disabled by operator"
	case m.Gone:
		return "offline; waiting for this hypha to return"
	case m.Leaving:
		return "leaving; no new work"
	case m.EnablePending:
		return "held: enable not yet acknowledged (retrying)"
	}
	if a := machineAvailability(m); a != nil && !a.Available {
		return availabilityWords(a.Reason)
	}
	if m.AdmissionReason != "" {
		return admissionWords(m.AdmissionReason)
	}
	if m.MemoryError != "" || m.HostMemoryError != "" {
		return "waiting for memory readings"
	}
	if m.DockerCleanup != "" {
		return "waiting for previous work to finish cleaning up"
	}
	return "available for work"
}

func availabilityWords(reason string) string {
	switch reason {
	case "waiting for 5 idle minutes":
		return "hypha in use by its owner; waiting for 5 idle minutes"
	case "on battery":
		return "hypha on battery; waiting for external power"
	case "cooling down":
		return "hypha hot; waiting to cool down"
	case "owner signals stale":
		return "hypha availability readings are out of date"
	case "power source unavailable":
		return "hypha power source is unknown"
	case "external power required":
		return "hypha needs external power"
	case "owner activity unavailable":
		return "hypha owner activity is unknown"
	case "heat sensing unavailable":
		return "hypha temperature is unknown"
	default:
		return "hypha availability is unknown"
	}
}
func admissionWords(reason string) string {
	if strings.HasPrefix(reason, "maximum simultaneous jobs reached") {
		return "hypha is busy; its simultaneous job limit has been reached"
	}
	return availabilityWords(reason)
}

func resourceWords(platform string) string {
	switch platform {
	case "cuda":
		return "NVIDIA (CUDA)"
	case "metal":
		return "Apple Metal"
	case "xpu":
		return "Intel (XPU)"
	case "fake":
		return "test resource (fake)"
	default:
		return "unknown resource"
	}
}
func profileSize(n uint64) string {
	if n == 0 {
		return "unknown"
	}
	return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
}

// CapabilityProfileLines is additive: benchmark/ranking code owns the profile.
func CapabilityProfileLines(m *Machine) []string {
	p := m.CapabilityProfile
	if p == nil {
		return []string{"capability profile not reported"}
	}
	layout := "memory layout unknown"
	switch p.MemoryLayout {
	case "unified":
		layout = "unified memory (shared by CPU and accelerators)"
	case "separate":
		layout = "separate system and accelerator memory"
	}
	cpus := "CPU count unknown"
	if p.LogicalCPUs > 0 {
		cpus = fmt.Sprintf("%d logical CPUs", p.LogicalCPUs)
	}
	platform := p.OS + "/" + p.Arch
	if p.OS == "" || p.Arch == "" {
		platform = "operating system or architecture unknown"
	}
	lines := []string{fmt.Sprintf("%s; %s; %s", platform, cpus, layout), "system memory: " + profileSize(p.SystemMemoryBytes)}
	for _, a := range p.Accelerators {
		resource := resourceWords(a.Type)
		if a.Name != "" {
			resource += " (" + a.Name + ")"
		}
		lines = append(lines, resource+": "+profileSize(a.MemoryBytes))
	}
	if len(p.Accelerators) == 0 {
		lines = append(lines, "accelerator measurements not reported")
	}
	lines = append(lines, "local storage: "+profileSize(p.DiskFreeBytes)+" free / "+profileSize(p.DiskTotalBytes)+" total")
	lines = append(lines, "onboarding benchmark: "+p.Benchmark.Summary())
	return lines
}
func ExtensionStatus(r ExtensionReport) string {
	work := "kind of work"
	name := strings.TrimPrefix(r.Work, "kind:")
	if strings.HasPrefix(r.Work, "program:") {
		work = "program"
		name = strings.TrimPrefix(r.Work, "program:")
	}
	state := "qualified"
	if !r.Qualified {
		state = "not qualified; setup and check required"
	}
	return fmt.Sprintf("%s@%s · %s %s · %s", r.Name, r.Version, work, name, state)
}
func JobReason(j *Job) string {
	if j.Reason != "" {
		return j.Reason
	}
	switch j.State {
	case "queued":
		return "waiting for a hypha to take this job"
	case "running":
		return "running now"
	case "stopping":
		return "waiting for this attempt to stop"
	}
	return ""
}
func JobWork(j *Job) string {
	if j.Spec.Kind != "" {
		return "kind of work: " + j.Spec.Kind
	}
	if j.Spec.Program != nil {
		return "program: " + j.Spec.Program.Type
	}
	return "program"
}

// Called under the queue lock on a detached status copy. The scheduler's reason
// remains authoritative; only its generic no-fit explanation needs more detail.
func (q *Queue) statusWaitingReason(j *Job) string {
	reason := j.Reason
	if rest, ok := strings.CutPrefix(reason, "requeued: disabled by operator; "); ok {
		copy := *j
		copy.Reason = rest
		return "requeued after being disabled; " + q.statusWaitingReason(&copy)
	}
	if strings.HasPrefix(reason, "waiting for an extension for ") {
		return "no extension is installed for this " + statusWorkType(j)
	}
	if strings.HasPrefix(reason, "waiting for a qualified extension for ") {
		return "no hypha has a qualified extension for this " + statusWorkType(j)
	}
	// Environment keys may be absolute lock-file paths. Never display them.
	if key := j.Spec.Environment.Key(); key != "" {
		reason = strings.ReplaceAll(reason, key, "required software")
	}
	if strings.HasPrefix(reason, "no machine in the fleet can run this right now") || reason == "waiting for a worker to pull this job" {
		blocked := []string{}
		possible := 0
		for _, m := range q.s.Machines {
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
			if !fits(j, &copy, max(m.TotalMemoryGB, m.FreeMemoryGB)) {
				continue
			}
			possible++
			state := HyphaAvailability(m)
			if q.s.DisabledHosts[hostID(m)] != "" {
				state = "disabled by operator"
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
		if reason == "waiting for a worker to pull this job" {
			return "waiting for a hypha to take this job"
		}
		qualifier := ""
		if j.Spec.extensionJob() {
			qualifier = "qualified "
		}
		resources := []string{}
		for _, platform := range j.Spec.Needs.Platform.Required {
			resources = append(resources, resourceWords(platform))
		}
		required := ""
		if len(resources) > 0 {
			required = " with " + strings.Join(resources, " or ")
		}
		return fmt.Sprintf("no %shypha%s can run this job right now; it needs %.1f GiB free accelerator memory and %.1f GiB free system memory", qualifier, required, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	}
	reason = strings.NewReplacer(
		"waiting for a worker to pull this job", "waiting for a hypha to take this job",
		"waiting for a busy machine to make room", "waiting for a busy hypha to make room",
		"waiting for a free machine", "waiting for a free hypha",
		"NAS checkpoint root is unreachable", "saved progress storage is unavailable",
		"building environment:", "preparing software:",
		"environment not ready:", "software not ready:",
		"concurrent experiment cap", "simultaneous experiment limit",
		"GPU memory share cap", "accelerator memory share limit",
		"telemetry unavailable", "memory readings unavailable",
	).Replace(reason)
	if reason == "" {
		return "waiting for a hypha to take this job"
	}
	return reason
}
func statusWorkType(j *Job) string {
	if j.Spec.Program != nil {
		return "program"
	}
	return "kind of work"
}
