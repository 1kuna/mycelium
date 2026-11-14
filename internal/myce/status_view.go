package myce

import (
	"fmt"
	"strings"
	"time"
)

const recoveryAttentionDelay = 2 * time.Minute

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
	case m.Recovering:
		state := "recovering saved attempts; no new work"
		if m.Suspect {
			state = "suspect; waiting for worker contact; " + state
		}
		if m.RecoveringSince != nil {
			state += " since " + m.RecoveringSince.UTC().Format(time.RFC3339)
		}
		if m.RecoveryNeedsAttention {
			state += "; needs attention (recovery longer than 2 minutes)"
		}
		return state
	case m.Suspect:
		return "suspect; waiting for worker contact; no new work"
	}
	if m.CPUCapacityReason != "" && m.GPU == "cpu" {
		return m.CPUCapacityReason
	}
	if why := inventoryWaitingReason(m); why != "" {
		return why
	}
	if reason := capabilityUnavailableReason(m); reason != "" {
		return reason
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
	if a := machineAvailability(m); a != nil && a.Available && a.Reason == "CPU thermal coverage waived for desktop" {
		return "available for work; CPU thermal coverage waived for desktop"
	}
	if m.CPUIsolation == "not-enforced" {
		return "available for work; GPU isolation is not enforced for CPU work (Metal has no device mask)"
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
	case "host CPU thermal coverage unavailable":
		return "host CPU thermal coverage unavailable"
	case "heat sensing unavailable":
		return "hypha temperature is unknown"
	default:
		return "hypha availability is unknown"
	}
}
func admissionWords(reason string) string {
	if reason == "planned host maintenance" {
		return reason
	}
	if strings.HasPrefix(reason, "maximum simultaneous jobs reached") {
		return "simultaneous job limit reached; reservations do not measure utilization"
	}
	return availabilityWords(reason)
}

func resourceWords(platform string) string {
	if name := backendFor(platform).resourceName; name != "" {
		return name
	}
	return "unknown resource"
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
	if m.GPU == "cpu" {
		lines = append(lines, fmt.Sprintf("CPU admission: %d unreserved / %d admitted logical cores (%d reserved for owner); utilization unknown; no accelerator required", m.FreeCPUCores, m.EffectiveCPUCores, m.CPUReserveCores))
	} else if len(p.Accelerators) == 0 {
		lines = append(lines, "accelerator measurements not reported")
	}
	if len(m.DiskVolumes) == 0 {
		lines = append(lines, "local storage: "+profileSize(p.DiskFreeBytes)+" free / "+profileSize(p.DiskTotalBytes)+" total (startup)")
	} else {
		for _, v := range m.DiskVolumes {
			label := strings.Join(v.Roots, "+") + " disk: "
			if v.Error != "" {
				if !v.SampledAt.IsZero() {
					lines = append(lines, fmt.Sprintf("%sdisk reading unavailable; last reported %.1f / %.1f GiB free (age %.1fs)", label, float64(v.FreeBytes)/(1<<30), float64(v.TotalBytes)/(1<<30), v.AgeSeconds))
				} else {
					lines = append(lines, label+"disk reading unavailable")
				}
				continue
			}
			rate := "change unknown"
			if v.ChangeBytesPerHour != nil {
				rate = fmt.Sprintf("%+.2f GiB/h", *v.ChangeBytesPerHour/(1<<30))
			}
			lines = append(lines, fmt.Sprintf("%s%.1f / %.1f GiB free; %s", label, float64(v.FreeBytes)/(1<<30), float64(v.TotalBytes)/(1<<30), rate)+fmt.Sprintf(" (age %.1fs)", v.AgeSeconds))
		}
	}
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
		if r.CheckState != "" {
			state = "not qualified; native check " + r.CheckState
		}
		if reason := extensionReportDiagnostic(r.Reason); reason != "" && (r.CheckState == "" || reason != "setup and check required") {
			state = "not qualified; " + reason
		}
	}
	return fmt.Sprintf("%s@%s · %s %s · %s", r.Name, r.Version, work, name, state)
}

// JobReason is the one-line reason with the hold's next action and evidence,
// ending with the job's finding when it has one.
// JobReason is the job's one current reason and its next action, as default
// status, wait and run print it. JobReasonDetail adds the evidence.
func JobReason(j *Job) string {
	return jobReason(j, false)
}

// JobReasonDetail adds the hold's evidence and the isolation note, for detail
// views (status --history and the status page).
func JobReasonDetail(j *Job) string {
	return jobReason(j, true)
}

func jobReason(j *Job, detail bool) string {
	text := (*Hold).Text
	if detail {
		text = (*Hold).FullText
	}
	reason := jobReasonWith(j, text)
	if isolation := jobNativeIsolationStatus(j); detail && isolation != "" {
		if reason != "" {
			reason += "; "
		}
		reason += isolation
	}
	if j.Finding != nil {
		if reason != "" {
			reason += "; "
		}
		reason += j.Finding.Text()
	}
	return reason
}

// JobHold is the job's one hold record: the queue's wait for a queued job, or
// its pause. Nil when nothing holds it.
func JobHold(j *Job) *Hold {
	if j.State == "paused" && j.Pause != nil {
		if j.Hold != nil && j.Hold.Kind == "paused" {
			return j.Hold
		}
		return pauseHold(j)
	}
	if j.State == "queued" {
		return j.Hold
	}
	return nil
}

// jobReasonText is the lifecycle/scheduler reason without the finding or the
// hold's next action, for views that carry those as their own fields.
func jobReasonText(j *Job) string {
	return jobReasonWith(j, func(h *Hold) string { return h.Reason })
}

func jobReasonWith(j *Job, holdText func(*Hold) string) string {
	reason := j.Reason
	if h := JobHold(j); h != nil {
		reason = holdText(h)
	}
	if reason == "" {
		switch j.State {
		case "queued":
			reason = "waiting for a hypha to take this job"
		case "running":
			reason = "running now"
		case "stopping":
			reason = "waiting for this attempt to stop"
		}
	}
	if j.HealthRecovery != nil {
		if reason != "" {
			reason += "; "
		}
		reason += j.HealthRecovery.Summary
		if j.HealthRecovery.NextStep != "" {
			reason += "; " + j.HealthRecovery.NextStep
		}
	}
	return reason
}
func JobWork(j *Job) string {
	if j.Spec.ExtensionCheck != nil {
		return "checking extension: " + j.Spec.Extension + " (" + j.Spec.ExtensionCheck.Work + ")"
	}
	if j.Spec.Extension != "" {
		return "extension: " + j.Spec.Extension
	}
	if j.Spec.Kind != "" {
		return "kind of work: " + j.Spec.Kind
	}
	if j.Spec.Program != nil {
		return "program: " + j.Spec.Program.Type
	}
	return "program"
}

// Called under the queue lock on a detached status copy. The scheduler's reason
// is authoritative; status only words it for readers.
func (q *Queue) statusWaitingReason(j *Job) string {
	j.Hold = q.jobHold(j)
	return q.statusWaitingReasonDetail(j)
}

func (q *Queue) statusWaitingReasonDetail(j *Job) string {
	reason := j.Reason
	if rest, ok := strings.CutPrefix(reason, "requeued: disabled by operator; "); ok {
		copy := *j
		copy.Reason = rest
		return "requeued after being disabled; " + q.statusWaitingReasonDetail(&copy)
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
