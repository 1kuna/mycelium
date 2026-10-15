package myce

import "strings"

// Public diagnostics use fixed messages: truncation cannot remove confidential
// text from an arbitrary upstream error.
func diagnostic(value, message string) string {
	if value == "" {
		return ""
	}
	return message
}

func (q *Queue) publicSnapshot() (Snapshot, error) {
	s, err := q.snapshot(true)
	for i := range s.LoadedModels {
		s.LoadedModels[i].EnvironmentRecipe = nil
		s.LoadedModels[i].Error = ceilingDiagnostic(s.LoadedModels[i].Error, "model load or unload failed")
	}
	for _, m := range s.Machines {
		m.Disabled = diagnostic(m.Disabled, "disabled by operator")
		if m.AdmissionReason != "" {
			if strings.HasPrefix(m.AdmissionReason, "maximum simultaneous jobs reached") {
				m.AdmissionReason = "maximum simultaneous jobs reached"
			} else {
				m.AdmissionReason = safeAvailabilityReason(m.AdmissionReason)
			}
		}
		if a := machineAvailability(m); a != nil && !a.Available {
			a.Reason = safeAvailabilityReason(a.Reason)
		}
		for i := range m.ExtensionReports {
			m.ExtensionReports[i].Reason = diagnostic(m.ExtensionReports[i].Reason, "setup and check required")
		}
		for i, name := range m.Environments {
			m.Environments[i] = statusEnvironmentName(name)
		}
		builds := map[string]float64{}
		for name, seconds := range m.EnvironmentBuildSeconds {
			builds[statusEnvironmentName(name)] = max(builds[statusEnvironmentName(name)], seconds)
		}
		m.EnvironmentBuildSeconds = builds
		for i := range m.EnvironmentReports {
			m.EnvironmentReports[i].Key = statusEnvironmentName(m.EnvironmentReports[i].Key)
		}
		m.MemoryEnforcement = memoryEnforcementDiagnostic(m.MemoryEnforcement)
		m.HostMemoryError = diagnostic(m.HostMemoryError, "host memory probe failed")
		m.MemoryError = diagnostic(m.MemoryError, "memory probe failed")
		m.FileTransferError = diagnostic(m.FileTransferError, "file transfer pending; only published progress is available elsewhere")
		m.CacheError = diagnostic(m.CacheError, "cache operation failed")
		m.DockerCleanup = diagnostic(m.DockerCleanup, "Docker cleanup unconfirmed")
		for i := range m.CopyReports {
			m.CopyReports[i].Error = diagnostic(m.CopyReports[i].Error, "model copy failed")
		}
		for i := range m.EnvironmentReports {
			m.EnvironmentReports[i].Error = diagnostic(m.EnvironmentReports[i].Error, "environment build failed")
		}
	}
	for _, j := range s.Jobs {
		// Status is a summary, never a copy of a submitted command, environment,
		// program arguments, extension files, or local filesystem paths.
		spec := Spec{Name: j.Spec.Name, Project: j.Spec.Project, Priority: j.Spec.Priority, Kind: j.Spec.Kind, Resumable: j.Spec.Resumable, Deadline: j.Spec.Deadline, StartBy: j.Spec.StartBy}
		if j.Spec.Program != nil {
			spec.Program = &Program{Type: j.Spec.Program.Type}
		}
		spec.Needs = j.Spec.Needs
		j.Spec = spec
		j.Extensions = nil
		j.EnvironmentRecipes = nil
		j.EnvironmentFailureLog = ""
		j.Checkpoint = ""
		if strings.HasPrefix(j.Reason, "hypha waiting: ") {
			j.Reason = "hypha waiting: " + availabilityWords(strings.TrimPrefix(j.Reason, "hypha waiting: "))
		}
		if j.State == "failed" {
			switch {
			case j.Reason == "out of memory after one retry on a larger hypha", j.Reason == "out of memory; previous capability unavailable":
				// Fixed scheduler diagnostics contain no payload or machine paths.
			case strings.HasPrefix(j.Reason, "production start deadline exceeded:"):
				j.Reason = "production start deadline exceeded"
			case strings.HasPrefix(j.Reason, "production cannot start: no eligible host is enabled;"):
				j.Reason = "production cannot start: no eligible host is enabled"
			default:
				j.Reason = ceilingDiagnostic(j.Reason, "execution failed")
			}
		}
	}
	return s, err
}

func statusEnvironmentName(name string) string {
	if strings.ContainsAny(name, "/\\") {
		return "required software"
	}
	return name
}

func safeAvailabilityReason(reason string) string {
	switch reason {
	case "host CPU thermal coverage unavailable", "waiting for 5 idle minutes", "on battery", "cooling down", "owner signals stale", "power source unavailable", "external power required", "owner activity unavailable", "heat sensing unavailable":
		return reason
	default:
		return "availability unknown"
	}
}
