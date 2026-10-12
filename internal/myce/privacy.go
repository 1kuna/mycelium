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
	s, err := q.Snapshot()
	for i := range s.LoadedModels {
		s.LoadedModels[i].Error = ceilingDiagnostic(s.LoadedModels[i].Error, "model load or unload failed")
	}
	for _, m := range s.Machines {
		m.MemoryEnforcement = memoryEnforcementDiagnostic(m.MemoryEnforcement)
		m.HostMemoryError = diagnostic(m.HostMemoryError, "host memory probe failed")
		m.MemoryError = diagnostic(m.MemoryError, "memory probe failed")
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
		j.EnvironmentFailureLog = ""
		if j.State == "failed" {
			switch {
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
