package myce

import (
	"os"
	"path/filepath"
)

// A real null-device descriptor avoids both pipe backpressure and inheritance
// of the worker's stdout/stderr (which may be connected to journald).
func openCommandOutput(dir, name, policy string, flags int) (*os.File, error) {
	if policy == "none" {
		return os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	}
	return os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_WRONLY|flags, 0600)
}

func syncCommandOutput(output *os.File) error {
	if output.Name() == os.DevNull {
		return nil // fsync is not supported by the null device.
	}
	return output.Sync()
}

// Missing capability advertisement denotes an older worker. Never infer
// software support from its hardware profile or inventory.
func logPolicyFits(policy string, m *Machine) bool {
	return policy != "none" || contains(m.Capabilities, "logs_none")
}

func (q *Queue) logsWaitingReason(j *Job) string {
	model, hasModel := q.model(j.Spec.Needs.Model)
	if j.Spec.Logs != "none" && (!hasModel || model.Logs != "none") {
		return ""
	}
	for _, m := range q.s.Machines {
		if m.Gone || m.Leaving || !q.projectAllows(j.Spec.Project, m.ID) || len(j.Spec.AllowedMachines) > 0 && !contains(j.Spec.AllowedMachines, m.ID) {
			continue
		}
		if hasModel && (!modelFenceAllows(model, m) || !q.projectAllows(model.Project, m.ID)) {
			continue
		}
		if contains(m.Capabilities, "logs_none") {
			return ""
		}
	}
	return "waiting for a worker with logs_none capability (logs: none)"
}
