package myce

// An endpoint is an explicit opt-in, not a health check. Missing advertisement
// never implies Docker support, including for workers predating this contract.
func dockerRuntimeFits(j *Job, m *Machine) bool {
	return !j.Spec.Run.Docker || contains(m.Capabilities, "docker") && (!dockerFileJob(j.Spec) || contains(m.Capabilities, dockerFilesCapability)) && (!dockerNeedsEnvelope(j.Spec) || contains(m.Capabilities, "docker_envelope")) && m.CapabilityProfile != nil && m.CapabilityProfile.OS == "linux"
}

func dockerRuntimeWaitingReason(j *Job, machines map[string]*Machine) string {
	if !j.Spec.Run.Docker {
		return ""
	}
	enabled := false
	for _, m := range machines {
		if m.Gone || m.Leaving || !machineAllowed(j.Spec.AllowedMachines, m.ID) {
			continue
		}
		if contains(m.Capabilities, "docker") {
			enabled = true
		}
		if dockerRuntimeFits(j, m) {
			return ""
		}
	}
	if enabled {
		if dockerFileJob(j.Spec) {
			return "needs docker_files_v1 runtime on Linux; no worker supports it"
		}
		if dockerNeedsEnvelope(j.Spec) {
			return dockerEnvelopeUnavailable
		}
		return "needs docker runtime on Linux; no worker supports it"
	}
	return "needs docker runtime; no worker has it enabled"
}
