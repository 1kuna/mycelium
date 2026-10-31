package myce

// Sandboxed native execution requires an advertised kernel boundary. Only the
// frozen stage policy can waive that requirement, including legacy registrations.
func nativeNetworkFits(j *Job, m *Machine) bool {
	if j.Spec.Run.Docker || nativeUnsandboxed(j) {
		return true
	}
	if !contains(m.Capabilities, "native_isolation_landlock") && !contains(m.Capabilities, "native_isolation_namespace") && !contains(m.Capabilities, "native_isolation_seatbelt") {
		return false
	}
	if j.Spec.Needs.Network && !contains(m.Capabilities, "native_network") {
		return false
	}
	return true
}

func nativeNetworkRequired(j *Job) bool {
	if j.Spec.Run.Docker {
		return false
	}
	if j.Spec.Needs.Network {
		return true
	}
	for _, e := range j.Extensions {
		for _, v := range e.Variants {
			if v.Needs.Network {
				return true
			}
		}
	}
	return false
}

// A child may only report EPERM/exit status for a network refusal. Keep the
// declaration and capability remedy in its attempt diagnostic.
func nativeAttemptError(j Job, message string) string {
	if message == "exit status 126" {
		message += "; program could not execute; check whether it was built for another architecture, name its required arch in targets, or declare portable: true if it runs on any arch"
	}
	if message != "" && !j.Spec.Run.Docker && !nativeUnsandboxed(&j) && !j.Spec.Needs.Network {
		return message + "; native networking is offline except reserved sidecar ports; declare needs.network: true for general outbound access (requires a capable host)"
	}
	return message
}
