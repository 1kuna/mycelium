package myce

// Sandboxed native execution requires an advertised kernel boundary. Only the
// frozen stage policy can waive that requirement, including legacy registrations.
func nativeNetworkFits(j *Job, m *Machine) bool {
	if j.Spec.ephemeral() && !contains(m.Capabilities, ephemeralCapability) {
		return false
	}
	if nativeUnsandboxed(j) {
		return true
	}
	if !nativeBoundaryReported(m) {
		return false
	}
	if j.Spec.Needs.Network && !contains(m.Capabilities, "native_network") {
		return false
	}
	return !nativeCIRequired(j) || contains(m.Capabilities, nativeCICapability)
}

const ephemeralRequirements = "ephemeral worker data requires Linux PID/mount user namespaces and a qualified core route"

func ephemeralHostReason(m *Machine) string {
	switch m.EphemeralRefusal {
	case ephemeralLinuxRequired:
		return "ephemeral worker data is supported only on Linux"
	case ephemeralCoreRouteUnqualified:
		return "the Linux core route is unqualified"
	case ephemeralDetachedUnqualified:
		return "the Linux core route or delegated cgroup for detached attempts is unqualified"
	case ephemeralCoreLimitUnqualified:
		return "the Linux core-limit helper is unqualified"
	}
	// Older Linux workers advertise Landlock only after a namespace policy
	// denial. A bare missing capability cannot establish a concrete cause.
	if m.EphemeralRefusal == ephemeralNamespacePolicy || contains(m.Capabilities, "native_isolation_landlock") {
		return "Linux PID/mount user namespaces were refused by host policy (see docs/native-isolation.md#optional-administrator-action-per-host-apparmor-user-namespaces for the AppArmor administrator option)"
	}
	if m.EphemeralRefusal == ephemeralNamespaceRequired {
		return "Linux PID/mount user namespaces are unqualified"
	}
	if os := machineTarget(m).OS; os != "" && os != "linux" {
		return "ephemeral worker data is supported only on Linux"
	}
	return ephemeralRequirements
}

func nativeBoundaryReported(m *Machine) bool {
	return contains(m.Capabilities, "native_isolation_landlock") || contains(m.Capabilities, "native_isolation_namespace") || contains(m.Capabilities, "native_isolation_seatbelt")
}

func nativeNetworkRequired(j *Job) bool {
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
	if message != "" && !nativeUnsandboxed(&j) && !j.Spec.Needs.Network {
		return message + "; native networking is offline except reserved sidecar ports; declare needs.network: true for general outbound access (requires a capable host)"
	}
	return message
}
