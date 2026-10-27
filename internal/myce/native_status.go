package myce

// NativeIsolationStatus describes the kernel boundary reported by the running
// worker. In particular, sidecar port exceptions are not full offline isolation.
func NativeIsolationStatus(m Machine) string {
	switch {
	case contains(m.Capabilities, "native_isolation_landlock"):
		return "native isolation: Landlock + seccomp; partial network isolation (reserved sidecar TCP ports permit off-host traffic); general egress unavailable"
	case contains(m.Capabilities, "native_isolation_namespace"):
		return "native isolation: user/mount/PID/IPC/network namespaces; general egress unavailable"
	case contains(m.Capabilities, "native_isolation_seatbelt"):
		return "native isolation: Seatbelt; outbound requires needs.network: true"
	case contains(m.Capabilities, "native_isolation_unavailable"):
		return "native isolation unavailable; native launch refused"
	default:
		return "native isolation unreported"
	}
}
