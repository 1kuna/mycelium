package myce

import "strings"

// MemoryProtectionNotice explains startup/status limits without changing the
// capability strings on the wire.
func MemoryProtectionNotice(enforcement string) string {
	if enforcement == darwinRSSGuardStatus {
		return "Memory protection checks sampled process usage; it cannot enforce a kernel memory limit on this Mac. For enforced limits, use a Linux worker service with --require-memory-ceilings."
	}
	if strings.HasPrefix(enforcement, "memory ceiling not enforced") {
		return enforcement + ". Jobs can exceed their memory allowance; for enforced limits, use a Linux worker service with delegated cgroup v2 memory and --require-memory-ceilings."
	}
	return enforcement
}
