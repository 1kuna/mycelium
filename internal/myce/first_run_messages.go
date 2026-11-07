package myce

import "strings"

// NativeIsolationAdvice points to the authoring boundary that owns the policy.
// Extension jobs cannot override the isolation of their frozen registration.
func NativeIsolationAdvice(j *Job) string {
	if j.Spec.extensionJob() || j.Extension != nil || len(j.Extensions) > 0 {
		return "Run only trusted code, or publish a new extension version with isolation: sandboxed and submit a new job; existing jobs keep their frozen policy. See docs/native-isolation.md."
	}
	return "Run only trusted code, or set isolation: sandboxed in job YAML for a new job; see docs/native-isolation.md."
}

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
