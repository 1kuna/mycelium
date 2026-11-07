package myce

import (
	"strings"
	"testing"
)

func TestFirstRunMessages(t *testing.T) {
	for _, status := range []string{darwinRSSGuardStatus, "memory ceiling not enforced on this host (requires a delegated worker service; foreground join is admission-only)"} {
		text := MemoryProtectionNotice(status)
		if !strings.Contains(text, "Linux worker service") || !strings.Contains(text, "--require-memory-ceilings") {
			t.Fatalf("missing explanation and next step: %s", text)
		}
	}
	const enforced = "memory ceilings enforced (cgroup v2)"
	if got := MemoryProtectionNotice(enforced); got != enforced {
		t.Fatalf("enforced notice changed: %s", got)
	}
}
