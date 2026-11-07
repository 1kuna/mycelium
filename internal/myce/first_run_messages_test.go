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

func TestFirstRunMessagesExtensionAdvice(t *testing.T) {
	j := &Job{Spec: Spec{Name: "sample", Project: "test", Kind: "sample", Isolation: "sandboxed"}}
	if err := j.Spec.Validate(); err == nil || !strings.Contains(err.Error(), "extension registration owns isolation") {
		t.Fatalf("extension job override unexpectedly accepted: %v", err)
	}
	advice := NativeIsolationAdvice(j)
	if !strings.Contains(advice, "publish a new extension version") || !strings.Contains(advice, "existing jobs keep their frozen policy") || strings.Contains(advice, "job YAML") {
		t.Fatalf("unusable extension sandboxing advice: %s", advice)
	}
}
