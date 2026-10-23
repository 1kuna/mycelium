package myce

import (
	"strings"
	"testing"
)

func TestExtensionPreparationWaitingReasonShowsFailure(t *testing.T) {
	e := compositionExtension(t, false)
	m := machine("worker", "fake")
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
	m.EnvironmentBuilder = true
	m.EnvironmentReports = []EnvironmentReport{{Key: extensionPreparationKey(e, &m), State: "failed", Error: "download stalled"}}
	j := &Job{Spec: Spec{ExtensionCheck: &ExtensionCheckRequest{Work: "extension:" + e.Name}}, Extensions: []Extension{e}}
	reason := environmentWaitingReason(j, &m)
	if !strings.Contains(reason, extensionKey(e)) || !strings.Contains(reason, "download stalled") || !strings.Contains(reason, "blocked") {
		t.Fatalf("waiting cause missing: %q", reason)
	}
}
