package main

import (
	"strings"

	"mycelium/internal/myce"
)

// Human CLI wording only: retain the server's reason, blocker and isolation
// fields verbatim for JSON clients and saved observations.
func jobReasonText(j *myce.Job) string {
	text := myce.JobReason(j)
	if j.Blocker != nil && j.Blocker.Code == "unknown_observation" {
		text = strings.Replace(text, "unknown_observation:", "Waiting for worker information:", 1)
		text += "; if this continues, run myce status and ask the queue owner to check worker connections"
	}
	if strings.Contains(j.Isolation, "unsandboxed:") {
		text = strings.Replace(text, "unsandboxed: plain native job defaults to unsandboxed in v1; set isolation: sandboxed to opt in", "plain commands run without sandbox protection", 1)
		text += "; this job can access the worker account's files; run only trusted code, or set isolation: sandboxed in job YAML (see docs/native-isolation.md)"
	}
	return statusSafetyText(text)
}

func statusSafetyText(text string) string {
	return strings.ReplaceAll(text, "GPU isolation is not enforced for CPU work (Metal has no device mask)", "CPU jobs can still access this Mac's GPU; run only trusted code (see docs/native-isolation.md)")
}
