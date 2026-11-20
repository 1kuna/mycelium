package myce

import (
	"fmt"
	"strings"
)

// Normalize old workers' command exits before memory learning and coverage.
// Attempt-parent OOM events prove a memory stop; pressure/max events alone do
// not. Keep successful and infrastructure-interrupted executions unchanged.
func classifyAttemptResourceFailure(j *Job, r Result) Result {
	if r.Success {
		return r
	}
	kernelOOM := false
	if p := r.MemoryPeak; p != nil && p.Source != processGroupMemorySource && p.Events != nil && j.MemoryEnvelope != nil && j.MemoryEnvelope.HostGB > 0 && memoryConstraintResult(j.StartedAt != nil, r) {
		e := p.Events
		kernelOOM = e.OOM > 0 || e.OOMGroupKill > 0 || e.OOMKill > 0 && e.Max > 0
	}
	ceiling := strings.HasPrefix(ceilingDiagnostic(r.Error, ""), "exceeded its ")
	rss := r.ExecutionCause != nil && r.ExecutionCause.Class == rssLimitExceeded && safeRSSLimit(r.ExecutionCause.RSSLimit) != nil
	if !kernelOOM && !ceiling && !rss {
		return r
	}
	r.OutOfMemory = true
	// Positive OOM evidence wins a concurrent infrastructure/preemption stop.
	r.Interrupted = false
	if r.ExecutionCause == nil || r.ExecutionCause.Class == "command_exit" {
		cause := RuntimeCause{Operation: "command", Class: "resource_limit"}
		if r.ExecutionCause != nil {
			cause.ExitCode = r.ExecutionCause.ExitCode
		}
		r.ExecutionCause = &cause
	}
	if kernelOOM && !ceiling && !rss && j.MemoryEnvelope != nil && j.MemoryEnvelope.HostGB > 0 && !floorCharged(j.MemoryEnvelope) && j.MemoryEnvelope.Enforcement != learnedMemoryGuardEnforcement {
		r.Error = fmt.Sprintf("exceeded its %g GiB memory ceiling", j.MemoryEnvelope.HostGB)
	}
	return r
}
