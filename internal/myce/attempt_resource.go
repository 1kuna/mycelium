package myce

import (
	"strings"
)

// Normalize old workers' command exits before memory learning and coverage.
// Attempt-parent OOM events prove a memory stop; pressure/max events alone do
// not. Keep successful and infrastructure-interrupted executions unchanged.

func classifyAttemptResourceFailure(j *Job, r Result) Result {
	if r.Success || r.ExecutionCause != nil && r.ExecutionCause.Class == memoryPressureCause {
		return r
	}
	ceiling := strings.HasPrefix(ceilingDiagnostic(r.Error, ""), "exceeded its ")
	rss := r.ExecutionCause != nil && r.ExecutionCause.Class == rssLimitExceeded && safeRSSLimit(r.ExecutionCause.RSSLimit) != nil
	if p := r.MemoryPeak; p != nil && p.Source != processGroupMemorySource && p.Events != nil && memoryConstraintResult(j.StartedAt != nil, r) {
		e := p.Events
		killed := e.OOMKill > 0 || e.OOMGroupKill > 0
		if killed && e.OOM == 0 && e.Max == 0 && !ceiling {
			r.ExecutionCause = &RuntimeCause{Operation: "command", Class: memoryPressureCause}
			r.OutOfMemory, r.Interrupted = false, true
			return r
		}
		ceiling = ceiling || e.OOM > 0 && killed || e.Max > 0 && killed
	}
	if ceiling || rss {
		r.OutOfMemory, r.Interrupted = true, false
		if r.ExecutionCause == nil || r.ExecutionCause.Class == "command_exit" {
			cause := RuntimeCause{Operation: "command", Class: "resource_limit"}
			if r.ExecutionCause != nil {
				cause.ExitCode = r.ExecutionCause.ExitCode
			}
			r.ExecutionCause = &cause
		}
	}
	return r
}
