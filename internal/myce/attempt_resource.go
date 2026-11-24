package myce

import (
	"fmt"
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
		limit := 0.0
		if pooledMemory(j) {
			limit = memoryPools(j).LimitGB
		} else if j.MemoryEnvelope != nil {
			limit = max(j.MemoryEnvelope.LimitHostGB, j.MemoryEnvelope.HostGB)
		}
		if !ceiling && limit > 0 && (e.OOM > 0 || e.Max > 0 && killed || !pooledMemory(j) && e.OOMGroupKill > 0) {
			r.Error = fmt.Sprintf("exceeded its %g GiB memory ceiling", limit)
			ceiling = true
		}
		if killed && !ceiling && pooledMemory(j) {
			r.ExecutionCause = &RuntimeCause{Operation: "command", Class: memoryPressureCause}
			r.OutOfMemory, r.Interrupted = false, true
			return r
		}
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
