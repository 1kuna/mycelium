package myce

import (
	"strings"
	"time"
)

// Three infrastructure endings (two retries), plus the one allowed OOM retry.
// Planned requeues can repeat indefinitely; retain only this latest window.
const attemptHistoryLimit = max(3, DefaultProductionInfrastructureRetries+1) + 1

// AttemptOutcome preserves the existing classifier's facts, not a new cause
// taxonomy. State is the queue decision after this attempt (queued or terminal).
// It is diagnostic evidence, never release proof or a retry/accounting input.
type AttemptOutcome struct {
	Attempt        int                 `json:"attempt"`
	Machine        string              `json:"machine"`
	Session        string              `json:"session"`
	EndedAt        time.Time           `json:"ended_at"`
	State          string              `json:"state"`
	Reason         string              `json:"reason,omitempty"`
	ExecutionCause *RuntimeCause       `json:"execution_cause,omitempty"`
	CleanupCause   *RuntimeCause       `json:"cleanup_cause,omitempty"`
	Interrupted    bool                `json:"interrupted,omitempty"`
	OutOfMemory    bool                `json:"out_of_memory,omitempty"`
	PoliteStop     bool                `json:"polite_stop,omitempty"`
	OperatorStop   bool                `json:"operator_stop,omitempty"`
	PriorityStop   *PriorityPreemption `json:"priority_stop,omitempty"`
}

// These are queue-written stop records, not worker error text. This diagnostic
// provenance does not establish signal delivery or change retry charging.
func historyPlannedStop(j *Job) bool {
	return j.State == "stopping" && priorityStop(j) == nil && (j.Reason == "machine leaving" ||
		strings.HasPrefix(j.Reason, "hypha waiting: ") || strings.HasPrefix(j.Reason, "making room for "))
}

func attemptOutcome(j *Job, r Result) AttemptOutcome {
	return AttemptOutcome{
		Attempt: j.Attempt, Machine: j.Machine, Session: j.Session,
		ExecutionCause: safeRuntimeCause(r.ExecutionCause), CleanupCause: safeRuntimeCause(r.CleanupCause),
		Interrupted: r.Interrupted, OutOfMemory: r.OutOfMemory,
		PoliteStop: r.PoliteStop, OperatorStop: r.OperatorStop || plannedMaintenanceStop(j),
		PriorityStop: r.PriorityStop,
	}
}

func (q *Queue) recordAttemptOutcome(j *Job, outcome AttemptOutcome) {
	if outcome.Attempt == 0 || outcome.Machine == "" || outcome.Session == "" {
		return // Never manufacture an attempt for an unassigned job.
	}
	outcome.EndedAt = q.clock.Now()
	outcome.State = j.State
	// Reuse status redaction, including availability text supplied by workers.
	outcome.Reason = statusJobCopy(&Job{State: j.State, Reason: j.Reason, ExecutionCause: outcome.ExecutionCause}).Reason
	for i, old := range j.AttemptHistory {
		if old.Attempt == outcome.Attempt && old.Machine == outcome.Machine && old.Session == outcome.Session {
			j.AttemptHistory[i] = outcome
			return
		}
	}
	j.AttemptHistory = append(j.AttemptHistory, outcome)
	if len(j.AttemptHistory) > attemptHistoryLimit {
		j.AttemptHistory = append([]AttemptOutcome(nil), j.AttemptHistory[len(j.AttemptHistory)-attemptHistoryLimit:]...)
	}
}
