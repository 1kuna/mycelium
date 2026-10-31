package myce

import (
	"log"
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
	Placement      *PlacementDecision  `json:"placement,omitempty"`
	Diagnostics    *AttemptDiagnostics `json:"diagnostics,omitempty"`
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
		Placement: j.Placement,
		Attempt:   j.Attempt, Machine: j.Machine, Session: j.Session,
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
	// Requeue records an outcome before Complete adds its full result. Carry
	// the current safe launch detail into that immutable ledger entry too.
	if outcome.ExecutionCause == nil && j.Attempt == outcome.Attempt && j.ExecutionCause != nil && j.ExecutionCause.Predicate != "" {
		outcome.ExecutionCause = safeRuntimeCause(j.ExecutionCause)
	}
	if j.Placement != nil && j.Placement.Attempt == outcome.Attempt {
		outcome.Placement = j.Placement
	}
	outcome.EndedAt = q.clock.Now()
	outcome.State = j.State
	// Reuse status redaction, including availability text supplied by workers.
	outcome.Reason = statusJobCopy(&Job{State: j.State, Reason: j.Reason, ExecutionCause: outcome.ExecutionCause}).Reason
	e := JobLedgerEntry{Attempt: outcome.Attempt, Machine: outcome.Machine, Session: outcome.Session, Kind: "outcome", Reason: outcome.Reason, State: outcome.State}
	if e.Reason == "" {
		e.Reason = e.State
	}
	duplicate := false
	for i := len(j.LedgerEntries) - 1; i >= 0; i-- {
		old := j.LedgerEntries[i]
		if old.Attempt == e.Attempt && old.Kind == e.Kind {
			duplicate = old.State == e.State && old.Reason == e.Reason
			break
		}
	}
	if !duplicate {
		q.appendJobLedger(j, e)
		if cause := outcome.ExecutionCause; cause != nil && cause.Predicate != "" {
			log.Printf("event=launch_outcome job=%s attempt=%d predicate=%s reason=%q detail=%q", j.ID, outcome.Attempt, cause.Predicate, cause.Reason, cause.String())
		}
	}
	q.alertAttemptLoss(j, outcome, false)
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
