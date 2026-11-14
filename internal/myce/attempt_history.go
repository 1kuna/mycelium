package myce

import (
	"log"
	"strings"
	"time"
)

// AttemptOutcome preserves the existing classifier's facts, not a new cause
// taxonomy. State is the queue decision after this attempt (queued or terminal).
// DeviceFault is queue-owned retry eligibility evidence. Other fields remain
// diagnostic evidence, never release proof or a retry/accounting input.
type AttemptOutcome struct {
	FaultEvidence     []*FaultEvidence        `json:"fault_evidence,omitempty"`
	SignalEvidence    *SignalEvidence         `json:"signal_evidence,omitempty"`
	DeviceFault       *DeviceFaultLoss        `json:"device_fault,omitempty"`
	ExecutionEvidence *ExecutionEvidence      `json:"execution_evidence,omitempty"`
	StopObservation   *AttemptStopObservation `json:"stop_observation,omitempty"`
	StagingTimings    *StagingTimings         `json:"staging_timings,omitempty"`
	inputsStaged      bool                    // transient positive staging evidence; not part of the wire/history shape.
	Placement         *PlacementDecision      `json:"placement,omitempty"`
	Diagnostics       *AttemptDiagnostics     `json:"diagnostics,omitempty"`
	Attempt           int                     `json:"attempt"`
	Machine           string                  `json:"machine"`
	Session           string                  `json:"session"`
	EndedAt           time.Time               `json:"ended_at"`
	State             string                  `json:"state"`
	Reason            string                  `json:"reason,omitempty"`
	ExecutionCause    *RuntimeCause           `json:"execution_cause,omitempty"`
	CleanupCause      *RuntimeCause           `json:"cleanup_cause,omitempty"`
	Forced            bool                    `json:"forced,omitempty"`
	Interrupted       bool                    `json:"interrupted,omitempty"`
	AdmissionRequeue  bool                    `json:"admission_requeue,omitempty"`
	OutOfMemory       bool                    `json:"out_of_memory,omitempty"`
	PoliteStop        bool                    `json:"polite_stop,omitempty"`
	OperatorStop      bool                    `json:"operator_stop,omitempty"`
	PriorityStop      *PriorityPreemption     `json:"priority_stop,omitempty"`
}

// These are queue-written stop records, not worker error text. This diagnostic
// provenance does not establish signal delivery or change retry charging.
func historyPlannedStop(j *Job) bool {
	return j.State == "stopping" && priorityStop(j) == nil && (j.Reason == "machine leaving" ||
		strings.HasPrefix(j.Reason, "hypha waiting: ") || strings.HasPrefix(j.Reason, "making room for "))
}

func attemptOutcome(j *Job, r Result) AttemptOutcome {
	return AttemptOutcome{
		FaultEvidence: j.FaultEvidence, SignalEvidence: r.SignalEvidence,
		ExecutionEvidence: safeExecutionEvidence(r.ExecutionEvidence, j.Attempt),
		inputsStaged:      r.Success || j.StartedAt != nil || j.InputsStagedAt != nil || r.InputsStagedAt != nil,
		StagingTimings:    j.StagingTimings,
		Placement:         j.Placement,
		Attempt:           j.Attempt, Machine: j.Machine, Session: j.Session,
		ExecutionCause: safeAttemptRuntimeCause(j, r.ExecutionCause), CleanupCause: safeAttemptRuntimeCause(j, r.CleanupCause),
		Forced: r.Forced, Interrupted: r.Interrupted, OutOfMemory: r.OutOfMemory,
		PoliteStop: r.PoliteStop, OperatorStop: r.OperatorStop || plannedMaintenanceStop(j),
		PriorityStop: r.PriorityStop,
	}
}

func (q *Queue) recordAttemptOutcome(j *Job, outcome AttemptOutcome) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
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
	if outcome.StagingTimings == nil && j.Attempt == outcome.Attempt {
		outcome.StagingTimings = j.StagingTimings
	}
	outcome.StopObservation = q.attemptStopObservation(outcome.Machine, outcome.Session)
	for _, old := range j.AttemptHistory {
		if old.Attempt == outcome.Attempt && old.Machine == outcome.Machine && old.Session == outcome.Session {
			outcome.StopObservation = old.StopObservation
			outcome.DeviceFault = old.DeviceFault
			if outcome.StagingTimings == nil {
				outcome.StagingTimings = old.StagingTimings
			}
			break
		}
	}
	outcome.EndedAt = q.clock.Now()
	outcome.State = j.State
	retainFinalizationSummary(j, &outcome)
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
	q.recordLaunchRefusalHealth(j, outcome)
	q.recordRemainingAttemptHealth(j, outcome)
	for i, old := range j.AttemptHistory {
		if old.Attempt == outcome.Attempt && old.Machine == outcome.Machine && old.Session == outcome.Session {
			j.AttemptHistory[i] = outcome
			return
		}
	}
	j.AttemptHistory = append(j.AttemptHistory, outcome)

}
