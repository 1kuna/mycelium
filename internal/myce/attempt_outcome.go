package myce

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

// These are runtime facts, not adapter conclusions. Intent is deliberately absent.
// Times are wall-clock evidence (not scheduling deadlines). A signal is added
// only after successful delivery and persistence; exit observation is immutable.
type deliveredSignal struct {
	Signal string    `json:"signal"`
	At     time.Time `json:"at"`
}
type memberExit struct {
	Status     int       `json:"status"`
	ObservedAt time.Time `json:"observed_at"`
}
type infrastructureFact struct {
	Kind   string    `json:"kind"`
	At     time.Time `json:"at"`
	Member string    `json:"member,omitempty"`
	Port   string    `json:"port,omitempty"`
	Group  int       `json:"group,omitempty"`
}

const (
	launchRefused     = "launch_refused"
	launchFailed      = "launch_failed"
	environmentFailed = "environment_failed"
	foreignPort       = "foreign_port"
)

type memberOutcome struct {
	Infrastructure  []infrastructureFact `json:"infrastructure_facts,omitempty"`
	Signals         []deliveredSignal    `json:"delivered_signals,omitempty"`
	Exit            *memberExit          `json:"exit,omitempty"`
	ObservationLost bool                 `json:"observation_lost,omitempty"`
}
type attemptCause struct {
	PriorityStop *PriorityPreemption
	memberOutcome
	OutOfMemory bool
	Cancelled   bool
	Stale       bool
}

func (facts memberOutcome) deliveredBeforeExit() bool {
	if facts.Exit == nil {
		return false
	}
	for _, signal := range facts.Signals {
		if !signal.At.IsZero() && (signal.Signal == "TERM" || signal.Signal == "KILL") && signal.At.Before(facts.Exit.ObservedAt) {
			return true
		}
	}
	return false
}

func (facts memberOutcome) recordedInfrastructure() bool {
	for _, fact := range facts.Infrastructure {
		if fact.At.IsZero() {
			continue
		}
		switch fact.Kind {
		case launchRefused, launchFailed, environmentFailed:
			return true
		case foreignPort:
			if fact.Port != "" && fact.Group > 0 {
				return true
			}
		}
	}
	return false
}

// The wrapper carries only facts already saved by Mycelium, preserving error
// identity for callers that retry admission or retain unconfirmed custody.
type recordedStartError struct {
	cause error
	facts memberOutcome
}

func (e *recordedStartError) Error() string { return e.cause.Error() }
func (e *recordedStartError) Unwrap() error { return e.cause }

func (s *workerStore) startFailure(j Job, kind string, cause error) error {
	facts := memberOutcome{Infrastructure: []infrastructureFact{{Kind: kind, At: time.Now().UTC()}}}
	if err := saveWorkerJSON(s.dir, fmt.Sprintf("launch-%s-%d.outcome.json", j.ID, j.Attempt), facts); err != nil {
		return errors.Join(cause, err)
	}
	return &recordedStartError{cause: cause, facts: facts}
}
func startFailureOutcome(owner Result, err error) Result {
	var recorded *recordedStartError
	facts := memberOutcome{}
	if errors.As(err, &recorded) {
		facts = recorded.facts
	}
	var rejection *rejectionError
	if errors.As(err, &rejection) && rejection.code == cpuIsolationUnavailable {
		owner.ExecutionCause = &RuntimeCause{Operation: "launch", Class: cpuIsolationUnavailable}
	}
	if errors.Is(err, errWorkerMemory) {
		owner.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit"}
	}
	return classifyAttemptOutcome(owner, attemptCause{memberOutcome: facts})
}

func classifyAttemptOutcome(result Result, facts attemptCause) Result {
	result.OutOfMemory = result.OutOfMemory || facts.OutOfMemory
	result.Interrupted = false
	result.PriorityStop = nil
	if facts.Stale || facts.Cancelled {
		result.Success, result.Forced = false, false
		return result
	}
	if result.OutOfMemory || facts.Exit != nil && facts.Exit.Status != 0 {
		result.Success = false
	}
	// A checkpoint handler can exit zero after TERM. Delivered priority stops
	// interrupt that partial attempt; intent or a signal after observed exit
	// cannot override an already completed payload.
	if facts.PriorityStop != nil && facts.deliveredBeforeExit() && !result.OutOfMemory {
		result.Success = false
	}
	if !result.Success && !result.OutOfMemory {
		result.Interrupted = facts.ObservationLost || facts.deliveredBeforeExit() || facts.recordedInfrastructure()
	}
	if !result.Interrupted {
		result.Forced = false
	}
	if facts.Exit != nil && facts.Exit.Status == 137 && !result.Interrupted && !result.OutOfMemory {
		if result.Error != "" {
			result.Error += "; "
		}
		result.Error += "SIGKILL; interruption cause unconfirmed; treated as stage failure"
	}
	return withStopProvenance(result, facts.PriorityStop)
}

// Planned intent can annotate a proven interruption, never manufacture one.
func withStopProvenance(result Result, stop *PriorityPreemption) Result {
	result.PriorityStop = nil
	if result.Interrupted && !result.Success && !result.OutOfMemory {
		result.PriorityStop = stop
	}
	return result
}

// The wait status supplies only the exit status. It never establishes cause.
func observedNativeExit(cmd *exec.Cmd, at time.Time) *memberExit {
	code := 1
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			code = 128 + int(status.Signal())
		}
	}
	return &memberExit{Status: code, ObservedAt: at}
}

// An operator marker is provenance, never evidence of signal delivery.
func operatorOutcome(result Result, stopped bool) Result {
	result.OperatorStop = result.OperatorStop || stopped
	return result
}
