package myce

import (
	"errors"
	"os/exec"
	"syscall"
)

// Infrastructure is positive stop/recovery evidence, never an exit-code guess.
// An observed payload failure (especially OOM) survives a concurrent stop.
type attemptCause struct {
	Infrastructure bool
	StageFailure   bool
	OutOfMemory    bool
	AmbiguousKill  bool
	StopSignal     bool // Adapter confirmed a stop signal (including a supervisor's encoded wait status).
}

func classifyAttemptOutcome(result Result, cause attemptCause) Result {
	result.OutOfMemory = result.OutOfMemory || cause.OutOfMemory
	result.Interrupted = cause.Infrastructure && !result.Success && !cause.StageFailure && !result.OutOfMemory
	if cause.StageFailure || result.OutOfMemory {
		result.Interrupted = false
		result.Success = false
		result.Forced = false
	}
	if result.Success {
		result.Forced = false
	}
	if cause.AmbiguousKill && !result.Interrupted && !result.OutOfMemory {
		if result.Error != "" {
			result.Error += "; "
		}
		result.Error += "SIGKILL without an infrastructure stop; treated as stage failure"
	}
	return result
}

// A native signal delivered during a stop is interruption evidence. Ordinary
// exits (including exit 7 after TERM) remain payload outcomes.
func nativeOutcome(result Result, waitErr error, cause attemptCause) Result {
	var exit *exec.ExitError
	if errors.As(waitErr, &exit) {
		status, ok := exit.Sys().(syscall.WaitStatus)
		stoppedSignal := cause.StopSignal || ok && status.Signaled() && (status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGKILL)
		cause.StageFailure = cause.StageFailure || !cause.Infrastructure || !stoppedSignal
		cause.AmbiguousKill = ok && status.Signaled() && status.Signal() == syscall.SIGKILL
	} else if waitErr != nil {
		cause.StageFailure = true
	}
	return classifyAttemptOutcome(result, cause)
}

// An operator marker records intent without changing runtime evidence. It cannot reinterpret a durable completion or stage failure.
func operatorOutcome(result Result, stopped bool) Result {
	result.OperatorStop = result.OperatorStop || stopped
	return result
}
