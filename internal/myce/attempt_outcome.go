package myce

// Infrastructure is positive stop/recovery evidence, never an exit-code guess.
// An observed payload failure (especially OOM) survives a concurrent stop.
type attemptCause struct {
	Infrastructure bool
	StageFailure   bool
	OutOfMemory    bool
	AmbiguousKill  bool
}

func classifyAttemptOutcome(result Result, cause attemptCause) Result {
	result.OutOfMemory = result.OutOfMemory || cause.OutOfMemory
	result.Interrupted = cause.Infrastructure && !cause.StageFailure && !result.OutOfMemory
	if cause.StageFailure || result.OutOfMemory {
		result.Success = false
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
