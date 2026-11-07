package myce

// ExecutionEvidence describes execution boundaries for one attempt, not the
// job's entire retry history or release custody. A missing record is unknown.
// May-have-run deliberately does not claim that exec succeeded.
type ExecutionEvidence struct {
	Attempt                int    `json:"attempt"`
	Main                   string `json:"main"`
	Sidecars               string `json:"sidecars"`
	EnvironmentPreparation string `json:"environment_preparation"`
}

const (
	executionNotStarted = "not_started"
	executionMayHaveRun = "may_have_run"
	executionUnknown    = "unknown"
)

func unstartedExecution(attempt int) *ExecutionEvidence {
	return &ExecutionEvidence{attempt, executionNotStarted, executionNotStarted, executionNotStarted}
}

// The actor retains one store across availability/checkpoint retries. Keep the
// observed boundaries monotonic until the physical identity changes. Restored
// state without a completed result never establishes a fresh no-execution proof.
func (s *workerStore) startExecutionEvidence(j Job) *ExecutionEvidence {
	key := jobKey(j)
	if s.liveEvidence == nil || s.liveEvidenceKey != key {
		s.liveEvidenceKey = key
		s.liveEvidence = unstartedExecution(j.Attempt)
		if s.state.Job != nil && jobKey(*s.state.Job) == key {
			s.liveEvidence.Main = executionUnknown
			s.liveEvidence.Sidecars = executionUnknown
			s.liveEvidence.EnvironmentPreparation = executionUnknown
		}
	}
	return s.liveEvidence
}

func safeExecutionEvidence(e *ExecutionEvidence, attempt int) *ExecutionEvidence {
	if e == nil || e.Attempt != attempt || attempt < 0 {
		return nil
	}
	state := func(s string) string {
		switch s {
		case executionNotStarted, executionMayHaveRun:
			return s
		default:
			return executionUnknown
		}
	}
	return &ExecutionEvidence{attempt, state(e.Main), state(e.Sidecars), state(e.EnvironmentPreparation)}
}

// Only a live start call carries this observation. Recovery without a completed
// result must remain unknown: a pre-exec snapshot cannot prove what ran later.
type executionEvidenceError struct {
	cause    error
	evidence *ExecutionEvidence
}

func (e *executionEvidenceError) Error() string { return e.cause.Error() }
func (e *executionEvidenceError) Unwrap() error { return e.cause }

const executionEvidenceProtocolHeader = "X-Myce-Execution-Evidence"
const executionEvidenceProtocol = "execution-evidence-v1"

// Negotiate the additive worker field before sending it to a strict old queue.
// Always strip a copy, preserving the durable result for a later upgrade.
func executionEvidenceWireInput(in any, supported bool) (any, bool) {
	switch v := in.(type) {
	case *Result:
		if v != nil {
			return executionEvidenceWireInput(*v, supported)
		}
	case *LogChunk:
		if v != nil {
			return executionEvidenceWireInput(*v, supported)
		}
	case Result:
		has := v.ExecutionEvidence != nil
		if !supported {
			v.ExecutionEvidence = nil
		}
		return v, has && supported
	case LogChunk:
		has := v.ExecutionEvidence != nil
		if !supported {
			v.ExecutionEvidence = nil
		}
		return v, has && supported
	}
	return in, false
}
