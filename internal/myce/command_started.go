package myce

// commandStarted answers whether any attempt's command may have begun, from
// retained launch and Started evidence: the queue's start acknowledgement on
// the job or any retained copy, or worker execution evidence for main that is
// may_have_run or unknown. False means no attempt's command began, so a
// resubmission cannot duplicate work this job already did.
func commandStarted(j *Job) bool {
	if j.CommandStarted != nil {
		return *j.CommandStarted
	}
	if j.StartedAt != nil {
		return true
	}
	for _, c := range j.Copies {
		if c.StartedAt != nil {
			return true
		}
	}
	mayHaveRun := func(e *ExecutionEvidence) bool { return e != nil && e.Main != executionNotStarted }
	if mayHaveRun(j.ExecutionEvidence) {
		return true
	}
	for _, a := range j.AttemptHistory {
		if mayHaveRun(a.ExecutionEvidence) {
			return true
		}
	}
	return false
}

func projectCommandStarted(j *Job) {
	started := commandStarted(j)
	j.CommandStarted = &started
}
