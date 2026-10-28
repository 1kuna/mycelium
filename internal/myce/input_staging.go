package myce

import "time"

// Call only after the existing execution/session fences. Worker timestamps are
// diagnostic evidence, never scheduling inputs; missing legacy facts stay nil.
func retainInputsStagedAt(j *Job, at *time.Time) {
	if j.InputsStagedAt == nil && at != nil && !at.IsZero() {
		copy := *at
		j.InputsStagedAt = &copy
	}
}

// Capture only after preparation returns: NAS copiers must all join and the
// complete snapshot must publish before staging completion can be reported.
func captureStagedInputs(clock Clock, prepare func() (string, error)) (stagedInputs, error) {
	directory, err := prepare()
	return stagedInputs{directory: directory, at: clock.Now()}, err
}
